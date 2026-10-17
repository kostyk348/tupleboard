//! HTTP-API поверх пространства. Без фреймворков: один accept-цикл, поток на соединение.
//!
//! ```text
//!     POST /out    {"fields": [...], "ttl": 600, "note": "…"}   → 201 {"id", "in_space"}
//!     POST /rd     {"pattern": [...], "timeout": 3}             → 200 {"fields","bindings"} | 404
//!     POST /in     {"pattern": […], "timeout", "lease", "owner"} → 200 {"fields","receipt",…} | 404
//!     POST /ack    {"receipt": "…"}                             → 200 {"acked": true} | 404
//!     POST /nack   {"receipt": "…"}                             → 200 {"returned": true} | 404
//!     POST /extend {"receipt": "…", "seconds": 30}              → 200 {"lease_in": 30} | 404
//!     GET  /stats | /tuples | /healthz
//!
//! Коды отказа — контракт: 400 (шаблон не той формы), 404 (совпадений или аренды нет),
//! 429 (мест нет, backpressure) с `Retry-After`.
//! ```

use crate::space::{Ack, Error as SpaceError, Space};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const MAX_BODY: usize = 1024 * 1024;
const RETRY_AFTER: &str = "1";
const IO_TIMEOUT: Duration = Duration::from_secs(60);

pub struct Server {
    listener: TcpListener,
    space: Arc<Space>,
}

impl Server {
    pub fn bind(addr: &str, space: Arc<Space>) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        eprintln!("пространство слушает http://{}", listener.local_addr()?);
        Ok(Self { listener, space })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Принимает соединения вечно; каждое обслуживается своим потоком.
    pub fn serve_forever(self) {
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    let space = Arc::clone(&self.space);
                    thread::spawn(move || {
                        if let Err(err) = handle(stream, &space) {
                            eprintln!("соединение: {err}");
                        }
                    });
                }
                Err(err) => eprintln!("accept: {err}"),
            }
        }
    }
}

struct Request {
    method: String,
    path: String,
    body: Vec<u8>,
}

fn read_request(stream: &mut TcpStream) -> std::io::Result<Option<Request>> {
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("/").to_string();

    let mut content_length = 0usize;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 {
            break;
        }
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse::<usize>().unwrap_or(0).min(MAX_BODY);
            }
        }
    }
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }
    Ok(Some(Request { method, path, body }))
}

fn write_response(
    stream: &mut TcpStream,
    code: u16,
    payload: &Value,
    extra: &[(&str, &str)],
) -> std::io::Result<()> {
    let body = serde_json::to_vec(payload)?;
    let mut head = format!(
        "HTTP/1.1 {code} {}\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n",
        reason(code),
        body.len()
    );
    for (name, value) in extra {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())?;
    stream.write_all(&body)?;
    stream.flush()
}

fn reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        404 => "Not Found",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Unknown",
    }
}

fn handle(mut stream: TcpStream, space: &Arc<Space>) -> std::io::Result<()> {
    let Some(request) = read_request(&mut stream)? else {
        return Ok(());
    };
    let payload = parse_json(&request.body);
    let (code, body, extra) = route(&request, payload, space);
    write_response(&mut stream, code, &body, &extra)
}

fn parse_json(body: &[u8]) -> std::result::Result<Value, SpaceError> {
    if body.is_empty() {
        return Ok(Value::Object(Default::default()));
    }
    serde_json::from_slice(body)
        .map_err(|err| crate::space::BadPattern::new(format!("тело запроса не JSON: {err}")).into())
}

fn route(
    request: &Request,
    payload: std::result::Result<Value, SpaceError>,
    space: &Arc<Space>,
) -> (u16, Value, Vec<(&'static str, &'static str)>) {
    let path = request.path.split('?').next().unwrap_or("/").to_string();
    if request.method == "GET" {
        return match path.as_str() {
            "/healthz" => {
                let stats = space.stats();
                (
                    200,
                    json!({"ok": true, "in_space": stats.in_space, "in_flight": stats.in_flight}),
                    vec![],
                )
            }
            "/stats" => (200, json!(space.stats()), vec![]),
            "/tuples" => (200, space.dump(), vec![]),
            _ => (404, json!({"error": "нет такого пути"}), vec![]),
        };
    }
    if request.method != "POST" {
        return (404, json!({"error": "нет такого пути"}), vec![]);
    }
    let payload = match payload {
        Ok(value) => value,
        Err(err) => return failure(err),
    };
    match call(&path, &payload, space) {
        Ok(response) => response,
        Err(err) => failure(err),
    }
}

fn call(
    path: &str,
    payload: &Value,
    space: &Arc<Space>,
) -> std::result::Result<(u16, Value, Vec<(&'static str, &'static str)>), SpaceError> {
    let timeout = payload
        .get("timeout")
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
        .max(0.0);
    let wait = Duration::from_secs_f64(timeout);
    match path {
        "/out" => {
            let fields = payload.get("fields").cloned().unwrap_or(Value::Null);
            let ttl = payload.get("ttl").and_then(Value::as_f64);
            let note = payload.get("note").and_then(Value::as_str).unwrap_or("");
            let item = space.out(fields, ttl, note)?;
            let in_space = space.stats().in_space;
            Ok((
                201,
                json!({"id": item.id, "fields": item.fields, "in_space": in_space}),
                vec![],
            ))
        }
        "/rd" => {
            let pattern = payload.get("pattern").cloned().unwrap_or(Value::Null);
            match space.rd(&pattern, wait)? {
                Some((fields, bindings)) => {
                    Ok((200, json!({"fields": fields, "bindings": bindings}), vec![]))
                }
                None => Ok((
                    404,
                    json!({"error": "совпадений нет", "pattern": pattern}),
                    vec![],
                )),
            }
        }
        "/in" => {
            let pattern = payload.get("pattern").cloned().unwrap_or(Value::Null);
            let lease = payload.get("lease").and_then(Value::as_f64);
            let owner = payload.get("owner").and_then(Value::as_str).unwrap_or("");
            match space.take(&pattern, wait, lease, owner)? {
                Some(taken) => Ok((200, json!(taken), vec![])),
                None => Ok((
                    404,
                    json!({"error": "совпадений нет", "pattern": pattern}),
                    vec![],
                )),
            }
        }
        "/ack" => {
            let receipt = payload.get("receipt").and_then(Value::as_str).unwrap_or("");
            let evidence = payload.get("evidence").and_then(Value::as_str);
            match space.ack(receipt, evidence) {
                Ack::Done(proof) => Ok((200, json!({"acked": true, "evidence": proof}), vec![])),
                Ack::Unknown => Ok((
                    404,
                    json!({"error": "такого receipt нет: аренда истекла или уже закрыта"}),
                    vec![],
                )),
                Ack::NoEvidence => Ok((
                    422,
                    json!({
                        "error": "нет доказательства — нет закрытия: пришли evidence (вывод команды, URL, лог)",
                        "hint": "POST /ack {\"receipt\": \"…\", \"evidence\": \"cargo test → ok\"}"
                    }),
                    vec![],
                )),
            }
        }
        "/nack" => {
            let receipt = payload.get("receipt").and_then(Value::as_str).unwrap_or("");
            if space.nack(receipt) {
                Ok((200, json!({"returned": true}), vec![]))
            } else {
                Ok((
                    404,
                    json!({"error": "такого receipt нет: аренда истекла или уже закрыта"}),
                    vec![],
                ))
            }
        }
        "/extend" => {
            let receipt = payload.get("receipt").and_then(Value::as_str).unwrap_or("");
            let seconds = payload
                .get("seconds")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            match space.extend(receipt, seconds)? {
                Some(lease_in) => Ok((200, json!({"lease_in": lease_in}), vec![])),
                None => Ok((
                    404,
                    json!({"error": "такого receipt нет: продлевать нечего"}),
                    vec![],
                )),
            }
        }
        _ => Ok((404, json!({"error": "нет такого пути"}), vec![])),
    }
}

fn failure(err: SpaceError) -> (u16, Value, Vec<(&'static str, &'static str)>) {
    match err {
        SpaceError::Bad(bad) => (400, json!({"error": bad.message}), vec![]),
        SpaceError::Full(full) => (
            429,
            json!({"error": full.to_string(), "capacity": full.capacity,
                   "in_space": full.in_space, "retry_after": 1}),
            vec![("Retry-After", RETRY_AFTER)],
        ),
    }
}
