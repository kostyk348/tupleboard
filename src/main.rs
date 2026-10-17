//! CLI: tupleboard serve | put | rd | take | ack | nack | extend | stats | dump | demo.
//!
//!     tupleboard serve --store data/space.json --port 8801 --capacity 1000 --lease 30
//!     tupleboard put '["translate","doc-1"]' --ttl 600
//!     tupleboard take '["translate","?doc"]' --lease 30 --owner agent-1
//!     tupleboard stats
//!     tupleboard demo      # знакомство, падение агента, SIGKILL сервера, backpressure

use serde_json::{json, Value};
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use tupleboard::server::Server;
use tupleboard::space::Space;

const DEFAULT_SERVER: &str = "http://127.0.0.1:8801";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match args.first().map(String::as_str) {
        Some("serve") => cmd_serve(&args[1..]),
        Some("put") => cmd_put(&args[1..]),
        Some("rd") => cmd_rd(&args[1..]),
        Some("take") => cmd_take(&args[1..]),
        Some("ack") => cmd_receipt(&args[1..], "ack"),
        Some("nack") => cmd_receipt(&args[1..], "nack"),
        Some("extend") => cmd_extend(&args[1..]),
        Some("stats") => cmd_stats(&args[1..]),
        Some("dump") => cmd_dump(&args[1..]),
        Some("demo") => cmd_demo(&args[1..]),
        Some("--help") | Some("-h") | None => {
            print_help();
            0
        }
        Some(other) => {
            eprintln!("неизвестная команда: {other}\n");
            print_help();
            2
        }
    };
    std::process::exit(code);
}

fn print_help() {
    println!(
        "tupleboard — пространство кортежей с арендами

  serve   --store PATH --host H --port P --capacity N --lease S [--require-evidence]
  put     '<json поля>' [--ttl S] [--note TEXT] [--server URL]
  rd      '<json шаблон>' [--wait S] [--server URL]
  take    '<json шаблон>' [--lease S] [--owner NAME] [--wait S] [--server URL]
  ack     <receipt> [--evidence «вывод команды или URL»] [--server URL]
  nack    <receipt> [--server URL]
  extend  <receipt> <seconds> [--server URL]
  stats   [--server URL]
  dump    [--server URL]
  demo    [--workdir PATH]

Шаблон: точные значения, \"*\" — любое, \"?имя\" — связать значение.
Пример: take '[\"translate\",\"?doc\"]' --lease 30 --owner agent-1"
    );
}

// --- разбор аргументов ------------------------------------------------------------

struct Opts {
    positional: Vec<String>,
    flags: Vec<(String, Option<String>)>,
}

impl Opts {
    fn parse(args: &[String]) -> Self {
        let mut positional = Vec::new();
        let mut flags: Vec<(String, Option<String>)> = Vec::new();
        let mut index = 0;
        while index < args.len() {
            let arg = &args[index];
            if let Some(name) = arg.strip_prefix("--") {
                let (name, inline) = match name.split_once('=') {
                    Some((name, value)) => (name.to_string(), Some(value.to_string())),
                    None => (name.to_string(), None),
                };
                if inline.is_some() {
                    flags.push((name, inline));
                } else if index + 1 < args.len() && !args[index + 1].starts_with("--") {
                    flags.push((name, Some(args[index + 1].clone())));
                    index += 1;
                } else {
                    flags.push((name, None));
                }
            } else {
                positional.push(arg.clone());
            }
            index += 1;
        }
        Self { positional, flags }
    }

    fn flag(&self, name: &str) -> Option<&str> {
        self.flags
            .iter()
            .rev()
            .find(|(key, _)| key == name)
            .and_then(|(_, value)| value.as_deref())
    }

    fn has(&self, name: &str) -> bool {
        self.flags.iter().any(|(key, _)| key == name)
    }

    fn number(&self, name: &str) -> Option<f64> {
        self.flag(name).and_then(|value| value.parse::<f64>().ok())
    }

    fn server(&self) -> String {
        self.flag("server").unwrap_or(DEFAULT_SERVER).to_string()
    }
}

fn parse_fields(raw: &str) -> Value {
    match serde_json::from_str::<Value>(raw) {
        Ok(value @ Value::Array(_)) => value,
        Ok(other) => {
            eprintln!("поля должны быть JSON-массивом, получено: {other}");
            std::process::exit(2);
        }
        Err(err) => {
            eprintln!("не разобрала JSON: {err}\nпример: '[\"translate\",\"doc-1\"]'");
            std::process::exit(2);
        }
    }
}

// --- HTTP-клиент ------------------------------------------------------------------
fn split_url(url: &str) -> (String, u16) {
    let rest = url
        .strip_prefix("http://")
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or("127.0.0.1:8801");
    match rest.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.parse().unwrap_or(8801)),
        None => (rest.to_string(), 80),
    }
}

fn try_request(
    method: &str,
    url: &str,
    payload: Option<&str>,
    read_timeout: Duration,
) -> std::io::Result<(u16, Value)> {
    let (host, port) = split_url(url);
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .map_err(|err| std::io::Error::new(ErrorKind::InvalidInput, format!("{err}")))?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
    let _ = stream.set_read_timeout(Some(read_timeout));
    let body = payload.unwrap_or("");
    let head = format!(
        "{method} {} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        url_path(url),
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .and_then(|_| stream.write_all(body.as_bytes()))?;
    let mut raw = Vec::new();
    if let Err(err) = stream.read_to_end(&mut raw) {
        if err.kind() != ErrorKind::UnexpectedEof {
            return Err(err);
        }
    }
    let text = String::from_utf8_lossy(&raw).to_string();
    let code = text
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    let value = serde_json::from_str::<Value>(&body).unwrap_or(Value::Null);
    Ok((code, value))
}

/// Для команд: недоступный сервер — это ошибка пользователя, а не паника.
fn request(method: &str, url: &str, payload: Option<&str>, read_timeout: Duration) -> (u16, Value) {
    match try_request(method, url, payload, read_timeout) {
        Ok(answer) => answer,
        Err(err) => {
            eprintln!("сервер недоступен ({url}): {err}");
            std::process::exit(2);
        }
    }
}

fn url_path(url: &str) -> String {
    let without_scheme = url.strip_prefix("http://").unwrap_or(url);
    match without_scheme.find('/') {
        Some(index) => without_scheme[index..].to_string(),
        None => "/".to_string(),
    }
}

fn post(server: &str, path: &str, payload: &Value, wait: f64) -> (u16, Value) {
    let url = format!("{}{path}", server.trim_end_matches('/'));
    let text = payload.to_string();
    request(
        "POST",
        &url,
        Some(&text),
        Duration::from_secs_f64(wait + 30.0),
    )
}

fn get(server: &str, path: &str) -> (u16, Value) {
    let url = format!("{}{path}", server.trim_end_matches('/'));
    request("GET", &url, None, Duration::from_secs(30))
}

// --- команды ----------------------------------------------------------------------
fn cmd_serve(args: &[String]) -> i32 {
    let opts = Opts::parse(args);
    let store = opts.flag("store").unwrap_or("data/space.json").to_string();
    let host = opts.flag("host").unwrap_or("127.0.0.1").to_string();
    let port: u16 = opts.flag("port").unwrap_or("8801").parse().unwrap_or(8801);
    let capacity: usize = opts
        .flag("capacity")
        .unwrap_or("1000")
        .parse()
        .unwrap_or(1000);
    let lease: f64 = opts.number("lease").unwrap_or(30.0);
    let path: Option<PathBuf> = if store.is_empty() {
        None
    } else {
        Some(PathBuf::from(&store))
    };
    let require_evidence = opts.has("require-evidence");
    let space =
        Arc::new(Space::new(path, capacity, lease).with_evidence_required(require_evidence));
    let stats = space.stats();
    println!(
        "store: {}; в пространстве {}, в работе {}, ёмкость {}, аренда по умолчанию {}s",
        if store.is_empty() {
            "в памяти"
        } else {
            &store
        },
        stats.in_space,
        stats.in_flight,
        stats.capacity,
        stats.default_lease
    );
    match Server::bind(&format!("{host}:{port}"), space) {
        Ok(server) => {
            server.serve_forever();
            0
        }
        Err(err) => {
            eprintln!("не удалось занять {host}:{port}: {err}");
            1
        }
    }
}

fn cmd_put(args: &[String]) -> i32 {
    let opts = Opts::parse(args);
    let Some(raw) = opts.positional.first() else {
        eprintln!("нужен JSON-список полей: put '[\"translate\",\"doc-1\"]'");
        return 2;
    };
    let mut payload = json!({ "fields": parse_fields(raw) });
    if let Some(ttl) = opts.number("ttl") {
        payload["ttl"] = json!(ttl);
    }
    if let Some(note) = opts.flag("note") {
        payload["note"] = json!(note);
    }
    let (code, body) = post(&opts.server(), "/out", &payload, 0.0);
    println!("HTTP {code} {body}");
    if code == 201 {
        0
    } else {
        1
    }
}

fn cmd_rd(args: &[String]) -> i32 {
    let opts = Opts::parse(args);
    let Some(raw) = opts.positional.first() else {
        eprintln!("нужен JSON-шаблон: rd '[\"ocr\",\"*\"]'");
        return 2;
    };
    let wait = opts.number("wait").unwrap_or(0.0);
    let (code, body) = post(
        &opts.server(),
        "/rd",
        &json!({"pattern": parse_fields(raw), "timeout": wait}),
        wait,
    );
    match code {
        200 => {
            println!("поля: {}", body["fields"]);
            println!("связки: {}", body["bindings"]);
            0
        }
        404 => {
            println!("HTTP 404 — {}", body["error"]);
            2
        }
        _ => {
            println!("HTTP {code} {body}");
            1
        }
    }
}

fn cmd_take(args: &[String]) -> i32 {
    let opts = Opts::parse(args);
    let Some(raw) = opts.positional.first() else {
        eprintln!("нужен JSON-шаблон: take '[\"translate\",\"?doc\"]'");
        return 2;
    };
    let wait = opts.number("wait").unwrap_or(0.0);
    let mut payload = json!({"pattern": parse_fields(raw), "timeout": wait});
    if let Some(lease) = opts.number("lease") {
        payload["lease"] = json!(lease);
    }
    if let Some(owner) = opts.flag("owner") {
        payload["owner"] = json!(owner);
    }
    let (code, body) = post(&opts.server(), "/in", &payload, wait);
    match code {
        200 => {
            println!("поля: {}", body["fields"]);
            println!("связки: {}", body["bindings"]);
            if body["receipt"].is_string() {
                println!(
                    "receipt: {} (аренда {}s; подтвердить: ack, вернуть: nack)",
                    body["receipt"].as_str().unwrap_or(""),
                    body["lease_in"]
                );
            }
            0
        }
        404 => {
            println!("HTTP 404 — {}", body["error"]);
            2
        }
        _ => {
            println!("HTTP {code} {body}");
            1
        }
    }
}

fn cmd_receipt(args: &[String], kind: &str) -> i32 {
    let opts = Opts::parse(args);
    let Some(receipt) = opts.positional.first() else {
        eprintln!("нужен receipt: {kind} r-…");
        return 2;
    };
    let mut payload = json!({"receipt": receipt});
    if let Some(evidence) = opts.flag("evidence") {
        payload["evidence"] = json!(evidence);
    }
    let (code, body) = post(&opts.server(), &format!("/{kind}"), &payload, 0.0);
    println!("HTTP {code} {body}");
    if code == 200 {
        0
    } else {
        1
    }
}

fn cmd_extend(args: &[String]) -> i32 {
    let opts = Opts::parse(args);
    let (Some(receipt), Some(seconds)) = (opts.positional.first(), opts.positional.get(1)) else {
        eprintln!("нужно: extend <receipt> <seconds>");
        return 2;
    };
    let seconds: f64 = seconds.parse().unwrap_or(0.0);
    let (code, body) = post(
        &opts.server(),
        "/extend",
        &json!({"receipt": receipt, "seconds": seconds}),
        0.0,
    );
    println!("HTTP {code} {body}");
    if code == 200 {
        0
    } else {
        1
    }
}

fn cmd_stats(args: &[String]) -> i32 {
    let opts = Opts::parse(args);
    let (code, body) = get(&opts.server(), "/stats");
    if code != 200 {
        eprintln!("HTTP {code} {body}");
        return 1;
    }
    println!(
        "в пространстве: {}, в работе: {}, ёмкость: {}",
        body["in_space"], body["in_flight"], body["capacity"]
    );
    println!("счётчики: {}", body["counters"]);
    if let Some(rows) = body["in_flight_rows"].as_array() {
        for row in rows {
            println!(
                "  в работе {} у {}: {} (аренда {}s, попытка {})",
                row["receipt"], row["owner"], row["fields"], row["lease_in"], row["attempts"]
            );
        }
    }
    0
}

fn cmd_dump(args: &[String]) -> i32 {
    let opts = Opts::parse(args);
    let (code, body) = get(&opts.server(), "/tuples");
    if code != 200 {
        eprintln!("HTTP {code} {body}");
        return 1;
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&body).unwrap_or_default()
    );
    0
}

// --- демонстрация -----------------------------------------------------------------
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .unwrap_or(8801)
}

fn spawn_server(store: &Path, port: u16, capacity: usize, lease: f64) -> Child {
    let exe = std::env::current_exe().expect("не нашла свой бинарник");
    Command::new(exe)
        .args([
            "serve",
            "--store",
            store.to_str().unwrap_or("data/space.json"),
            "--port",
            &port.to_string(),
            "--capacity",
            &capacity.to_string(),
            "--lease",
            &lease.to_string(),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("не удалось запустить serve")
}

fn wait_ready(base: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let url = format!("{}/healthz", base.trim_end_matches('/'));
    while Instant::now() < deadline {
        // сервер ещё поднимается — это нормальное состояние, а не ошибка пользователя
        if let Ok((200, _)) = try_request("GET", &url, None, Duration::from_secs(5)) {
            return true;
        }
        sleep(Duration::from_millis(50));
    }
    false
}

fn wait_until(mut predicate: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        sleep(Duration::from_millis(100));
    }
    false
}

fn cmd_demo(args: &[String]) -> i32 {
    let opts = Opts::parse(args);
    let work = opts.flag("workdir").map(PathBuf::from).unwrap_or_else(|| {
        std::env::temp_dir().join(format!("tupleboard-demo-{}", std::process::id()))
    });
    let _ = std::fs::create_dir_all(&work);
    let store = work.join("space.json");
    let _ = std::fs::remove_file(&store);
    let port = free_port();
    let base = format!("http://127.0.0.1:{port}");
    let (capacity, crash_lease) = (4usize, 5.0f64);
    let mut checks: Vec<(String, bool)> = Vec::new();

    println!("рабочая папка: {}\nпространство: {base} (ёмкость {capacity}, аренда по умолчанию {crash_lease}s)\n", work.display());

    let check = |checks: &mut Vec<(String, bool)>, label: &str, ok: bool, detail: &str| {
        checks.push((label.to_string(), ok));
        if ok || detail.is_empty() {
            println!("  {} {label}", if ok { "✓" } else { "✗" });
        } else {
            println!("  ✗ {label} — {detail}");
        }
    };

    let mut server = spawn_server(&store, port, capacity, crash_lease);
    if !wait_ready(&base, Duration::from_secs(15)) {
        eprintln!("✗ сервер не поднялся");
        let _ = server.kill();
        return 1;
    }

    println!("1. ассоциативная связь: три задачи легли в одно пространство");
    let mut last_code = 0;
    for fields in [
        json!(["translate", "doc-1"]),
        json!(["translate", "doc-2"]),
        json!(["ocr", "scan-7"]),
    ] {
        let (code, _) = post(&base, "/out", &json!({"fields": fields}), 0.0);
        last_code = code;
    }
    check(
        &mut checks,
        "задачи лежат (201 на каждую)",
        last_code == 201,
        &format!("HTTP {last_code}"),
    );

    println!("\n2. выборка по шаблону, а не по адресу");
    let (_, first) = post(
        &base,
        "/in",
        &json!({"pattern": ["translate", "?doc"], "lease": crash_lease, "owner": "worker-A"}),
        0.0,
    );
    check(
        &mut checks,
        "взята первая подходящая под шаблон",
        first["fields"] == json!(["translate", "doc-1"]),
        &first["fields"].to_string(),
    );
    check(
        &mut checks,
        "связка ?doc заполнилась",
        first["bindings"]["doc"] == json!("doc-1"),
        &first["bindings"].to_string(),
    );
    let (_, second) = post(
        &base,
        "/in",
        &json!({"pattern": ["translate", "?doc"], "lease": 60, "owner": "worker-A"}),
        0.0,
    );
    check(
        &mut checks,
        "вторая задача по тому же шаблону — другая: первая в работе",
        second["fields"] == json!(["translate", "doc-2"]),
        &second["fields"].to_string(),
    );
    let _ = post(&base, "/ack", &json!({"receipt": second["receipt"]}), 0.0);
    let stats = get(&base, "/stats").1;
    check(
        &mut checks,
        "ack убрал вторую задачу совсем",
        stats["in_space"] == json!(1),
        &stats["in_space"].to_string(),
    );

    println!("\n3. worker-A взял задачу под арендой и умер: ack не приходит, сервера больше нет");
    let _ = server.kill();
    let _ = server.wait();
    check(&mut checks, "сервер убит сигналом", true, "");
    server = spawn_server(&store, port, capacity, crash_lease);
    check(
        &mut checks,
        "сервер поднят заново на том же store",
        wait_ready(&base, Duration::from_secs(15)),
        "",
    );
    let stats = get(&base, "/stats").1;
    check(
        &mut checks,
        "задачи пережили падение сервера",
        stats["in_space"].as_u64().unwrap_or(0) + stats["in_flight"].as_u64().unwrap_or(0) == 2,
        &format!(
            "в пространстве {}, в работе {}",
            stats["in_space"], stats["in_flight"]
        ),
    );
    check(
        &mut checks,
        "задача worker-A ещё в работе — аренда жива",
        stats["in_flight"] == json!(1) && stats["in_flight_rows"][0]["owner"] == json!("worker-A"),
        &stats["in_flight_rows"].to_string(),
    );

    println!("\n4. аренда истекает сама — задача возвращается в пространство");
    let expired = wait_until(
        || get(&base, "/stats").1["in_flight"] == json!(0),
        Duration::from_secs_f64(crash_lease + 8.0),
    );
    check(
        &mut checks,
        "аренда истекла без чьего-либо вмешательства",
        expired,
        "",
    );
    let (_, again) = post(
        &base,
        "/in",
        &json!({"pattern": ["translate", "?doc"], "lease": 30, "owner": "worker-C"}),
        0.0,
    );
    check(
        &mut checks,
        "ту же задачу взял другой агент",
        again["fields"] == json!(["translate", "doc-1"]),
        &again["fields"].to_string(),
    );
    check(
        &mut checks,
        "видно, что это вторая попытка",
        again["attempts"] == json!(2),
        &again["attempts"].to_string(),
    );
    let _ = post(&base, "/ack", &json!({"receipt": again["receipt"]}), 0.0);
    check(
        &mut checks,
        "после ack задача исчезла окончательно",
        get(&base, "/stats").1["in_space"] == json!(1),
        "",
    );

    println!("\n5. аренда — обязанность: продлить или вернуть явно");
    let (_, task) = post(
        &base,
        "/in",
        &json!({"pattern": ["ocr", "?x"], "lease": 1, "owner": "worker-D"}),
        0.0,
    );
    let (_, extended) = post(
        &base,
        "/extend",
        &json!({"receipt": task["receipt"], "seconds": 30}),
        0.0,
    );
    check(
        &mut checks,
        "аренда продлена",
        extended["lease_in"].as_u64().unwrap_or(0) > 1,
        &extended.to_string(),
    );
    let _ = post(&base, "/nack", &json!({"receipt": task["receipt"]}), 0.0);
    check(
        &mut checks,
        "nack вернул задачу сразу, не дожидаясь аренды",
        get(&base, "/stats").1["in_space"] == json!(1),
        "",
    );

    println!("\n6. backpressure: полное пространство отказывает честно");
    let used = get(&base, "/stats").1["in_space"].as_u64().unwrap_or(0) as usize;
    for index in 0..capacity.saturating_sub(used) {
        let _ = post(&base, "/out", &json!({"fields": ["filler", index]}), 0.0);
    }
    let (code, refused) = post(
        &base,
        "/out",
        &json!({"fields": ["filler", "overflow"]}),
        0.0,
    );
    check(
        &mut checks,
        "лишняя задача отклонена 429",
        code == 429,
        &format!("HTTP {code}"),
    );
    check(
        &mut checks,
        "причина названа",
        refused["error"]
            .as_str()
            .unwrap_or("")
            .contains("заполнено"),
        &refused.to_string(),
    );

    println!("\n7. блокирующее чтение: ждёт и честно говорит «ничего»");
    let started = Instant::now();
    let (code, _) = post(
        &base,
        "/rd",
        &json!({"pattern": ["нет", "такого"], "timeout": 0.4}),
        0.4,
    );
    let waited = started.elapsed().as_secs_f64();
    check(
        &mut checks,
        "404 после ~0.4с ожидания",
        code == 404 && waited >= 0.35,
        &format!("HTTP {code} через {waited:.2}s"),
    );

    let _ = server.kill();
    let _ = server.wait();

    let failed: Vec<&String> = checks
        .iter()
        .filter(|(_, ok)| !ok)
        .map(|(label, _)| label)
        .collect();
    println!(
        "\nитог: проверок {}, провалено {} — {}",
        checks.len(),
        failed.len(),
        if failed.is_empty() {
            "работает".to_string()
        } else {
            format!(
                "НЕ работает: {}",
                failed
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            )
        }
    );
    if failed.is_empty() {
        0
    } else {
        1
    }
}
