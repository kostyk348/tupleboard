//! Командный слой: то, чего нет ни в одном из наших инструментов.
//!
//! Четыре контура, каждый — из кибернетики, но каждый проверяем запуском:
//!
//! 1. **Клетка (S1).** Работа берётся по шаблону и живёт под арендой — это делает `space`.
//! 2. **Гейт доказательства (S3\*).** Закрытие требует артефакта: файл, URL или команда
//!    с указанием, как воспроизвести. Без артефакта закрыть нельзя.
//! 3. **Аудит (S3\*, продолжение).** Аудит не читает доказательство, а **перепроверяет** его:
//!    файл существует, URL отвечает 2xx, команда из allow-list политики завершилась нулём.
//!    Чего проверить нельзя — так и говорит: `Unverifiable`, а не «ок».
//! 4. **Эскалация (S5) и алгедоника.** К владельцу можно нести только четыре класса вопросов
//!    (публикация, деньги, доступы, идентичность) — и у каждого обязаны быть три части:
//!    что предлагаю, что будет при молчании, цена ошибки. Всё остальное команда решает сама.
//!    Аварийный сигнал (Бир: algedonic alert) идёт в обход уровней, но с лимитом частоты,
//!    иначе он перестаёт быть аварийным.

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

pub const ESCALATION_CLASSES: [&str; 4] = ["publish", "money", "access", "identity"];
pub const ALGEDONIC_LIMIT_PER_HOUR: usize = 3;
pub const AUDIT_KINDS: [&str; 3] = ["file", "url", "command"];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Evidence {
    pub kind: String,
    pub value: String,
    #[serde(default)]
    pub repro: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Escalation {
    pub class: String,
    pub question: String,
    pub propose: String,
    pub if_silent: String,
    pub cost: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosedRow {
    pub at: u64,
    pub receipt: String,
    pub evidence: Evidence,
    #[serde(default)]
    pub verdict: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Confirmed,
    Mismatch(String),
    Unverifiable(String),
}

impl Verdict {
    pub fn as_str(&self) -> &str {
        match self {
            Verdict::Confirmed => "confirmed",
            Verdict::Mismatch(_) => "mismatch",
            Verdict::Unverifiable(_) => "unverifiable",
        }
    }
    pub fn why(&self) -> &str {
        match self {
            Verdict::Confirmed => "",
            Verdict::Mismatch(why) | Verdict::Unverifiable(why) => why,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    /// Команды, которые аудит имеет право запускать. Всё остальное — «проверить не смогла».
    #[serde(default)]
    pub allowed_commands: Vec<String>,
    #[serde(default = "default_algedonic_limit")]
    pub algedonic_per_hour: usize,
}

fn default_algedonic_limit() -> usize {
    ALGEDONIC_LIMIT_PER_HOUR
}

/// Один дефолт на оба пути (serde и `Default`) — иначе политика без файла молча получает 0.
impl Default for Policy {
    fn default() -> Self {
        Self {
            allowed_commands: Vec::new(),
            algedonic_per_hour: default_algedonic_limit(),
        }
    }
}

impl Policy {
    pub fn load(path: &Path) -> Self {
        fs::read_to_string(path)
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default()
    }
}

// --- проверки формы ------------------------------------------------------------

pub fn validate_evidence(evidence: &Evidence) -> Result<(), String> {
    let kind = evidence.kind.trim();
    let value = evidence.value.trim();
    if !AUDIT_KINDS.contains(&kind) {
        return Err(format!(
            "вид доказательства {kind:?} неизвестен; допустимо: {}",
            AUDIT_KINDS.join(", ")
        ));
    }
    if value.is_empty() {
        return Err("доказательство пустое: нужен файл, URL или команда".to_string());
    }
    if kind == "command" && evidence.repro.trim().is_empty() {
        return Err("для команды нужен repro: как именно её воспроизвести".to_string());
    }
    if kind == "url" && !(value.starts_with("http://") || value.starts_with("https://")) {
        return Err("URL должен начинаться с http:// или https://".to_string());
    }
    Ok(())
}

pub fn validate_escalation(escalation: &Escalation) -> Result<(), String> {
    if !ESCALATION_CLASSES.contains(&escalation.class.trim()) {
        return Err(format!(
            "класс {:?} не эскалируется: к владельцу идут только {} — остальное команда решает сама",
            escalation.class,
            ESCALATION_CLASSES.join(", ")
        ));
    }
    for (name, value) in [
        ("question", &escalation.question),
        ("propose", &escalation.propose),
        ("if_silent", &escalation.if_silent),
        ("cost", &escalation.cost),
    ] {
        if value.trim().is_empty() {
            return Err(format!(
                "в эскалации нет поля {name}: без него это перекладывание"
            ));
        }
    }
    Ok(())
}

// --- аудит ---------------------------------------------------------------------

/// Перепроверка доказательства. «Не смогла проверить» — отдельный вердикт, а не «ок».
pub fn audit(evidence: &Evidence, policy: &Policy) -> Verdict {
    if let Err(problem) = validate_evidence(evidence) {
        return Verdict::Mismatch(problem);
    }
    let value = evidence.value.trim();
    match evidence.kind.trim() {
        "file" => {
            if Path::new(value).exists() {
                Verdict::Confirmed
            } else {
                Verdict::Mismatch(format!("файла нет: {value}"))
            }
        }
        "url" => match http_status(value) {
            Ok(code) if (200..300).contains(&code) => Verdict::Confirmed,
            Ok(code) => Verdict::Mismatch(format!("URL отвечает {code}: {value}")),
            Err(err) => Verdict::Unverifiable(format!("URL недоступен: {err}")),
        },
        "command" => {
            let repro = evidence.repro.trim();
            if !policy
                .allowed_commands
                .iter()
                .any(|allowed| allowed == repro)
            {
                return Verdict::Unverifiable(
                    "команды нет в allow-list политики — аудит не запускает произвольный код"
                        .to_string(),
                );
            }
            match run_command(repro) {
                Ok(0) => Verdict::Confirmed,
                Ok(code) => Verdict::Mismatch(format!("команда завершилась кодом {code}: {repro}")),
                Err(err) => Verdict::Unverifiable(format!("команду не удалось запустить: {err}")),
            }
        }
        other => Verdict::Mismatch(format!("неизвестный вид доказательства: {other}")),
    }
}

fn http_status(url: &str) -> Result<u16, String> {
    use std::io::{Read, Write};
    use std::net::{TcpStream, ToSocketAddrs};
    let rest = url
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let (host_port, path) = match rest.find('/') {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, "/"),
    };
    let host_port = if host_port.contains(':') {
        host_port.to_string()
    } else {
        format!("{host_port}:80")
    };
    let addr = host_port
        .to_socket_addrs()
        .map_err(|err| err.to_string())?
        .next()
        .ok_or_else(|| "адрес не разрешился".to_string())?;
    let mut stream = TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(10))
        .map_err(|e| e.to_string())?;
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(15)));
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: {host_port}\r\nUser-Agent: tupleboard-audit\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    let text = String::from_utf8_lossy(&raw);
    text.split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| "ответ не похож на HTTP".to_string())
}

fn run_command(command: &str) -> Result<i32, String> {
    let output = std::process::Command::new("sh")
        .arg("-c")
        .arg(command)
        .output()
        .map_err(|err| err.to_string())?;
    Ok(output.status.code().unwrap_or(-1))
}

// --- журналы -------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Team {
    pub dir: PathBuf,
    pub policy: Policy,
}

impl Team {
    pub fn new(dir: PathBuf, policy: Policy) -> Self {
        let _ = fs::create_dir_all(&dir);
        Self { dir, policy }
    }

    fn closed_path(&self) -> PathBuf {
        self.dir.join("closed.jsonl")
    }

    fn escalate_path(&self) -> PathBuf {
        self.dir.join("escalations.jsonl")
    }

    fn algedonic_path(&self) -> PathBuf {
        self.dir.join("algedonic.jsonl")
    }

    fn append(path: &Path, line: &str) -> Result<(), String> {
        use std::io::Write;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|err| err.to_string())?;
        writeln!(file, "{line}").map_err(|err| err.to_string())
    }

    /// Закрытие: доказательство обязательно (даже если правило в пространстве выключено).
    pub fn close(&self, receipt: &str, evidence: &Evidence) -> Result<Verdict, String> {
        validate_evidence(evidence)?;
        let verdict = audit(evidence, &self.policy);
        let row = ClosedRow {
            at: now(),
            receipt: receipt.to_string(),
            evidence: evidence.clone(),
            verdict: verdict.as_str().to_string(),
        };
        let line = serde_json::to_string(&row).map_err(|err| err.to_string())?;
        Self::append(&self.closed_path(), &line)?;
        Ok(verdict)
    }

    pub fn escalate(&self, escalation: &Escalation) -> Result<(), String> {
        validate_escalation(escalation)?;
        let line = serde_json::to_string(escalation).map_err(|err| err.to_string())?;
        Self::append(&self.escalate_path(), &line)
    }

    /// Алгедоника: сигнал в обход уровней, но не чаще лимита в час.
    pub fn algedonic(&self, alert: &str) -> Result<usize, String> {
        if alert.trim().is_empty() {
            return Err("пустой аварийный сигнал".to_string());
        }
        let recent = self.algedonic_last_hour();
        if recent >= self.policy.algedonic_per_hour {
            return Err(format!(
                "лимит аварийных сигналов исчерпан: {recent} за час (лимит {}); \
                 если всё ещё горит — это не авария, а состояние, и его надо чинить в обычном контуре",
                self.policy.algedonic_per_hour
            ));
        }
        let line = serde_json::json!({"at": now(), "alert": alert}).to_string();
        Self::append(&self.algedonic_path(), &line)?;
        Ok(recent + 1)
    }

    fn algedonic_last_hour(&self) -> usize {
        let Ok(raw) = fs::read_to_string(self.algedonic_path()) else {
            return 0;
        };
        let cutoff = now().saturating_sub(3600);
        raw.lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|row| row["at"].as_u64().unwrap_or(0) >= cutoff)
            .count()
    }

    /// Перепроверка всех закрытий (или последних `limit`). Возвращает отчёт.
    pub fn audit_all(&self, limit: Option<usize>) -> AuditReport {
        let Ok(raw) = fs::read_to_string(self.closed_path()) else {
            return AuditReport::default();
        };
        let rows: Vec<ClosedRow> = raw
            .lines()
            .filter_map(|line| serde_json::from_str::<ClosedRow>(line).ok())
            .collect();
        let start = match limit {
            Some(limit) => rows.len().saturating_sub(limit),
            None => 0,
        };
        let mut report = AuditReport::default();
        for row in &rows[start..] {
            report.checked += 1;
            match audit(&row.evidence, &self.policy) {
                Verdict::Confirmed => report.confirmed += 1,
                Verdict::Mismatch(why) => {
                    report.mismatched += 1;
                    report
                        .problems
                        .push(format!("{}: {} — {}", row.receipt, row.evidence.value, why));
                }
                Verdict::Unverifiable(why) => {
                    report.unverifiable += 1;
                    report
                        .problems
                        .push(format!("{}: не проверено — {}", row.receipt, why));
                }
            }
        }
        report
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct AuditReport {
    pub checked: usize,
    pub confirmed: usize,
    pub mismatched: usize,
    pub unverifiable: usize,
    pub problems: Vec<String>,
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "tupleboard-team-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn evidence_must_have_known_kind_and_value() {
        let bad = Evidence {
            kind: "слово".into(),
            value: "x".into(),
            repro: "".into(),
        };
        assert!(validate_evidence(&bad).is_err());
        let empty = Evidence {
            kind: "file".into(),
            value: "  ".into(),
            repro: "".into(),
        };
        assert!(validate_evidence(&empty).is_err());
        let no_repro = Evidence {
            kind: "command".into(),
            value: "cargo test".into(),
            repro: "".into(),
        };
        assert!(validate_evidence(&no_repro).unwrap_err().contains("repro"));
        let ok = Evidence {
            kind: "file".into(),
            value: "/tmp/x".into(),
            repro: "".into(),
        };
        assert!(validate_evidence(&ok).is_ok());
    }

    #[test]
    fn only_four_classes_are_escalated() {
        let base = Escalation {
            class: "publish".into(),
            question: "публиковать?".into(),
            propose: "да, публичный репозиторий".into(),
            if_silent: "не публикую".into(),
            cost: "час простоя".into(),
        };
        assert!(validate_escalation(&base).is_ok());
        let wrong = Escalation {
            class: "как лучше".into(),
            ..base.clone()
        };
        let problem = validate_escalation(&wrong).unwrap_err();
        assert!(problem.contains("не эскалируется"), "{problem}");
        assert!(problem.contains("publish"), "в ошибке видно, что можно");
    }

    #[test]
    fn escalation_needs_all_three_parts() {
        let partial = Escalation {
            class: "money".into(),
            question: "покупать?".into(),
            propose: "".into(),
            if_silent: "".into(),
            cost: "".into(),
        };
        let problem = validate_escalation(&partial).unwrap_err();
        assert!(problem.contains("propose"), "{problem}");
    }

    #[test]
    fn audit_checks_file_existence() {
        let dir = temp_dir("file");
        let path = dir.join("artifact.txt");
        let missing = Evidence {
            kind: "file".into(),
            value: path.display().to_string(),
            repro: "".into(),
        };
        assert!(matches!(
            audit(&missing, &Policy::default()),
            Verdict::Mismatch(_)
        ));
        fs::write(&path, "ok").unwrap();
        assert_eq!(audit(&missing, &Policy::default()), Verdict::Confirmed);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn audit_runs_only_allowlisted_commands() {
        let policy = Policy {
            allowed_commands: vec!["true".into(), "false".into()],
            algedonic_per_hour: 3,
        };
        let allowed = Evidence {
            kind: "command".into(),
            value: "true".into(),
            repro: "true".into(),
        };
        assert_eq!(audit(&allowed, &policy), Verdict::Confirmed);
        let lying = Evidence {
            kind: "command".into(),
            value: "false".into(),
            repro: "false".into(),
        };
        assert!(matches!(audit(&lying, &policy), Verdict::Mismatch(_)));
        let foreign = Evidence {
            kind: "command".into(),
            value: "rm -rf /".into(),
            repro: "rm -rf /".into(),
        };
        assert!(
            matches!(audit(&foreign, &policy), Verdict::Unverifiable(_)),
            "не из allow-list — даже не пробуем запускать"
        );
    }

    #[test]
    fn default_policy_allows_algedonic() {
        assert_eq!(
            Policy::default().algedonic_per_hour,
            ALGEDONIC_LIMIT_PER_HOUR
        );
        let dir = temp_dir("default-policy");
        let team = Team::new(dir.clone(), Policy::default());
        assert_eq!(
            team.algedonic("первый сигнал").unwrap(),
            1,
            "политика без файла не глушит канал"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn algedonic_is_rate_limited() {
        let dir = temp_dir("algedonic");
        let team = Team::new(
            dir.clone(),
            Policy {
                allowed_commands: vec![],
                algedonic_per_hour: 2,
            },
        );
        assert_eq!(team.algedonic("диск 100%").unwrap(), 1);
        assert_eq!(team.algedonic("диск 100%").unwrap(), 2);
        let problem = team.algedonic("диск 100%").unwrap_err();
        assert!(problem.contains("лимит"), "{problem}");
        assert!(
            problem.contains("не авария, а состояние"),
            "лимит объясняет, что делать"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn close_records_verdict_and_audit_reports_mismatch() {
        let dir = temp_dir("close");
        let team = Team::new(dir.clone(), Policy::default());
        let ghost = Evidence {
            kind: "file".into(),
            value: "/tmp/нет-такого-файла-tupleboard".into(),
            repro: "".into(),
        };
        let verdict = team.close("r-1", &ghost).unwrap();
        assert!(
            matches!(verdict, Verdict::Mismatch(_)),
            "закрыли — и сразу видно, что пусто"
        );
        let report = team.audit_all(None);
        assert_eq!((report.checked, report.mismatched), (1, 1));
        assert!(report.problems[0].contains("файла нет"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn audit_respects_limit() {
        let dir = temp_dir("limit");
        let path = dir.join("ok.txt");
        fs::write(&path, "x").unwrap();
        let team = Team::new(dir.clone(), Policy::default());
        let evidence = Evidence {
            kind: "file".into(),
            value: path.display().to_string(),
            repro: "".into(),
        };
        team.close("r-1", &evidence).unwrap();
        team.close("r-2", &evidence).unwrap();
        assert_eq!(team.audit_all(Some(1)).checked, 1);
        assert_eq!(team.audit_all(None).checked, 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn policy_is_read_from_file_and_falls_back_quietly() {
        let dir = temp_dir("policy");
        let path = dir.join("policy.json");
        assert!(Policy::load(&path).allowed_commands.is_empty());
        fs::write(
            &path,
            r#"{"allowed_commands":["cargo test"],"algedonic_per_hour":1}"#,
        )
        .unwrap();
        let policy = Policy::load(&path);
        assert_eq!(policy.allowed_commands, vec!["cargo test".to_string()]);
        assert_eq!(policy.algedonic_per_hour, 1);
        let _ = fs::remove_dir_all(&dir);
    }
}
