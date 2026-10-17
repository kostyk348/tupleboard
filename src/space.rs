//! Пространство кортежей: ассоциативная разделяемая память с арендами.
//!
//! Операции — классические лйндовские плюс аренда:
//! `out` положить, `rd` прочитать не забирая, `take` забрать (под арендой),
//! `ack` подтвердить, `nack` вернуть сразу, `extend` продлить аренду.
//!
//! Шаблон — JSON-массив той же длины, что и данные:
//!   * точное значение — `1`, `"translate"`, `true`, `null`, `[1,2]`;
//!   * `"*"` — любое значение;
//!   * `"?имя"` — любое значение, связать с именем (одно имя — одно значение).
//!
//! Время пространства (аренды, TTL) отделено от времени ожидания: первое можно подменить
//! в тестах, второе всегда реальное, иначе блокирующее чтение не истечёт никогда.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const DEFAULT_CAPACITY: usize = 1000;
pub const DEFAULT_LEASE: f64 = 30.0;
pub const WILDCARD: &str = "*";
pub const BINDING_PREFIX: char = '?';

static SEQ: AtomicU64 = AtomicU64::new(0);

/// Часы пространства: секунды от эпохи. Подменяются в тестах.
pub type Clock = Arc<dyn Fn() -> f64 + Send + Sync>;

pub fn system_clock() -> Clock {
    Arc::new(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
    })
}

fn new_id(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!(
        "{}{:012x}",
        prefix,
        nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ seq
    )
}

// --- ошибки -------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadPattern {
    pub message: String,
}

impl BadPattern {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for BadPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceFull {
    pub capacity: usize,
    pub in_space: usize,
}

impl fmt::Display for SpaceFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "пространство заполнено: {} из {}",
            self.in_space, self.capacity
        )
    }
}

/// Отказ пространства: либо кривой шаблон (400), либо нет места (429).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Bad(BadPattern),
    Full(SpaceFull),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Bad(err) => write!(f, "{err}"),
            Error::Full(err) => write!(f, "{err}"),
        }
    }
}

impl error::Error for Error {}

impl From<BadPattern> for Error {
    fn from(value: BadPattern) -> Self {
        Error::Bad(value)
    }
}

impl From<SpaceFull> for Error {
    fn from(value: SpaceFull) -> Self {
        Error::Full(value)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

// --- данные -------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Counters {
    #[serde(default)]
    pub out: u64,
    #[serde(default)]
    pub reads: u64,
    #[serde(default)]
    pub taken: u64,
    #[serde(default)]
    pub acked: u64,
    #[serde(default)]
    pub nacked: u64,
    #[serde(default)]
    pub returned: u64,
    #[serde(default)]
    pub dropped: u64,
    #[serde(default)]
    pub refused: u64,
    #[serde(default)]
    pub extended: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub id: String,
    pub fields: Value,
    pub at: f64,
    #[serde(default)]
    pub ttl: Option<f64>,
    #[serde(default)]
    pub attempts: u32,
    #[serde(default)]
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Flight {
    pub receipt: String,
    pub item: Item,
    pub owner: String,
    pub taken: f64,
    pub deadline: f64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct State {
    #[serde(default)]
    items: Vec<Item>,
    #[serde(default)]
    in_flight: HashMap<String, Flight>,
    #[serde(default)]
    counters: Counters,
}

/// Чем кончилась попытка подтвердить работу.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ack {
    /// Подтверждено; внутри — доказательство, если оно требовалось.
    Done(Option<String>),
    /// Такого receipt нет: аренда истекла или уже закрыта.
    Unknown,
    /// Работа есть, а доказательства нет — закрыть нельзя.
    NoEvidence,
}

/// Результат взятия под арендой.
#[derive(Debug, Clone, Serialize)]
pub struct Taken {
    pub fields: Value,
    pub bindings: BTreeMap<String, Value>,
    pub id: String,
    pub attempts: u32,
    pub receipt: Option<String>,
    pub lease_in: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FlightRow {
    pub receipt: String,
    pub owner: String,
    pub fields: Value,
    pub lease_in: u64,
    pub attempts: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct Stats {
    pub in_space: usize,
    pub in_flight: usize,
    pub capacity: usize,
    pub default_lease: f64,
    pub counters: Counters,
    pub in_flight_rows: Vec<FlightRow>,
}

// --- шаблоны ------------------------------------------------------------------

/// `Ok(Some(bindings))` — подходит, `Ok(None)` — нет, `Err` — шаблон не той формы.
pub fn matches(pattern: &Value, fields: &Value) -> Result<Option<BTreeMap<String, Value>>> {
    let pattern_items = match pattern {
        Value::Array(items) => items,
        _ => return Err(BadPattern::new("шаблон должен быть JSON-массивом полей").into()),
    };
    let field_items = match fields {
        Value::Array(items) => items,
        _ => return Err(BadPattern::new("данные должны быть JSON-массивом полей").into()),
    };
    if pattern_items.len() != field_items.len() {
        return Ok(None);
    }
    let mut bindings: BTreeMap<String, Value> = BTreeMap::new();
    for (expect, value) in pattern_items.iter().zip(field_items.iter()) {
        if let Value::String(text) = expect {
            if text == WILDCARD {
                continue;
            }
            if let Some(name) = text.strip_prefix(BINDING_PREFIX) {
                if name.is_empty() {
                    return Err(BadPattern::new("пустое имя переменной в шаблоне: '?'").into());
                }
                match bindings.get(name) {
                    Some(bound) if bound != value => return Ok(None),
                    Some(_) => {}
                    None => {
                        bindings.insert(name.to_string(), value.clone());
                    }
                }
                continue;
            }
        }
        if expect != value {
            return Ok(None);
        }
    }
    Ok(Some(bindings))
}

// --- пространство -------------------------------------------------------------

pub struct Space {
    state: Mutex<State>,
    cv: Condvar,
    clock: Clock,
    capacity: usize,
    default_lease: f64,
    path: Option<PathBuf>,
    /// POSIWID на уровне примитива: «сделано» подтверждается артефактом, а не словом.
    require_evidence: bool,
}

impl Space {
    /// Пространство с реальными часами. `path = None` — только в памяти.
    pub fn new(path: Option<PathBuf>, capacity: usize, lease: f64) -> Self {
        Self::with_clock(path, capacity, lease, system_clock())
    }

    pub fn with_clock(path: Option<PathBuf>, capacity: usize, lease: f64, clock: Clock) -> Self {
        let space = Self {
            state: Mutex::new(State::default()),
            cv: Condvar::new(),
            clock,
            capacity: capacity.max(1),
            default_lease: lease.max(0.0),
            path,
            require_evidence: false,
        };
        space.load();
        {
            let mut state = space.lock();
            space.sweep_locked(&mut state);
        }
        space
    }

    /// Включить правило «нет доказательства — нет закрытия».
    pub fn with_evidence_required(mut self, yes: bool) -> Self {
        self.require_evidence = yes;
        self
    }

    pub fn evidence_required(&self) -> bool {
        self.require_evidence
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn now(&self) -> f64 {
        (self.clock)()
    }

    fn load(&self) {
        let Some(path) = &self.path else { return };
        let Ok(raw) = fs::read_to_string(path) else {
            return;
        };
        if let Ok(state) = serde_json::from_str::<State>(&raw) {
            *self.lock() = state;
        }
    }

    fn save_locked(&self, state: &State) {
        let Some(path) = &self.path else { return };
        let _ = write_atomic(path, state);
    }

    /// Просроченные аренды возвращаются в пространство, протухшее по TTL — исчезает.
    fn sweep_locked(&self, state: &mut State) -> (usize, usize) {
        let now = self.now();
        let expired: Vec<String> = state
            .in_flight
            .iter()
            .filter(|(_, flight)| flight.deadline <= now)
            .map(|(receipt, _)| receipt.clone())
            .collect();
        let returned = expired.len();
        for receipt in expired {
            if let Some(flight) = state.in_flight.remove(&receipt) {
                state.items.push(flight.item); // в хвост: не долбим одного и того же
                state.counters.returned += 1;
            }
        }
        let before = state.items.len();
        state.items.retain(|item| match item.ttl {
            Some(ttl) => now < item.at + ttl,
            None => true,
        });
        let dropped = before - state.items.len();
        state.counters.dropped += dropped as u64;
        if returned > 0 || dropped > 0 {
            self.save_locked(state);
            self.cv.notify_all();
        }
        (returned, dropped)
    }

    fn find(
        &self,
        state: &State,
        pattern: &Value,
    ) -> Result<Option<(usize, BTreeMap<String, Value>)>> {
        for (index, item) in state.items.iter().enumerate() {
            if let Some(bindings) = matches(pattern, &item.fields)? {
                return Ok(Some((index, bindings)));
            }
        }
        Ok(None)
    }

    /// Сколько ждать до ближайшего «само пройдёт»: аренда или TTL.
    fn next_moment(&self, state: &State) -> Option<Duration> {
        let now = self.now();
        let mut soonest: Option<f64> = None;
        for flight in state.in_flight.values() {
            let moment = flight.deadline - now;
            if moment > 0.0 {
                soonest = Some(soonest.map_or(moment, |best: f64| best.min(moment)));
            }
        }
        for item in &state.items {
            if let Some(ttl) = item.ttl {
                let moment = item.at + ttl - now;
                if moment > 0.0 {
                    soonest = Some(soonest.map_or(moment, |best: f64| best.min(moment)));
                }
            }
        }
        soonest.map(Duration::from_secs_f64)
    }

    /// Положить кортеж. Мест нет — честный отказ, а не безграничный рост.
    pub fn out(&self, fields: Value, ttl: Option<f64>, note: &str) -> Result<Item> {
        if !fields.is_array() {
            return Err(BadPattern::new("поля должны быть JSON-массивом").into());
        }
        let mut state = self.lock();
        self.sweep_locked(&mut state);
        if state.items.len() >= self.capacity {
            state.counters.refused += 1;
            self.save_locked(&state);
            return Err(SpaceFull {
                capacity: self.capacity,
                in_space: state.items.len(),
            }
            .into());
        }
        let item = Item {
            id: new_id(""),
            fields,
            at: self.now(),
            ttl,
            attempts: 0,
            note: note.to_string(),
        };
        state.items.push(item.clone());
        state.counters.out += 1;
        self.cv.notify_all();
        self.save_locked(&state);
        Ok(item)
    }

    /// Прочитать, не забирая. Ждёт до `timeout`.
    pub fn rd(
        &self,
        pattern: &Value,
        timeout: Duration,
    ) -> Result<Option<(Value, BTreeMap<String, Value>)>> {
        let deadline = Instant::now() + timeout;
        let mut state = self.lock();
        loop {
            self.sweep_locked(&mut state);
            if let Some((index, bindings)) = self.find(&state, pattern)? {
                state.counters.reads += 1;
                let fields = state.items[index].fields.clone();
                return Ok(Some((fields, bindings)));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            let pause = self
                .next_moment(&state)
                .map_or(remaining, |soon| soon.min(remaining));
            let (guard, _) = self
                .cv
                .wait_timeout(state, pause.max(Duration::from_millis(1)))
                .unwrap_or_else(|poison| poison.into_inner());
            state = guard;
        }
    }

    /// Забрать по шаблону. С арендой — «в работу»: другие не видят, но задача не потеряна.
    pub fn take(
        &self,
        pattern: &Value,
        timeout: Duration,
        lease: Option<f64>,
        owner: &str,
    ) -> Result<Option<Taken>> {
        let lease_seconds = lease.unwrap_or(self.default_lease).max(0.0);
        let deadline = Instant::now() + timeout;
        let mut state = self.lock();
        loop {
            self.sweep_locked(&mut state);
            if let Some((index, bindings)) = self.find(&state, pattern)? {
                let mut item = state.items.remove(index);
                item.attempts += 1;
                state.counters.taken += 1;
                let receipt = if lease_seconds > 0.0 {
                    let receipt = new_id("r-");
                    state.in_flight.insert(
                        receipt.clone(),
                        Flight {
                            receipt: receipt.clone(),
                            item: item.clone(),
                            owner: if owner.is_empty() {
                                "anonymous".to_string()
                            } else {
                                owner.to_string()
                            },
                            taken: self.now(),
                            deadline: self.now() + lease_seconds,
                        },
                    );
                    Some(receipt)
                } else {
                    None
                };
                let taken = Taken {
                    fields: item.fields.clone(),
                    bindings,
                    id: item.id.clone(),
                    attempts: item.attempts,
                    receipt,
                    lease_in: lease_seconds as u64,
                };
                self.cv.notify_all();
                self.save_locked(&state);
                return Ok(Some(taken));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            let pause = self
                .next_moment(&state)
                .map_or(remaining, |soon| soon.min(remaining));
            let (guard, _) = self
                .cv
                .wait_timeout(state, pause.max(Duration::from_millis(1)))
                .unwrap_or_else(|poison| poison.into_inner());
            state = guard;
        }
    }

    /// Подтвердить выполнение. С включённым правилом доказательство обязательно:
    /// «сделано» — это артефакт (вывод команды, URL, лог), а не заявление.
    /// Файл состояния (если пространство на диске).
    pub fn path(&self) -> Option<&std::path::Path> {
        self.path.as_deref()
    }

    /// Поля взятого кортежа по receipt (для приёмки без повторной передачи данных).
    pub fn flight_fields(&self, receipt: &str) -> Option<Value> {
        self.lock()
            .in_flight
            .get(receipt)
            .map(|flight| flight.item.fields.clone())
    }

    /// Сколько раз эту работу уже брали (для правил «после N неудач расторгаем»).
    pub fn flight_attempts(&self, receipt: &str) -> Option<u32> {
        self.lock()
            .in_flight
            .get(receipt)
            .map(|flight| flight.item.attempts)
    }

    pub fn ack(&self, receipt: &str, evidence: Option<&str>) -> Ack {
        let mut state = self.lock();
        if !state.in_flight.contains_key(receipt) {
            return Ack::Unknown;
        }
        let proof = evidence.map(str::trim).filter(|text| !text.is_empty());
        if self.require_evidence && proof.is_none() {
            return Ack::NoEvidence;
        }
        state.in_flight.remove(receipt);
        state.counters.acked += 1;
        self.save_locked(&state);
        Ack::Done(proof.map(str::to_string))
    }

    pub fn nack(&self, receipt: &str) -> bool {
        let mut state = self.lock();
        let Some(flight) = state.in_flight.remove(receipt) else {
            return false;
        };
        state.items.push(flight.item);
        state.counters.nacked += 1;
        self.cv.notify_all();
        self.save_locked(&state);
        true
    }

    pub fn extend(&self, receipt: &str, seconds: f64) -> Result<Option<u64>> {
        if seconds <= 0.0 {
            return Err(BadPattern::new("продление должно быть положительным").into());
        }
        let mut state = self.lock();
        let now = self.now();
        let Some(flight) = state.in_flight.get_mut(receipt) else {
            return Ok(None);
        };
        flight.deadline = flight.deadline.max(now + seconds);
        let lease_in = (flight.deadline - now).max(0.0) as u64;
        state.counters.extended += 1;
        self.save_locked(&state);
        Ok(Some(lease_in))
    }

    pub fn stats(&self) -> Stats {
        let mut state = self.lock();
        self.sweep_locked(&mut state);
        let now = self.now();
        let mut in_flight_rows: Vec<FlightRow> = state
            .in_flight
            .values()
            .map(|flight| FlightRow {
                receipt: flight.receipt.clone(),
                owner: flight.owner.clone(),
                fields: flight.item.fields.clone(),
                lease_in: (flight.deadline - now).max(0.0) as u64,
                attempts: flight.item.attempts,
            })
            .collect();
        in_flight_rows.sort_by(|left, right| left.receipt.cmp(&right.receipt));
        Stats {
            in_space: state.items.len(),
            in_flight: state.in_flight.len(),
            capacity: self.capacity,
            default_lease: self.default_lease,
            counters: state.counters,
            in_flight_rows,
        }
    }

    pub fn dump(&self) -> Value {
        let mut state = self.lock();
        self.sweep_locked(&mut state);
        let now = self.now();
        let mut flights: Vec<Value> = state
            .in_flight
            .values()
            .map(|flight| {
                json!({
                    "receipt": flight.receipt,
                    "owner": flight.owner,
                    "fields": flight.item.fields,
                    "attempts": flight.item.attempts,
                    "deadline_in": (flight.deadline - now).max(0.0) as u64,
                })
            })
            .collect();
        flights.sort_by_key(|row| row["receipt"].as_str().unwrap_or("").to_string());
        json!({
            "in_space": state.items,
            "in_flight": flights,
        })
    }
}

/// Атомарная запись: снимок либо старый, либо новый, но не половинчатый.
fn write_atomic(path: &Path, state: &State) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let payload = serde_json::to_vec_pretty(state)?;
    let tmp = path.with_extension(format!("tmp{}", std::process::id()));
    fs::write(&tmp, payload)?;
    fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    struct TestClock {
        millis: AtomicU64,
    }

    impl TestClock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                millis: AtomicU64::new(1_000_000),
            })
        }
        fn advance(&self, seconds: f64) {
            self.millis
                .fetch_add((seconds * 1000.0) as u64, Ordering::SeqCst);
        }
        fn as_clock(self: &Arc<Self>) -> Clock {
            let clone = Arc::clone(self);
            Arc::new(move || clone.millis.load(Ordering::SeqCst) as f64 / 1000.0)
        }
    }

    fn space(clock: &Arc<TestClock>, capacity: usize) -> Space {
        Space::with_clock(None, capacity, 30.0, clock.as_clock())
    }

    fn fields(value: Value) -> Value {
        value
    }

    // --- шаблоны ---
    #[test]
    fn exact_fields_match() {
        let bindings = matches(
            &json!(["translate", "doc-1"]),
            &json!(["translate", "doc-1"]),
        )
        .unwrap()
        .unwrap();
        assert!(bindings.is_empty());
    }

    #[test]
    fn wildcard_matches_any() {
        assert!(
            matches(&json!(["translate", "*"]), &json!(["translate", {"a": 1}]))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn binding_returns_value() {
        let bindings = matches(
            &json!(["translate", "?doc"]),
            &json!(["translate", "doc-7"]),
        )
        .unwrap()
        .unwrap();
        assert_eq!(bindings.get("doc").unwrap(), &json!("doc-7"));
    }

    #[test]
    fn same_binding_name_must_be_equal() {
        assert!(matches(&json!(["?x", "?x"]), &json!([1, 1]))
            .unwrap()
            .is_some());
        assert!(matches(&json!(["?x", "?x"]), &json!([1, 2]))
            .unwrap()
            .is_none());
    }

    #[test]
    fn length_mismatch_is_not_a_match() {
        assert!(matches(&json!(["a"]), &json!(["a", "b"]))
            .unwrap()
            .is_none());
    }

    #[test]
    fn bool_and_number_are_different() {
        assert!(matches(&json!([true]), &json!([1])).unwrap().is_none());
    }

    #[test]
    fn non_array_pattern_is_refused() {
        assert!(matches(&json!("translate"), &json!(["translate"])).is_err());
    }

    #[test]
    fn empty_binding_name_is_refused() {
        assert!(matches(&json!(["?", "x"]), &json!([1, 2])).is_err());
    }

    // --- операции ---
    #[test]
    fn out_then_rd_does_not_remove() {
        let clock = TestClock::new();
        let space = space(&clock, 10);
        space
            .out(fields(json!(["ocr", "scan-7"])), None, "")
            .unwrap();
        let found = space
            .rd(&json!(["ocr", "?x"]), Duration::ZERO)
            .unwrap()
            .unwrap();
        assert_eq!(found.0, json!(["ocr", "scan-7"]));
        assert_eq!(space.stats().in_space, 1);
    }

    #[test]
    fn take_without_lease_removes_forever() {
        let clock = TestClock::new();
        let space = space(&clock, 10);
        space.out(json!(["job"]), None, "").unwrap();
        let taken = space
            .take(&json!(["job"]), Duration::ZERO, Some(0.0), "A")
            .unwrap()
            .unwrap();
        assert!(taken.receipt.is_none());
        assert_eq!(space.stats().in_space, 0);
    }

    #[test]
    fn take_with_lease_hides_from_others() {
        let clock = TestClock::new();
        let space = space(&clock, 10);
        space.out(json!(["job"]), None, "").unwrap();
        let taken = space
            .take(&json!(["job"]), Duration::ZERO, Some(30.0), "A")
            .unwrap()
            .unwrap();
        assert!(taken.receipt.is_some());
        assert_eq!(space.stats().in_flight, 1);
        assert!(space.rd(&json!(["job"]), Duration::ZERO).unwrap().is_none());
        assert!(space
            .take(&json!(["job"]), Duration::ZERO, Some(30.0), "B")
            .unwrap()
            .is_none());
    }

    #[test]
    fn lease_expiry_returns_task_with_attempt_count() {
        let clock = TestClock::new();
        let space = space(&clock, 10);
        space.out(json!(["translate", "doc-1"]), None, "").unwrap();
        space
            .take(&json!(["translate", "*"]), Duration::ZERO, Some(10.0), "A")
            .unwrap();
        clock.advance(11.0);
        assert_eq!(space.stats().in_flight, 0);
        assert_eq!(space.stats().counters.returned, 1);
        let again = space
            .take(&json!(["translate", "*"]), Duration::ZERO, Some(10.0), "B")
            .unwrap()
            .unwrap();
        assert_eq!(again.fields, json!(["translate", "doc-1"]));
        assert_eq!(again.attempts, 2, "видно, что попытка вторая");
    }

    #[test]
    fn ack_removes_forever() {
        let clock = TestClock::new();
        let space = space(&clock, 10);
        space.out(json!(["job"]), None, "").unwrap();
        let taken = space
            .take(&json!(["job"]), Duration::ZERO, Some(10.0), "A")
            .unwrap()
            .unwrap();
        assert!(matches!(
            space.ack(taken.receipt.as_deref().unwrap(), None),
            Ack::Done(_)
        ));
        clock.advance(100.0);
        assert_eq!(space.stats().in_space, 0);
        assert_eq!(space.stats().in_flight, 0);
    }

    #[test]
    fn nack_returns_immediately() {
        let clock = TestClock::new();
        let space = space(&clock, 10);
        space.out(json!(["job"]), None, "").unwrap();
        let taken = space
            .take(&json!(["job"]), Duration::ZERO, Some(600.0), "A")
            .unwrap()
            .unwrap();
        assert!(space.nack(taken.receipt.as_deref().unwrap()));
        assert_eq!(space.stats().in_space, 1, "nack не ждёт аренды");
    }

    #[test]
    fn extend_pushes_deadline() {
        let clock = TestClock::new();
        let space = space(&clock, 10);
        space.out(json!(["job"]), None, "").unwrap();
        let taken = space
            .take(&json!(["job"]), Duration::ZERO, Some(10.0), "A")
            .unwrap()
            .unwrap();
        let extended = space
            .extend(taken.receipt.as_deref().unwrap(), 100.0)
            .unwrap()
            .unwrap();
        assert_eq!(extended, 100);
        clock.advance(60.0);
        assert_eq!(space.stats().in_flight, 1, "продлённая аренда жива");
    }

    #[test]
    fn unknown_receipts_are_refused() {
        let clock = TestClock::new();
        let space = space(&clock, 10);
        assert_eq!(space.ack("r-нет", None), Ack::Unknown);
        assert!(!space.nack("r-нет"));
        assert!(space.extend("r-нет", 10.0).unwrap().is_none());
    }

    #[test]
    fn returned_task_goes_to_the_tail() {
        let clock = TestClock::new();
        let space = space(&clock, 10);
        space.out(json!(["first"]), None, "").unwrap();
        space.out(json!(["second"]), None, "").unwrap();
        space
            .take(&json!(["first"]), Duration::ZERO, Some(5.0), "A")
            .unwrap();
        clock.advance(6.0);
        let next = space.rd(&json!(["*"]), Duration::ZERO).unwrap().unwrap();
        assert_eq!(next.0, json!(["second"]), "вернувшаяся задача не первая");
    }

    #[test]
    fn ttl_drops_tuple_without_reader() {
        let clock = TestClock::new();
        let space = space(&clock, 10);
        space.out(json!(["news"]), Some(5.0), "").unwrap();
        clock.advance(6.0);
        assert_eq!(space.stats().in_space, 0);
        assert_eq!(space.stats().counters.dropped, 1);
    }

    #[test]
    fn capacity_refuses_with_reason() {
        let clock = TestClock::new();
        let space = space(&clock, 2);
        space.out(json!(["a"]), None, "").unwrap();
        space.out(json!(["b"]), None, "").unwrap();
        let refusal = space.out(json!(["c"]), None, "").unwrap_err();
        match refusal {
            Error::Full(full) => assert_eq!(full.capacity, 2),
            other => panic!("ожидался отказ по ёмкости, получено {other:?}"),
        }
        assert_eq!(space.stats().counters.refused, 1);
    }

    #[test]
    fn capacity_does_not_count_in_flight() {
        let clock = TestClock::new();
        let space = space(&clock, 1);
        space.out(json!(["a"]), None, "").unwrap();
        space
            .take(&json!(["a"]), Duration::ZERO, Some(30.0), "A")
            .unwrap();
        space.out(json!(["b"]), None, "").unwrap();
        let stats = space.stats();
        assert_eq!((stats.in_space, stats.in_flight), (1, 1));
    }

    #[test]
    fn rd_blocks_until_producer_appears() {
        let clock = TestClock::new();
        let space = Arc::new(space(&clock, 10));
        let producer = {
            let space = Arc::clone(&space);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(80));
                space.out(json!(["late"]), None, "").unwrap();
            })
        };
        let found = space
            .rd(&json!(["late"]), Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert_eq!(found.0, json!(["late"]));
        producer.join().unwrap();
    }

    #[test]
    fn take_is_atomic_under_concurrency() {
        let clock = TestClock::new();
        let space = Arc::new(space(&clock, 10));
        space.out(json!(["only-one"]), None, "").unwrap();
        let mut handles = Vec::new();
        for _ in 0..8 {
            let space = Arc::clone(&space);
            handles.push(std::thread::spawn(move || {
                space
                    .take(
                        &json!(["only-one"]),
                        Duration::from_millis(150),
                        Some(0.0),
                        "A",
                    )
                    .unwrap()
                    .is_some()
            }));
        }
        let winners = handles
            .into_iter()
            .filter(|_| true)
            .map(|handle| handle.join().unwrap())
            .filter(|won| *won)
            .count();
        assert_eq!(winners, 1, "одну задачу может взять ровно один агент");
        assert_eq!(space.stats().counters.taken, 1);
    }

    #[test]
    fn state_survives_restart() {
        let clock = TestClock::new();
        let dir = std::env::temp_dir().join(format!("tupleboard-test-{}", std::process::id()));
        let path = dir.join("space.json");
        let _ = fs::remove_file(&path);
        {
            let space = Space::with_clock(Some(path.clone()), 10, 30.0, clock.as_clock());
            space.out(json!(["translate", "doc-1"]), None, "").unwrap();
            let taken = space
                .take(&json!(["translate", "*"]), Duration::ZERO, Some(30.0), "A")
                .unwrap()
                .unwrap();
            space
                .extend(taken.receipt.as_deref().unwrap(), 300.0)
                .unwrap();
        }
        let again = Space::with_clock(Some(path.clone()), 10, 30.0, clock.as_clock());
        let stats = again.stats();
        assert_eq!(stats.in_flight, 1, "аренда пережила перезапуск");
        assert_eq!(stats.in_flight_rows[0].owner, "A");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn expired_lease_returns_right_after_restart() {
        let clock = TestClock::new();
        let dir = std::env::temp_dir().join(format!("tupleboard-test-exp-{}", std::process::id()));
        let path = dir.join("space.json");
        let _ = fs::remove_file(&path);
        {
            let space = Space::with_clock(Some(path.clone()), 10, 30.0, clock.as_clock());
            space.out(json!(["job"]), None, "").unwrap();
            space
                .take(&json!(["job"]), Duration::ZERO, Some(10.0), "A")
                .unwrap();
        }
        clock.advance(11.0);
        let again = Space::with_clock(Some(path.clone()), 10, 30.0, clock.as_clock());
        assert_eq!(again.stats().in_space, 1, "на старте уборка вернула задачу");
        assert_eq!(again.stats().in_flight, 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn ack_without_evidence_is_refused_when_rule_is_on() {
        let clock = TestClock::new();
        let space =
            Space::with_clock(None, 10, 30.0, clock.as_clock()).with_evidence_required(true);
        space.out(json!(["job"]), None, "").unwrap();
        let taken = space
            .take(&json!(["job"]), Duration::ZERO, Some(30.0), "A")
            .unwrap()
            .unwrap();
        let receipt = taken.receipt.as_deref().unwrap();
        assert_eq!(space.ack(receipt, None), Ack::NoEvidence);
        assert_eq!(
            space.ack(receipt, Some("   ")),
            Ack::NoEvidence,
            "пробелы — не доказательство"
        );
        assert_eq!(
            space.stats().in_flight,
            1,
            "работа не закрыта и не потеряна"
        );
        assert!(matches!(
            space.ack(receipt, Some("cargo test → 25 passed")),
            Ack::Done(Some(_))
        ));
        assert_eq!(space.stats().in_flight, 0);
    }

    #[test]
    fn evidence_is_optional_by_default() {
        let clock = TestClock::new();
        let space = space(&clock, 10);
        space.out(json!(["job"]), None, "").unwrap();
        let taken = space
            .take(&json!(["job"]), Duration::ZERO, Some(30.0), "A")
            .unwrap()
            .unwrap();
        assert!(matches!(
            space.ack(taken.receipt.as_deref().unwrap(), None),
            Ack::Done(None)
        ));
    }

    #[test]
    fn counters_count_what_happened() {
        let clock = TestClock::new();
        let space = space(&clock, 10);
        space.out(json!(["x"]), None, "").unwrap();
        space.rd(&json!(["x"]), Duration::ZERO).unwrap();
        let taken = space
            .take(&json!(["x"]), Duration::ZERO, Some(5.0), "A")
            .unwrap()
            .unwrap();
        space
            .extend(taken.receipt.as_deref().unwrap(), 5.0)
            .unwrap();
        space.ack(taken.receipt.as_deref().unwrap(), None);
        let counters = space.stats().counters;
        assert_eq!((counters.out, counters.reads, counters.taken), (1, 1, 1));
        assert_eq!((counters.acked, counters.extended), (1, 1));
    }
}
