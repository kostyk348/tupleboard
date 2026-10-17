//! Договор на работу: карточка, которая сама знает, как её принять.
//!
//! Чем это отличается от канбана — не словами, а механикой:
//!
//! | канбан | здесь |
//! |---|---|
//! | исполнителя выбирает диспетчер по строке `assignee` | работу **находит** клетка по своей возможности (`capability`), диспетчера нет |
//! | статус `done` ставит тот, кто работал | приёмку **выполняет система**: проверка из договора запускается и решает |
//! | упавший воркер оставляет карточку в `running` до ручного `reclaim` | аренда истекает и договор возвращается сам |
//! | очередь бесконечна | приём ограничен ёмкостью: 429 вместо тихого роста |
//! | зависимости — отдельные рёбра в БД | `depends_on` внутри договора; занятый зависимостью договор уходит в хвост, а не блокирует клетку |
//! | «работает» = заявление | «принято» = проверка вернула 0 / файл на месте / URL ответил 2xx |
//!
//! Договор в пространстве — один кортеж с фиксированной формой полей:
//! `["pact", capability, priority, id, <договор целиком>]`.
//! Такая форма даёт ассоциативный поиск: «любой договор на `rust`» — это шаблон
//! `["pact", "rust", "*", "*", "*"]`, без индексов и без диспетчера.

use crate::space::{Ack, Error as SpaceError, Space};
use crate::team::{audit, Evidence, Policy, Verdict};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Сколько раз можно не пройти приёмку, прежде чем договор будет расторгнут.
pub const MAX_ACCEPT_FAILURES: u32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pact {
    pub id: String,
    pub capability: String,
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub accepts: Evidence,
    #[serde(default)]
    pub note: String,
}

impl Pact {
    pub fn fields(&self) -> Value {
        json!(["pact", self.capability, self.priority, self.id, self])
    }

    fn from_fields(fields: &Value) -> Option<Pact> {
        let row = fields.as_array()?;
        if row.len() != 5 {
            return None;
        }
        serde_json::from_value(row[4].clone()).ok()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Claim {
    pub receipt: String,
    pub pact: Pact,
    pub attempts: u32,
    pub lease_in: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Finish {
    /// Приёмка прошла: договор закрыт, id попал в список сделанного.
    Accepted { evidence: String },
    /// Приёмка не прошла: договор вернулся в пространство (или расторгнут после N попыток).
    Rejected {
        why: String,
        returned: bool,
        attempts: u32,
    },
    /// Такого receipt нет.
    Unknown,
}

/// Доска договоров: тонкий слой над пространством. Состояние «сделано» живёт здесь.
pub struct PactBoard {
    space: Arc<Space>,
    policy: Policy,
    done: Mutex<HashSet<String>>,
    accepted: AtomicU64,
    rejected: AtomicU64,
    gave_up: AtomicU64,
}

impl PactBoard {
    pub fn new(space: Arc<Space>, policy: Policy) -> Self {
        let done = load_done(&space);
        Self {
            space,
            policy,
            done: Mutex::new(done),
            accepted: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            gave_up: AtomicU64::new(0),
        }
    }

    /// Список «сделано» лежит рядом с состоянием: перезапуск не должен ложно разблокировать зависимости.
    fn save_done(&self) {
        let Some(path) = self.space.path() else {
            return;
        };
        let done = self
            .done
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        let target = done_path(path);
        let mut ids: Vec<String> = done.into_iter().collect();
        ids.sort();
        let _ = std::fs::write(target, serde_json::to_vec_pretty(&ids).unwrap_or_default());
    }

    pub fn space(&self) -> &Arc<Space> {
        &self.space
    }

    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    /// Выложить договор. Приём ограничен ёмкостью пространства — честный 429, а не рост.
    pub fn add(&self, pact: &Pact) -> Result<String, SpaceError> {
        if pact.id.trim().is_empty() {
            return Err(SpaceError::Bad(crate::space::BadPattern::new(
                "у договора должен быть id",
            )));
        }
        let item = self.space.out(pact.fields(), None, &pact.note)?;
        Ok(item.id)
    }

    /// Забрать договор по возможности. Диспетчера нет: клетка сама говорит, что умеет.
    pub fn claim(
        &self,
        capability: &str,
        owner: &str,
        lease: Option<f64>,
    ) -> Result<Option<Claim>, SpaceError> {
        let pattern = json!(["pact", capability, "*", "*", "*"]);
        let mut guard = 0usize;
        let limit = self.space.stats().in_space + 1;
        let mut seen: HashSet<String> = HashSet::new();
        loop {
            let taken = self.space.take(&pattern, Duration::ZERO, lease, owner)?;
            let Some(taken) = taken else {
                return Ok(None);
            };
            let Some(receipt) = taken.receipt.clone() else {
                return Err(SpaceError::Bad(crate::space::BadPattern::new(
                    "договор взят без аренды: так нельзя, работа должна возвращаться",
                )));
            };
            let Some(pact) = Pact::from_fields(&taken.fields) else {
                // не наш кортеж — вернуть как был и не мешать
                self.space.nack(&receipt);
                guard += 1;
                if guard >= limit {
                    return Ok(None);
                }
                continue;
            };
            if !seen.insert(pact.id.clone()) {
                // круг замкнулся: всё, что осталось, ждёт зависимостей — не занимаем клетку
                self.space.nack(&receipt);
                return Ok(None);
            }
            let missing: Vec<String> = pact
                .depends_on
                .iter()
                .filter(|dep| !self.is_done(dep))
                .cloned()
                .collect();
            if !missing.is_empty() {
                // зависимость не готова: договор уходит в хвост, клетка не занята
                self.space.nack(&receipt);
                guard += 1;
                if guard >= limit {
                    return Ok(None);
                }
                continue;
            }
            return Ok(Some(Claim {
                receipt,
                pact,
                attempts: taken.attempts,
                lease_in: taken.lease_in,
            }));
        }
    }

    fn is_done(&self, id: &str) -> bool {
        self.done
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .contains(id)
    }

    /// Финал договора. Приёмку выполняет система, а не тот, кто работал.
    pub fn finish(&self, receipt: &str, fields: &Value) -> Result<Finish, SpaceError> {
        let Some(pact) = Pact::from_fields(fields) else {
            return Err(SpaceError::Bad(crate::space::BadPattern::new(
                "это не договор: ожидается кортеж [\"pact\", capability, priority, id, pact]",
            )));
        };
        let attempts = self.space.flight_attempts(receipt).unwrap_or(1);
        let verdict = audit(&pact.accepts, &self.policy);
        let text = format!("{}: {}", pact.accepts.kind, pact.accepts.value);
        match verdict {
            Verdict::Confirmed => match self.space.ack(receipt, Some(&text)) {
                Ack::Done(_) => {
                    self.done
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .insert(pact.id.clone());
                    self.accepted.fetch_add(1, Ordering::Relaxed);
                    self.save_done();
                    Ok(Finish::Accepted { evidence: text })
                }
                Ack::Unknown | Ack::NoEvidence => Ok(Finish::Unknown),
            },
            Verdict::Mismatch(why) | Verdict::Unverifiable(why) => {
                if attempts >= MAX_ACCEPT_FAILURES {
                    // договор расторгается: бесконечно возвращать одно и то же — не работа
                    let _ = self
                        .space
                        .ack(receipt, Some("расторгнут после N провалов приёмки"));
                    self.done
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .remove(&pact.id);
                    self.gave_up.fetch_add(1, Ordering::Relaxed);
                    self.save_done();
                    return Ok(Finish::Rejected {
                        why,
                        returned: false,
                        attempts,
                    });
                }
                self.space.nack(receipt);
                self.rejected.fetch_add(1, Ordering::Relaxed);
                Ok(Finish::Rejected {
                    why,
                    returned: true,
                    attempts,
                })
            }
        }
    }

    pub fn status(&self) -> Value {
        let done = self
            .done
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len();
        let stats = self.space.stats();
        json!({
            "done": done,
            "accepted": self.accepted.load(Ordering::Relaxed),
            "rejected": self.rejected.load(Ordering::Relaxed),
            "gave_up": self.gave_up.load(Ordering::Relaxed),
            "in_space": stats.in_space,
            "in_flight": stats.in_flight,
            "capacity": stats.capacity,
            "space_counters": stats.counters,
        })
    }
}

fn done_path(space_path: &std::path::Path) -> std::path::PathBuf {
    space_path.with_extension("done.json")
}

fn load_done(space: &Space) -> HashSet<String> {
    let Some(path) = space.path() else {
        return HashSet::new();
    };
    std::fs::read_to_string(done_path(path))
        .ok()
        .and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
        .map(|ids| ids.into_iter().collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::space::Space;

    fn board(capacity: usize) -> PactBoard {
        PactBoard::new(
            Arc::new(Space::new(None, capacity, 30.0)),
            Policy {
                allowed_commands: vec!["true".into()],
                algedonic_per_hour: 3,
            },
        )
    }

    fn pact(id: &str, capability: &str, kind: &str, value: &str) -> Pact {
        Pact {
            id: id.into(),
            capability: capability.into(),
            priority: 0,
            depends_on: vec![],
            accepts: Evidence {
                kind: kind.into(),
                value: value.into(),
                repro: String::new(),
            },
            note: String::new(),
        }
    }

    fn temp_path(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("pact-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("artifact.txt")
    }

    #[test]
    fn claim_matches_by_capability_not_by_assignee() {
        let board = board(10);
        board.add(&pact("a", "rust", "file", "/tmp/x")).unwrap();
        board.add(&pact("b", "python", "file", "/tmp/y")).unwrap();
        let claim = board.claim("rust", "cell-1", Some(30.0)).unwrap().unwrap();
        assert_eq!(
            claim.pact.id, "a",
            "клетка получает работу по своей возможности"
        );
        assert_eq!(claim.attempts, 1);
    }

    #[test]
    fn accepted_contract_closes_and_marks_done() {
        let board = board(10);
        let path = temp_path("accept");
        std::fs::write(&path, "готово").unwrap();
        board
            .add(&pact("a", "rust", "file", &path.display().to_string()))
            .unwrap();
        let claim = board.claim("rust", "cell", Some(30.0)).unwrap().unwrap();
        let outcome = board.finish(&claim.receipt, &claim.pact.fields()).unwrap();
        assert!(matches!(outcome, Finish::Accepted { .. }), "{outcome:?}");
        assert!(
            board.is_done("a"),
            "зависимые договоры теперь разблокированы"
        );
        assert_eq!(board.status()["in_space"], json!(0));
    }

    #[test]
    fn failed_acceptance_returns_contract_instead_of_closing_it() {
        let board = board(10);
        board
            .add(&pact("a", "rust", "file", "/tmp/артефакта-нет-pact"))
            .unwrap();
        let claim = board.claim("rust", "cell", Some(30.0)).unwrap().unwrap();
        let outcome = board.finish(&claim.receipt, &claim.pact.fields()).unwrap();
        match outcome {
            Finish::Rejected {
                returned,
                attempts,
                why,
            } => {
                assert!(returned, "договор возвращён в пространство");
                assert_eq!(attempts, 1);
                assert!(why.contains("файла нет"), "{why}");
            }
            other => panic!("ожидался отказ приёмки, получено {other:?}"),
        }
        assert!(!board.is_done("a"));
        assert_eq!(board.status()["in_space"], json!(1), "работа не потеряна");
    }

    #[test]
    fn contract_is_terminated_after_three_failed_acceptances() {
        let board = board(10);
        board
            .add(&pact("a", "rust", "file", "/tmp/артефакта-нет-pact"))
            .unwrap();
        for attempt in 1..=MAX_ACCEPT_FAILURES {
            let claim = board.claim("rust", "cell", Some(30.0)).unwrap().unwrap();
            let outcome = board.finish(&claim.receipt, &claim.pact.fields()).unwrap();
            match outcome {
                Finish::Rejected {
                    returned, attempts, ..
                } => {
                    assert_eq!(attempts, attempt);
                    assert_eq!(
                        returned,
                        attempt < MAX_ACCEPT_FAILURES,
                        "на последней попытке расторгаем"
                    );
                }
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(board.status()["gave_up"], json!(1));
        assert_eq!(
            board.status()["in_space"],
            json!(0),
            "расторгнутый договор не висит вечно"
        );
    }

    #[test]
    fn dependencies_gate_the_claim() {
        let board = board(10);
        let path = temp_path("deps");
        std::fs::write(&path, "ок").unwrap();
        let mut first = pact("first", "rust", "file", &path.display().to_string());
        let mut second = pact("second", "rust", "file", &path.display().to_string());
        second.depends_on = vec!["first".into()];
        board.add(&first).unwrap();
        board.add(&second).unwrap();
        let claim = board.claim("rust", "cell", Some(30.0)).unwrap().unwrap();
        assert_eq!(
            claim.pact.id, "first",
            "пока зависимость не сделана, второй не выдаётся"
        );
        board.finish(&claim.receipt, &claim.pact.fields()).unwrap();
        let next = board.claim("rust", "cell", Some(30.0)).unwrap().unwrap();
        assert_eq!(next.pact.id, "second");
        let _ = first.id;
    }

    #[test]
    fn blocked_contracts_do_not_occupy_the_cell() {
        let board = board(10);
        let path = temp_path("blocked");
        std::fs::write(&path, "ок").unwrap();
        let mut blocked = pact("blocked", "rust", "file", &path.display().to_string());
        blocked.depends_on = vec!["далёкое-будущее".into()];
        board.add(&blocked).unwrap();
        assert!(
            board.claim("rust", "cell", Some(30.0)).unwrap().is_none(),
            "клетка не занята договором, который ещё нельзя делать"
        );
        assert_eq!(board.status()["in_space"], json!(1), "и договор не потерян");
        assert_eq!(board.status()["in_flight"], json!(0));
        assert!(
            board.space().stats().counters.taken <= 2,
            "заблокированный договор не крутится по кругу: взятий {}",
            board.space().stats().counters.taken
        );
    }

    #[test]
    fn done_survives_board_restart() {
        let dir = std::env::temp_dir().join(format!("pact-restart-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = dir.join("space.json");
        let artifact = dir.join("artifact.txt");
        std::fs::write(&artifact, "ок").unwrap();
        let policy = Policy::default();
        {
            let space = Arc::new(Space::new(Some(store.clone()), 10, 30.0));
            let board = PactBoard::new(Arc::clone(&space), policy.clone());
            board
                .add(&pact(
                    "base",
                    "rust",
                    "file",
                    &artifact.display().to_string(),
                ))
                .unwrap();
            let claim = board.claim("rust", "cell", Some(30.0)).unwrap().unwrap();
            board.finish(&claim.receipt, &claim.pact.fields()).unwrap();
        }
        let space = Arc::new(Space::new(Some(store.clone()), 10, 30.0));
        let board = PactBoard::new(space, policy);
        assert!(
            board.is_done("base"),
            "перезапуск не должен забывать сделанное"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn admission_is_bounded_by_capacity() {
        let board = board(1);
        board.add(&pact("a", "rust", "file", "/tmp/x")).unwrap();
        let refusal = board.add(&pact("b", "rust", "file", "/tmp/y")).unwrap_err();
        assert!(matches!(refusal, SpaceError::Full(_)), "{refusal:?}");
    }

    #[test]
    fn contract_without_id_is_refused() {
        let board = board(1);
        let refusal = board.add(&pact("", "rust", "file", "/tmp/x")).unwrap_err();
        assert!(matches!(refusal, SpaceError::Bad(_)));
    }
}
