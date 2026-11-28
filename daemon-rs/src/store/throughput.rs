use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{json, Value};
use std::collections::BTreeMap;

const SCHEMA_VERSION: u32 = 2;
const WINDOW_MS: i64 = 2 * 60 * 60 * 1000;
const RETENTION_MS: i64 = 48 * 60 * 60 * 1000;

#[derive(Clone, Serialize, Deserialize)]
struct Sample {
    at: i64,
    tokens: u64,
    duration_ms: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct PersistedThroughput {
    version: u32,
    samples: BTreeMap<String, BTreeMap<String, Vec<Sample>>>,
}

#[derive(Clone, Default)]
pub(super) struct Throughput(BTreeMap<String, BTreeMap<String, Vec<Sample>>>);

impl Serialize for Throughput {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        PersistedThroughput {
            version: SCHEMA_VERSION,
            samples: self.0.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Throughput {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let persisted = PersistedThroughput::deserialize(deserializer)?;
        if persisted.version != SCHEMA_VERSION {
            return Err(serde::de::Error::custom(format!(
                "unsupported throughput schema version {}",
                persisted.version
            )));
        }
        Ok(Self(persisted.samples))
    }
}

impl Throughput {
    pub(super) fn record(
        &mut self,
        agent: &str,
        model: &str,
        at: i64,
        tokens: u64,
        duration_ms: u64,
    ) {
        if tokens == 0 || duration_ms == 0 {
            return;
        }
        let samples = self
            .0
            .entry(agent.into())
            .or_default()
            .entry(model.into())
            .or_default();
        samples.push(Sample {
            at,
            tokens,
            duration_ms,
        });
        let latest = samples.iter().map(|s| s.at).max().unwrap_or(at);
        samples.retain(|s| s.at >= latest.saturating_sub(WINDOW_MS));
    }

    pub(super) fn prune(&mut self, now: i64) -> bool {
        let mut changed = false;
        self.0.retain(|_, models| {
            models.retain(|_, samples| {
                let keep = samples
                    .iter()
                    .any(|s| s.at > now.saturating_sub(RETENTION_MS));
                changed |= !keep;
                keep
            });
            !models.is_empty()
        });
        changed
    }

    pub(super) fn rates(&self, now: i64) -> Vec<Value> {
        let mut rates = Vec::new();
        for (agent, models) in &self.0 {
            for (model, samples) in models {
                let Some(latest) = samples.iter().map(|s| s.at).max() else {
                    continue;
                };
                let expires = latest.saturating_add(RETENTION_MS);
                if now >= expires {
                    continue;
                }
                let tokens: f64 = samples.iter().map(|s| s.tokens as f64).sum();
                let duration: f64 = samples.iter().map(|s| s.duration_ms as f64).sum();
                if duration > 0.0 {
                    rates.push(json!({
                        "agent": agent,
                        "model": model,
                        "tokensPerSecond": tokens * 1000.0 / duration,
                        "expiresAt": expires,
                    }));
                }
            }
        }
        rates
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_latest_model_window_and_expires_at_two_days() {
        let mut t = Throughput::default();
        t.record("a", "m", 0, 1000, 1000);
        t.record("a", "m", 1, 20, 1000);
        t.record("a", "m", WINDOW_MS + 1, 100, 2000);
        t.record("b", "m", WINDOW_MS + 2, 7, 1000);
        let expiry = WINDOW_MS + 1 + RETENTION_MS;
        assert_eq!(t.rates(expiry - 1)[0]["tokensPerSecond"], 40.0);
        assert!(t.prune(expiry));
        assert_eq!(t.rates(expiry).len(), 1);
        assert_eq!(t.rates(expiry)[0]["agent"], "b");
    }

    #[test]
    fn ignores_zero_token_or_zero_duration_samples() {
        let mut t = Throughput::default();
        t.record("a", "m", 10, 0, 5000);
        t.record("a", "m", 20, 100, 0);
        assert!(t.rates(30).is_empty());
        t.record("a", "m", 30, 100, 2000);
        assert_eq!(t.rates(40)[0]["tokensPerSecond"], 50.0);
    }

    #[test]
    fn rejects_legacy_unversioned_format() {
        let legacy = r#"{"claude":{"m":[{"at":1000,"tokens":10,"duration_ms":600000}]}}"#;
        assert!(serde_json::from_str::<Throughput>(legacy).is_err());

        let mut t = Throughput::default();
        t.record("claude", "m", 1000, 100, 2000);
        let encoded = serde_json::to_string(&t).unwrap();
        let decoded: Throughput = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.rates(1000)[0]["tokensPerSecond"], 50.0);
    }
}
