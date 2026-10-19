//! Events: what happened to calls, conferences, legs and threads (`call.created`, `dtmf`,
//! `play.finished`, `thread.stalled`, ...). How they reach other services is for the
//! service's own API, still to be described. Until then they go to the log (debug level:
//! they carry phone-call details and digits), and tests capture them.

use std::sync::Mutex;

use serde_json::Value;

pub struct Events {
    /// Keeps events in memory (tests, embedding).
    captured: Option<Mutex<Vec<(String, Value)>>>,
}

impl Events {
    /// Events go to the log only.
    pub fn log_only() -> Events {
        Events { captured: None }
    }

    /// Keeps published events in memory; read them with `captured`.
    pub fn capture() -> Events {
        Events { captured: Some(Mutex::new(Vec::new())) }
    }

    /// Events kept by `capture`, as (key, event).
    pub fn captured(&self) -> Vec<(String, Value)> {
        self.captured.as_ref().map(|c| c.lock().unwrap().clone()).unwrap_or_default()
    }

    /// An event, keyed by its call or conference id (or the node, for thread events).
    pub fn publish(&self, key: &str, event: &Value) {
        if let Some(c) = &self.captured {
            c.lock().unwrap().push((key.to_string(), event.clone()));
            return;
        }
        tracing::debug!(key, event = %event, "event");
    }
}
