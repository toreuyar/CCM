//! Debug commands: flag-gated, exact-match shortcuts that bypass the Foreman.
//!
//! A debug command fires only when the last message is a user message whose
//! content is byte-for-byte identical to the command string — no trimming, no
//! case-folding, no pattern matching. The registry below is the single place
//! where commands are declared; each returns a programmatic result payload.

use serde_json::{Value, json};

use crate::state::AppState;

pub struct DebugOutcome {
    pub command: &'static str,
    pub result: Value,
}

/// Checks the payload against the exact-match command registry. Returns
/// `None` when nothing matches, in which case the normal pipeline runs.
pub fn handle(state: &AppState, payload: &Value) -> Option<Result<DebugOutcome, String>> {
    let content = last_user_content(payload)?;
    match content.as_str() {
        "debug-delete" => Some(delete_all(state)),
        _ => None,
    }
}

fn delete_all(state: &AppState) -> Result<DebugOutcome, String> {
    let removed = state
        .portfolio
        .reset()
        .map_err(|e| format!("portfolio wipe failed: {e}"))?;
    Ok(DebugOutcome {
        command: "debug-delete",
        result: json!({
            "debug_command": "debug-delete",
            "deleted": removed,
            "status": "ok",
            "system_state": "default"
            ,"history_retained":true
        }),
    })
}

fn last_user_content(payload: &Value) -> Option<String> {
    let messages = payload.get("messages")?.as_array()?;
    let last = messages.last()?;
    if last.get("role").and_then(Value::as_str) != Some("user") {
        return None;
    }
    last.get("content")?.as_str().map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::portfolio::{NewAgent, Portfolio};
    use std::sync::Arc;

    fn state() -> AppState {
        let config = Config {
            routing: Default::default(),
            api_key: "k".into(),
            upstream_base_url: "https://ollama.com/v1".into(),
            model: "deepseek-v4.1-flash".into(),
            host: "127.0.0.1".into(),
            port: 8080,
            upstream: crate::config::UpstreamConfig::default(),
            portfolio_db: ":memory:".into(),
            max_iterations: 2,
            debug_enabled: true,
            creation: crate::config::CreationConfig::default(),
            improvement: crate::improvement::ImprovementConfig::default(),
            personality_transparency: crate::config::PersonalityTransparency::default(),
            memory: crate::config::MemoryConfig::default(),
            shutdown: crate::config::ShutdownConfig::default(),
        };
        AppState {
            providers: Default::default(),
            config: Arc::new(config),
            http: reqwest::Client::new(),
            portfolio: Arc::new(Portfolio::open(":memory:").unwrap()),
            external_api_enabled: true,
            auth: None,
        }
    }

    fn payload(content: &str) -> Value {
        json!({"model": "any", "messages": [{"role": "user", "content": content}]})
    }

    #[test]
    fn exact_match_deletes_all_agents() {
        let state = state();
        state
            .portfolio
            .insert(&NewAgent {
                provider: String::new(),
                name: "a".into(),
                purpose: "p".into(),
                system_prompt: "s".into(),
                model: "m".into(),
            })
            .unwrap();
        state
            .portfolio
            .insert(&NewAgent {
                provider: String::new(),
                name: "b".into(),
                purpose: "p".into(),
                system_prompt: "s".into(),
                model: "m".into(),
            })
            .unwrap();

        let outcome = handle(&state, &payload("debug-delete")).unwrap().unwrap();
        assert_eq!(outcome.command, "debug-delete");
        assert_eq!(outcome.result["deleted"]["agents"], 2);
        assert_eq!(outcome.result["status"], "ok");
        assert_eq!(outcome.result["system_state"], "default");
        assert!(state.portfolio.list().unwrap().is_empty());
    }

    #[test]
    fn near_matches_do_not_fire() {
        let state = state();
        for content in [
            " debug-delete",
            "debug-delete ",
            "Debug-delete",
            "debug-delete\n",
            "debug-deletex",
            "debug",
        ] {
            assert!(
                handle(&state, &payload(content)).is_none(),
                "fired on: {content:?}"
            );
        }
    }

    #[test]
    fn empty_portfolio_deletes_zero() {
        let state = state();
        let outcome = handle(&state, &payload("debug-delete")).unwrap().unwrap();
        assert_eq!(outcome.result["deleted"]["agents"], 0);
    }

    #[test]
    fn non_user_last_message_never_fires() {
        let state = state();
        let p = json!({"messages": [
            {"role": "user", "content": "debug-delete"},
            {"role": "assistant", "content": "debug-delete"}
        ]});
        assert!(handle(&state, &p).is_none());
    }

    #[test]
    fn non_string_content_never_fires() {
        let state = state();
        let p = json!({"messages": [{"role": "user", "content": [{"type": "text"}]}]});
        assert!(handle(&state, &p).is_none());
    }
}
