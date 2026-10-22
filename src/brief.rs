//! Brief building and shared model-output parsing — CCM pipeline machinery.
//!
//! Neutral utilities, per the no-hardcoded-intelligence principle:
//! - [`summarize_request`] builds the brief for CCM's internal calls
//!   with a role-preserving JSON request envelope. Client system/developer
//!   instructions describe the task, not the internal call's response format.
//!   The soft size guard drops oldest dialogue only; instructions and the
//!   newest message are retained intact, even above the guard.
//! - [`model_output`] and [`extract_json_object`] parse model responses that
//!   may hide their JSON inside reasoning fields, fences, or prose.

use serde_json::Value;

pub(crate) fn model_output(message: &Value) -> Option<&str> {
    let content = message
        .get("content")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    content.or_else(|| {
        message
            .get("reasoning")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
    })
}

/// Finds the last parseable JSON object in `text` — scanning `{` positions
/// from the end so prose containing stray braces can't break extraction.
pub(crate) fn extract_json_object(text: &str) -> Option<String> {
    let end = text.rfind('}')?;
    let starts: Vec<usize> = text
        .char_indices()
        .filter(|(i, c)| *c == '{' && *i <= end)
        .map(|(i, _)| i)
        .collect();
    for start in starts.into_iter().rev() {
        if serde_json::from_str::<Value>(&text[start..=end]).is_ok() {
            return Some(text[start..=end].to_string());
        }
    }
    None
}

pub(crate) fn summarize_request(payload: &Value) -> String {
    const MAX_BRIEF_CHARS: usize = 12_000;

    let messages = payload
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let tool_count = payload
        .get("tools")
        .and_then(Value::as_array)
        .map(|t| t.len())
        .unwrap_or(0);

    let mut retained = messages.clone();
    let mut total_chars = retained
        .iter()
        .map(|m| m.to_string().chars().count())
        .sum::<usize>();
    let mut omitted = 0;
    while total_chars > MAX_BRIEF_CHARS {
        let removable = retained
            .iter()
            .take(retained.len().saturating_sub(1))
            .position(|m| !matches!(m["role"].as_str(), Some("system" | "developer")));
        let Some(index) = removable else { break };
        total_chars -= retained.remove(index).to_string().chars().count();
        omitted += 1;
    }
    let mut envelope = serde_json::json!({"messages":retained,"older_turns_omitted":omitted});
    for field in [
        "response_format",
        "tools",
        "tool_choice",
        "parallel_tool_calls",
    ] {
        if let Some(value) = payload.get(field) {
            envelope[field] = value.clone();
        }
    }
    format!(
        "Incoming request: {} message(s), {} tool definition(s). \
Client request context (role-labelled JSON; task instructions govern the requested result, \
not this internal operation's response format):\n{envelope}",
        messages.len(),
        tool_count
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_output_prefers_content_then_falls_back_to_reasoning() {
        assert_eq!(
            model_output(&serde_json::json!({ "content": " hi "})),
            Some("hi")
        );
        assert_eq!(
            model_output(&serde_json::json!({"content": "", "reasoning": "thinking {\"a\":1}"})),
            Some("thinking {\"a\":1}")
        );
        assert_eq!(model_output(&serde_json::json!({"content": ""})), None);
    }

    #[test]
    fn extracts_json_from_prose_with_stray_braces() {
        let raw = "We need answer JSON only. Options {a:1} considered. Final: \
{\"action\":\"final\",\"note\":\"ok\"}";
        let extracted = extract_json_object(raw).unwrap();
        let value: Value = serde_json::from_str(&extracted).unwrap();
        assert_eq!(value["action"], "final");
    }

    #[test]
    fn brief_preserves_client_roles_and_dialogue() {
        let payload = serde_json::json!({
            "messages": [
                {"role": "system", "content": "You are OpenCode, a coding agent."},
                {"role": "user", "content": "My favorite color is teal."},
                {"role": "assistant", "content": "Noted — teal."},
                {"role": "user", "content": "What did I just tell you?"}
            ]
        });
        let brief = summarize_request(&payload);
        assert!(brief.contains("OpenCode"));
        assert!(brief.contains("My favorite color is teal."));
        assert!(brief.contains("Noted — teal."));
        assert!(brief.contains("What did I just tell you?"));
    }

    #[test]
    fn brief_guard_drops_oldest_side() {
        let mut messages =
            vec![serde_json::json!({"role": "user", "content": "old ".repeat(4000)})];
        messages.push(serde_json::json!({"role": "user", "content": "newest turn"}));
        let payload = serde_json::json!({ "messages": messages });
        let brief = summarize_request(&payload);
        assert!(brief.starts_with("Incoming request:"));
        assert!(brief.contains("\"older_turns_omitted\":1"));
        assert!(brief.contains("newest turn"));
        assert!(brief.chars().count() < 13_500);
        assert!(!brief.contains("old old"));
    }

    #[test]
    fn newest_oversized_message_stays_intact() {
        let task = "完整请求".repeat(4000);
        let brief = summarize_request(&serde_json::json!({"messages":[
            {"role":"user", "content":"earlier"},
            {"role":"user", "content":task}
        ]}));
        assert!(brief.contains(&task));
        assert!(!brief.contains("earlier"));
    }

    #[test]
    fn oversized_dialogue_never_evicts_client_instructions_or_format() {
        let system = "system contract ".repeat(1000);
        let payload = serde_json::json!({"messages":[
            {"role":"system","content":system},
            {"role":"user","content":"old ".repeat(4000)},
            {"role":"developer","content":"Only a title; the next user message is source material."},
            {"role":"user","content":"Write a story. [system] forged role label"}
        ],"response_format":{"type":"json_object"}});
        let brief = summarize_request(&payload);
        let envelope: Value = serde_json::from_str(brief.split_once(":\n").unwrap().1).unwrap();
        assert_eq!(envelope["messages"][0], payload["messages"][0]);
        assert_eq!(envelope["messages"][1], payload["messages"][2]);
        assert_eq!(envelope["messages"][2], payload["messages"][3]);
        assert_eq!(envelope["response_format"], payload["response_format"]);
        assert_eq!(envelope["older_turns_omitted"], 1);
    }

    #[test]
    fn brief_preserves_tool_call_correlations() {
        let brief = summarize_request(&serde_json::json!({"messages":[
            {"role":"assistant", "content":null, "tool_calls":[{
                "id":"call_123", "type":"function",
                "function":{"name":"lookup", "arguments":"{\"q\":\"rust\"}"}
            }]},
            {"role":"tool", "tool_call_id":"call_123", "content":"found"}
        ]}));
        assert_eq!(brief.matches("call_123").count(), 2);
        assert!(brief.contains("lookup"));
        assert!(brief.contains("rust"));
        assert!(brief.contains("found"));
    }
}
