use super::*;
use crate::{config::PersonalityTransparency, model_config::Provider};

fn configured(url: &str, structured: bool) -> Arc<AppState> {
    let mut s = state(url, false);
    let c = Arc::make_mut(&mut Arc::make_mut(&mut s).config);
    c.personality_transparency = PersonalityTransparency::Extrovert;
    c.routing.provider = "mock".into();
    c.routing.providers.insert(
        "mock".into(),
        Provider {
            base_url: url.into(),
            api_key_env: None,
            structured_output: structured,
            ..Default::default()
        },
    );
    s
}

fn reasoned(content: &str, reason: &str) -> Value {
    let mut v = message(content);
    v["choices"][0]["message"]["reasoning_content"] = json!(reason);
    v
}

#[tokio::test]
async fn invalid_decision_is_corrected_without_leaking_reasoning_or_constraining_final_answer() {
    for structured in [true, false] {
        for stream in [true, false] {
            let (provider, script) = upstream(vec![
                reasoned("plain prose, not JSON", "INVALID_THOUGHT"),
                reasoned(
                    r#"{"decision":"fallback","chat_narration_allowed":false}"#,
                    "VALID_THOUGHT",
                ),
                message("report"),
                message("FINAL"),
            ])
            .await;
            let s = configured(&provider.url, structured);
            let server = proxy(s.clone()).await;
            let response = request(&server, "Return only FINAL", stream).await;
            assert_eq!(response.status(), 200);
            let text = response.text().await.unwrap();
            assert!(text.contains("FINAL"), "{text}");
            assert!(text.contains("VALID_THOUGHT"));
            assert!(!text.contains("INVALID_THOUGHT"));
            let seen = &script.lock().unwrap().1;
            assert_eq!(seen.len(), 4);
            for v in &seen[..2] {
                assert_eq!(v.get("response_format").is_some(), structured);
                if structured {
                    assert_eq!(v["response_format"]["type"], "json_schema");
                }
            }
            assert!(
                seen[1]["messages"].as_array().unwrap().last().unwrap()["content"]
                    .as_str()
                    .unwrap()
                    .contains("validation failed")
            );
            assert!(seen[2].get("response_format").is_none());
            assert!(seen[3].get("response_format").is_none());
        }
    }
}

#[tokio::test]
async fn repeated_bad_decisions_fail_after_exactly_three_without_actions() {
    let (provider, script) = upstream(vec![reasoned("not JSON", "NEVER_EXPOSE"); 3]).await;
    let s = configured(&provider.url, true);
    let server = proxy(s.clone()).await;
    let response = request(&server, "task", true).await;
    let text = response.text().await.unwrap();
    assert!(text.contains("routing protocol failure"), "{text}");
    assert!(text.contains("after 3 attempts"));
    assert!(!text.contains("NEVER_EXPOSE"));
    assert!(!text.contains("data: [DONE]"));
    assert_eq!(script.lock().unwrap().1.len(), 3);
    assert!(s.portfolio.list().unwrap().is_empty());
}

#[tokio::test]
async fn unsupported_schema_is_not_silently_downgraded_or_rerouted() {
    let (provider, script) = upstream(vec![
        json!({"__status":400,"error":{"message":"response_format unsupported"}}),
    ])
    .await;
    let server = proxy(configured(&provider.url, true)).await;
    let response = request(&server, "task", false).await;
    assert_eq!(response.status(), 503);
    assert_eq!(script.lock().unwrap().1.len(), 1);
}

#[tokio::test]
async fn corrections_share_one_total_budget() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = serve(
        Router::new()
            .route(
                "/chat/completions",
                post(|State(calls): State<Arc<AtomicUsize>>| async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Json(message("invalid"))
                }),
            )
            .with_state(calls.clone()),
    )
    .await;
    let mut s = configured(&provider.url, false);
    let c = Arc::make_mut(&mut Arc::make_mut(&mut s).config);
    c.upstream.total_timeout_ms = 250;
    c.upstream.first_response_timeout_ms = 400;
    let server = proxy(s).await;
    let start = Instant::now();
    let text = request(&server, "task", false).await.text().await.unwrap();
    assert!(text.contains("routing total_timeout"), "{text}");
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(calls.load(Ordering::SeqCst) <= 3);
}

#[test]
fn schema_rejects_unavailable_ids_tools_and_malformed_arguments_before_action() {
    let s = state("http://unused", false);
    let context = crate::policy::Context {
        client: "test".into(),
        policy: crate::policy::CreationPolicy::AlwaysPostExecution,
        phase: Some("post".into()),
        orders: vec![],
        locked: false,
    };
    let schema = crate::foreman_protocol::schema(&s, &context).unwrap();
    for v in [
        json!({"decision":"use","agent_id":"missing"}),
        json!({"decision":"create","requirements":"not allowed"}),
        json!({"decision":"tool","name":"set_personality","arguments":{"mode":"extrovert","user_instruction":"switch"}}),
        json!({"decision":"tool","name":"get_statistics","arguments":{"unexpected":true}}),
        json!({"decision":"fallback","chat_narration_allowed":"yes"}),
        json!({"decision":"fallback","unrecognized":true}),
    ] {
        assert!(
            crate::foreman_protocol::validate(&v.to_string(), &schema).is_err(),
            "{v}"
        );
    }
    assert!(
        crate::foreman_protocol::validate(
            r#"{"decision":"tool","name":"get_statistics","arguments":{}}"#,
            &schema
        )
        .is_ok()
    );
    assert!(crate::foreman_protocol::validate("prose {}", &schema).is_err());
}
