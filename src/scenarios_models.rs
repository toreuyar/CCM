use super::*;
use crate::config::{PersonalityTransparency, ServerConfig};
use crate::model_config::{Assignment, Candidate, Model, Overrides, Provider};

fn routed(a: &str, b: &str) -> Arc<AppState> {
    let mut s = state(a, false);
    let config = Arc::make_mut(&mut Arc::make_mut(&mut s).config);
    config.routing.providers.insert(
        "a".into(),
        Provider {
            structured_output: false,
            base_url: a.into(),
            api_key_env: None,
            ..Default::default()
        },
    );
    config.routing.providers.insert(
        "b".into(),
        Provider {
            structured_output: false,
            base_url: b.into(),
            api_key_env: None,
            ..Default::default()
        },
    );
    for (id, provider, inference) in [("public-a", "a", "actual-a"), ("public-b", "b", "actual-b")]
    {
        config.routing.models.insert(
            id.into(),
            Model {
                settings: Overrides {
                    provider: Some(provider.into()),
                    model: Some(inference.into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
    }
    config.routing.default_model = "public-a".into();
    config.initialize_models().unwrap();
    s
}

#[test]
fn documented_model_examples_parse_and_reject_unknown_settings() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for name in [
        "ccm.example.json",
        "examples/ccm.local-provider.json",
        "examples/ccm.multiple-providers.json",
    ] {
        ServerConfig::load(&root.join(name), true).unwrap();
    }
    let server =
        ServerConfig::load(&root.join("examples/ccm.multiple-providers.json"), true).unwrap();
    assert_eq!(
        server.routing.models["ccm-local"]
            .settings
            .provider
            .as_deref(),
        Some("local")
    );
    assert_eq!(
        server.routing.providers["local"].base_url,
        "http://127.0.0.1:11434/v1"
    );
    assert!(serde_json::from_value::<Model>(json!({"memory":{"budget_mib":64}})).is_err());
}

#[test]
fn nested_model_and_agent_inheritance_is_field_level_and_secrets_are_not_serialized() {
    let mut s = routed("http://127.0.0.1:11/v1", "http://127.0.0.1:12/v1");
    let config = Arc::make_mut(&mut Arc::make_mut(&mut s).config);
    config.api_key = "synthetic-secret".into();
    config
        .routing
        .models
        .get_mut("public-b")
        .unwrap()
        .settings
        .upstream
        .idle_timeout_ms = Some(1700);
    config.routing.agents.insert(
        "foreman".into(),
        Overrides {
            model: Some("role-model".into()),
            ..Default::default()
        },
    );
    let selected = config.select_model(Some("public-b")).unwrap();
    assert_eq!(selected.upstream.idle_timeout_ms, 1700);
    assert_eq!(
        selected.upstream.total_timeout_ms,
        config.upstream.total_timeout_ms
    );
    let agent = selected
        .for_agent("foreman", "foreman", None, None)
        .unwrap();
    assert_eq!(agent.inference_model(), "role-model");
    assert_eq!(agent.routing.provider, "b");
    assert_eq!(agent.upstream.idle_timeout_ms, 1700);
    assert!(!json!(config).to_string().contains("synthetic-secret"));
    assert_ne!(
        config
            .select_model(Some("public-a"))
            .unwrap()
            .client_scope("alice"),
        selected.client_scope("alice")
    );
}

#[tokio::test]
async fn exposed_models_route_each_role_and_reject_unknown_models() {
    let (a, sa) = upstream(vec![
        message(r#"{"decision":"fallback"}"#),
        message("report-a"),
        message("final-a"),
    ])
    .await;
    let (b, sb) = upstream(vec![
        message(r#"{"decision":"fallback"}"#),
        message("report-b"),
        message("final-b"),
    ])
    .await;
    let s = routed(&a.url, &b.url);
    let server = proxy(s.clone()).await;
    for (id, expected) in [("public-a", "final-a"), ("public-b", "final-b")] {
        let response = reqwest::Client::new()
            .post(format!("{}/v1/chat/completions", server.url))
            .json(&json!({"model":id,"messages":[{"role":"user","content":"task"}]}))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body: Value = response.json().await.unwrap();
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["model"], id);
        assert_eq!(body["choices"][0]["message"]["content"], expected);
    }
    assert!(
        sa.lock()
            .unwrap()
            .1
            .iter()
            .all(|v| v["model"] == "actual-a")
    );
    assert!(
        sb.lock()
            .unwrap()
            .1
            .iter()
            .all(|v| v["model"] == "actual-b")
    );
    let unknown = reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", server.url))
        .json(&json!({"model":"nope","messages":[{"role":"user","content":"task"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(unknown.status(), 400);
    let listed: Value = reqwest::get(format!("{}/v1/models", server.url))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["data"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn generated_assignment_routes_to_separate_provider_and_catalog_is_presented() {
    let (a, sa) = upstream(vec![]).await;
    let (b, sb) = upstream(vec![message("specialist-report")]).await;
    let mut s = routed(&a.url, &b.url);
    let config = Arc::make_mut(&mut Arc::make_mut(&mut s).config);
    config.routing.agent_models.insert(
        "long-context".into(),
        Candidate {
            provider: "b".into(),
            model: "specialist-b".into(),
            context_tokens: 32000,
            capabilities: vec!["test reasoning".into()],
            reasoning: "Administrator test guidance".into(),
        },
    );
    let selected =
        crate::model_config::with_config(&s, s.config.select_model(Some("public-a")).unwrap())
            .unwrap();
    let mut candidate = definition();
    candidate["model_choice"] =
        json!({"catalog_id":"long-context","reason":"Fits requested context"});
    sa.lock().unwrap().0 = vec![message(&candidate.to_string())].into();
    let agent = crate::meta::create_agent(&selected, "create", "requirements: use long-context", 0)
        .await
        .unwrap();
    assert_eq!(agent.provider, "b");
    assert_eq!(agent.model, "specialist-b");
    let observed = sa.lock().unwrap().1.clone();
    assert!(
        observed[0]["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("Administrator test guidance")
    );
    let value = crate::loop_runner::agent_call(
        &selected,
        &agent,
        &[json!({"role":"user","content":"task"})],
        &json!({}),
        "agent-test",
    )
    .await
    .unwrap();
    assert_eq!(value["content"], "specialist-report");
    assert_eq!(sb.lock().unwrap().1[0]["model"], "specialist-b");
    candidate["model_choice"]["catalog_id"] = json!("invented");
    assert!(crate::meta::parse_assigned_agent(&candidate, &selected.config, None).is_err());
}

#[test]
fn generated_default_and_revision_preserve_assignment_without_catalog() {
    let mut s = routed("http://127.0.0.1:11/v1", "http://127.0.0.1:12/v1");
    Arc::make_mut(&mut Arc::make_mut(&mut s).config)
        .routing
        .generated_agent_default = Assignment {
        provider: Some("b".into()),
        model: Some("default-specialist".into()),
    };
    let agent = crate::meta::parse_assigned_agent(&definition(), &s.config, None).unwrap();
    assert_eq!(agent.model, "default-specialist");
    assert_eq!(agent.provider, "b");
    let existing = json!({"model":"original","provider":"a"});
    let revised =
        crate::meta::parse_assigned_agent(&definition(), &s.config, Some(&existing)).unwrap();
    assert_eq!(revised.model, "original");
    assert_eq!(revised.provider, "a");
}

#[test]
fn preferences_migrate_once_and_remain_model_scoped() {
    let s = routed("http://127.0.0.1:11/v1", "http://127.0.0.1:12/v1");
    s.portfolio
        .set_client_personality("alice", "extrovert")
        .unwrap();
    let run = s
        .portfolio
        .trace_begin("old", &json!({"messages":[]}), "external")
        .unwrap();
    s.portfolio
        .bind_policy("old", &run, "alice", s.config.creation.policy)
        .unwrap();
    s.portfolio.migrate_model_preferences("public-a").unwrap();
    s.portfolio.migrate_model_preferences("public-a").unwrap();
    let a = s
        .config
        .select_model(Some("public-a"))
        .unwrap()
        .client_scope("alice");
    let b = s
        .config
        .select_model(Some("public-b"))
        .unwrap()
        .client_scope("alice");
    assert_eq!(s.portfolio.policy_context("old").unwrap().client, a);
    assert_eq!(
        s.portfolio.client_personality(&a).unwrap(),
        PersonalityTransparency::Extrovert
    );
    assert_eq!(
        s.portfolio.client_personality(&b).unwrap(),
        PersonalityTransparency::Introvert
    );
}

#[test]
fn run_and_call_snapshots_preserve_resolved_provider_and_settings() {
    let s = routed("http://127.0.0.1:11/v1", "http://127.0.0.1:12/v1");
    let selected = s.config.select_model(Some("public-b")).unwrap();
    s.portfolio.save_model_snapshot("run", &selected).unwrap();
    let old = s.portfolio.call_config("run", "agent", &selected).unwrap();
    let mut changed = selected.clone();
    changed.routing.inference_model = "new-model".into();
    s.portfolio.save_model_snapshot("run", &changed).unwrap();
    let saved = s.portfolio.model_snapshot("run").unwrap().unwrap();
    assert_eq!(saved.inference_model(), "actual-b");
    assert_eq!(
        s.portfolio
            .call_config("run", "agent", &changed)
            .unwrap()
            .inference_model(),
        old.inference_model()
    );
}

#[tokio::test]
async fn role_override_and_tool_continuation_keep_originating_model_and_provider() {
    let (a, sa) = upstream(vec![]).await;
    let (b, sb) = upstream(vec![
        message(r#"{"decision":"fallback"}"#),
        message("final"),
    ])
    .await;
    let mut s = routed(&a.url, &b.url);
    Arc::make_mut(&mut Arc::make_mut(&mut s).config)
        .routing
        .agents
        .insert(
            "foreman".into(),
            Overrides {
                provider: Some("b".into()),
                model: Some("planner".into()),
                ..Default::default()
            },
        );
    sa.lock().unwrap().0 = vec![tool_message(), message("after tool")].into();
    let server = proxy(s.clone()).await;
    let client = reqwest::Client::new();
    let mut payload = tool_payload();
    payload["model"] = json!("public-a");
    let response = client
        .post(format!("{}/v1/chat/completions", server.url))
        .json(&payload)
        .send()
        .await
        .unwrap();
    let first: Value = response.json().await.unwrap();
    assert_eq!(
        first["choices"][0]["finish_reason"], "tool_calls",
        "{first}"
    );
    let assistant = first["choices"][0]["message"].clone();
    let id = assistant["tool_calls"][0]["id"].clone();
    payload["messages"].as_array_mut().unwrap().extend([
        assistant,
        json!({"role":"tool","tool_call_id":id,"content":"observed"}),
    ]);
    payload["model"] = json!("public-b");
    assert_eq!(
        client
            .post(format!("{}/v1/chat/completions", server.url))
            .json(&payload)
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    payload["model"] = json!("public-a");
    let changed = Arc::make_mut(&mut Arc::make_mut(&mut s).config);
    changed
        .routing
        .models
        .get_mut("public-a")
        .unwrap()
        .settings
        .model = Some("changed".into());
    changed.routing.agents.clear();
    let restarted = proxy(s.clone()).await;
    let response = client
        .post(format!("{}/v1/chat/completions", restarted.url))
        .json(&payload)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let final_body: Value = response.json().await.unwrap();
    assert_eq!(status, 200, "{final_body}");
    assert_eq!(final_body["choices"][0]["message"]["content"], "final");
    assert!(
        sa.lock()
            .unwrap()
            .1
            .iter()
            .all(|v| v["model"] == "actual-a")
    );
    assert!(sb.lock().unwrap().1.iter().all(|v| v["model"] == "planner"));
}

#[tokio::test]
async fn evaluator_can_reassign_only_to_catalog_models() {
    let (a, sa) = upstream(vec![]).await;
    let mut s = routed(&a.url, &a.url);
    Arc::make_mut(&mut Arc::make_mut(&mut s).config)
        .routing
        .agent_models
        .insert(
            "review".into(),
            Candidate {
                provider: "b".into(),
                model: "review-model".into(),
                context_tokens: 16000,
                capabilities: vec!["review".into()],
                reasoning: "Admin rationale".into(),
            },
        );
    let selected =
        crate::model_config::with_config(&s, s.config.select_model(Some("public-a")).unwrap())
            .unwrap();
    let agent = insert(&selected);
    let mut replacement = definition();
    replacement["model_choice"] =
        json!({"catalog_id":"review","reason":"Observed need for review specialization"});
    sa.lock().unwrap().0=vec![message(&json!({"action":"revise","reason":"model fit","evidence":"observed result","replacement":replacement}).to_string())].into();
    crate::meta::evaluate(
        &selected,
        "eval",
        "agent",
        &agent.id,
        &json!(agent),
        "task",
        "report",
        0,
    )
    .await
    .unwrap();
    let revised = selected.portfolio.get(&agent.id).unwrap().unwrap();
    assert_eq!(revised.provider, "b");
    assert_eq!(revised.model, "review-model");
    assert!(
        sa.lock().unwrap().1[0]["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("Admin rationale")
    );
}

#[test]
fn legacy_migration_changes_live_agents_but_keeps_historical_snapshots() {
    let path = std::env::temp_dir().join(format!("model-migrate-{}.db", uuid::Uuid::new_v4()));
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE agents(id TEXT PRIMARY KEY,name TEXT NOT NULL,purpose TEXT NOT NULL,system_prompt TEXT NOT NULL,model TEXT NOT NULL,created_at INTEGER NOT NULL,usage_count INTEGER NOT NULL DEFAULT 0,last_used_at INTEGER); INSERT INTO agents(id,name,purpose,system_prompt,model,created_at) VALUES('legacy','legacy','p','original','old-provider-model',0);").unwrap();
    }
    {
        let db = Portfolio::open(path.to_str().unwrap()).unwrap();
        let agent = db.get("legacy").unwrap().unwrap();
        assert_eq!(agent.model, "");
        assert_eq!(agent.provider, "");
        assert_eq!(agent.system_prompt, "original");
        let conn = db.conn.lock().unwrap();
        let historical:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM definition_versions WHERE definition LIKE '%old-provider-model%')",[],|r|r.get(0)).unwrap();
        assert!(historical);
    }
    std::fs::remove_file(&path).unwrap();
    let _ = std::fs::remove_file(format!("{}.ccm-lock", path.display()));
}
