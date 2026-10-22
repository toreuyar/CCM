//! Workforce acceptance scenarios; execute only when authorized.
use super::*;
use crate::observe::now_ms;

#[test]
fn rolling_windows_count_attempts_and_idempotent_assignments_at_boundaries() {
    let state = state("http://127.0.0.1:1", false);
    let agent = insert(&state);
    state
        .portfolio
        .workforce_usage(&agent.id, "assignment", "task", "r")
        .unwrap();
    state
        .portfolio
        .workforce_usage(&agent.id, "assignment", "task", "continuation")
        .unwrap();
    let now = now_ms() + 1000;
    let conn = state.portfolio.conn.lock().unwrap();
    for (i, hours) in [0, 1, 6, 24, 168, 720].iter().enumerate() {
        conn.execute(
            "INSERT INTO workforce_usage(entity_id,kind,invocation_key,request_id,at_ms) VALUES(?1,'model_call',?2,'r',?3)",
            rusqlite::params![agent.id, format!("attempt-{i}"), now - hours * 3600000],
        )
        .unwrap();
    }
    // Future timestamps are not part of any current window.
    conn.execute(
        "INSERT INTO workforce_usage(entity_id,kind,invocation_key,request_id,at_ms) VALUES(?1,'model_call','future','r',?2)",
        rusqlite::params![agent.id, now + 1],
    )
    .unwrap();
    drop(conn);
    let snapshot = state.portfolio.workforce(now).unwrap();
    let entity = &snapshot["entities"][0];
    for (i, window) in ["1h", "6h", "1d", "7d", "30d"].iter().enumerate() {
        assert_eq!(entity["usage"][window]["assignment"], 1);
        assert_eq!(entity["usage"][window]["model_call"], i + 1);
    }
    assert_eq!(
        state.portfolio.workforce(now + 31 * 86400000).unwrap()["entities"][0]["usage"]["30d"]["assignment"],
        0
    );
}

#[tokio::test]
async fn retired_cleanup_keeps_evidence_and_skips_model_evaluation() {
    let state = state("http://127.0.0.1:1", false);
    let agent = insert(&state);
    let now = now_ms();
    state
        .portfolio
        .trace_snapshot(&agent.id, "agent", &json!(agent))
        .unwrap();
    state
        .portfolio
        .workforce_usage(&agent.id, "assignment", "a", "r")
        .unwrap();
    {
        let conn = state.portfolio.conn.lock().unwrap();
        conn.execute("UPDATE agents SET retired=1 WHERE id=?1", [&agent.id])
            .unwrap();
        conn.execute(
            "UPDATE agents SET retired_ms=?2 WHERE id=?1",
            rusqlite::params![agent.id, now],
        )
        .unwrap();
        conn.execute("INSERT INTO evaluations(request_id,entity_id,entity_kind,verdict,before_definition,applied) VALUES('r',?1,'agent','{\"action\":\"retire\"}',?2,1)",rusqlite::params![agent.id,json!(agent).to_string()]).unwrap();
    }
    assert!(state.portfolio.get(&agent.id).unwrap().is_none());
    let error = crate::meta::evaluate(
        &state,
        "r",
        "agent",
        &agent.id,
        &json!(agent),
        "task",
        "result",
        0,
    )
    .await
    .unwrap_err();
    assert!(error.contains("evaluation skipped"));
    assert_eq!(
        state
            .portfolio
            .purge_retired_agents(now + crate::workforce::RETENTION_MS - 1)
            .unwrap(),
        0
    );
    assert_eq!(
        state.portfolio.workforce(now).unwrap()["entities"][0]["retired"],
        true
    );
    assert_eq!(
        state
            .portfolio
            .purge_retired_agents(now + crate::workforce::RETENTION_MS)
            .unwrap(),
        1
    );
    assert!(
        state.portfolio.workforce(now).unwrap()["entities"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let conn = state.portfolio.conn.lock().unwrap();
    for table in ["definition_versions", "evaluations", "workforce_usage"] {
        let count: i64 = conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE entity_id=?1"),
                [&agent.id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(count > 0, "lost history from {table}");
    }
}

#[tokio::test]
async fn catalogue_and_sse_observe_creation_without_an_execution() {
    use futures::StreamExt;
    let state = state("http://127.0.0.1:1", false);
    let server = serve(crate::web::routes().with_state(state.clone())).await;
    let client = reqwest::Client::new();
    let initial: Value = client
        .get(format!("{}/api/workforce", server.url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(initial["entities"].as_array().unwrap().is_empty());
    state
        .portfolio
        .trace_emit("", None, "test.ready", &json!({}))
        .unwrap();
    let response = client
        .get(format!(
            "{}/api/events?after={}",
            server.url, initial["cursor"]
        ))
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();
    tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let agent = insert(&state);
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut bytes = Vec::new();
        while !String::from_utf8_lossy(&bytes).contains("workforce.changed") {
            bytes.extend_from_slice(&stream.next().await.unwrap().unwrap());
        }
    })
    .await
    .expect("creation event did not reach idle subscriber");
    let snapshot: Value = client
        .get(format!("{}/api/workforce", server.url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(snapshot["entities"].as_array().unwrap().len(), 1);
    assert_eq!(
        snapshot["entities"][0]["definition"]["system_prompt"],
        agent.system_prompt
    );
    assert!(snapshot["cursor"].as_i64() > initial["cursor"].as_i64());
}

#[test]
fn legacy_retirement_without_evidence_gets_a_fresh_clock() {
    let path =
        std::env::temp_dir().join(format!("ccm-workforce-legacy-{}.db", uuid::Uuid::new_v4()));
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE agents(id TEXT PRIMARY KEY,name TEXT NOT NULL,purpose TEXT NOT NULL,system_prompt TEXT NOT NULL,model TEXT NOT NULL,created_at INTEGER NOT NULL,usage_count INTEGER NOT NULL DEFAULT 0,last_used_at INTEGER,retired INTEGER NOT NULL DEFAULT 0);
            INSERT INTO agents(id,name,purpose,system_prompt,model,created_at,retired) VALUES('legacy','legacy','p','s','m',0,1);").unwrap();
    }
    let before = now_ms();
    {
        let portfolio = crate::portfolio::Portfolio::open(path.to_str().unwrap()).unwrap();
        let snapshot = portfolio.workforce(now_ms()).unwrap();
        assert!(snapshot["entities"][0]["retired_ms"].as_i64().unwrap() >= before);
        assert_eq!(portfolio.purge_retired_agents(now_ms()).unwrap(), 0);
    }
    std::fs::remove_file(path).unwrap();
}

#[test]
fn usage_and_retirement_clock_survive_reopen() {
    let path = std::env::temp_dir().join(format!("ccm-workforce-{}.db", uuid::Uuid::new_v4()));
    let id;
    {
        let portfolio = crate::portfolio::Portfolio::open(path.to_str().unwrap()).unwrap();
        let agent = portfolio
            .insert(&NewAgent {
                provider: String::new(),
                name: "persisted".into(),
                purpose: "p".into(),
                system_prompt: "s".into(),
                model: "m".into(),
            })
            .unwrap();
        id = agent.id;
        portfolio
            .workforce_usage(&id, "assignment", "task", "r")
            .unwrap();
        portfolio
            .conn
            .lock()
            .unwrap()
            .execute("UPDATE agents SET retired=1 WHERE id=?1", [&id])
            .unwrap();
    }
    {
        let portfolio = crate::portfolio::Portfolio::open(path.to_str().unwrap()).unwrap();
        let snapshot = portfolio.workforce(now_ms()).unwrap();
        assert_eq!(snapshot["entities"][0]["usage"]["1h"]["assignment"], 1);
        assert!(snapshot["entities"][0]["retired_ms"].is_i64());
    }
    std::fs::remove_file(path).unwrap();
}
