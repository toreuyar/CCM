//! Bounded serialized read-model cache. Cold GET work runs off runtime threads;
//! live event cursors bypass response caching and read the memory-backed database.
use crate::memory::{Budget, Lease};
use axum::{
    body::{Body, to_bytes},
    http::{HeaderMap, Request, StatusCode},
    middleware::Next,
    response::Response,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

struct Entry {
    body: bytes::Bytes,
    headers: HeaderMap,
    at: Instant,
    _lease: Lease,
}
struct Cache {
    entries: Mutex<HashMap<String, Arc<Entry>>>,
    budget: Arc<Budget>,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
}
static CACHE: OnceLock<Cache> = OnceLock::new();
static READERS: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(4));
pub async fn read_permit() -> tokio::sync::SemaphorePermit<'static> {
    READERS.acquire().await.unwrap()
}
pub fn init(limit: usize) {
    let _ = CACHE.set(Cache {
        entries: Mutex::new(HashMap::new()),
        budget: Budget::new(limit),
        hits: std::sync::atomic::AtomicU64::new(0),
        misses: std::sync::atomic::AtomicU64::new(0),
    });
}
pub fn metrics() -> serde_json::Value {
    CACHE.get().map(|c| serde_json::json!({"bytes":c.budget.used(),"limit_bytes":c.budget.limit(),"entries":c.entries.lock().unwrap().len(),
        "hits":c.hits.load(std::sync::atomic::Ordering::Relaxed),"misses":c.misses.load(std::sync::atomic::Ordering::Relaxed)})).unwrap_or_default()
}

pub async fn layer(request: Request<Body>, next: Next) -> Response {
    let Some(cache) = CACHE.get() else {
        return next.run(request).await;
    };
    cached(cache, request, next).await
}

async fn cached(cache: &Cache, request: Request<Body>, next: Next) -> Response {
    let path = request.uri().path();
    if request.method() != axum::http::Method::GET {
        let response = next.run(request).await;
        cache.entries.lock().unwrap().clear();
        return response;
    }
    if !path.starts_with("/api/") || path == "/api/events" || path == "/api/memory" {
        return next.run(request).await;
    }
    let partition = request
        .extensions()
        .get::<crate::auth::AuthContext>()
        .map(crate::auth::AuthContext::cache_partition)
        .unwrap_or_else(|| "unscoped-test-route".into());
    let key = format!("{partition}:{}", request.uri());
    let live_cursor = path == "/api/events/page";
    let cached = cache
        .entries
        .lock()
        .unwrap()
        .get(&key)
        .filter(|e| !live_cursor && e.at.elapsed() < Duration::from_millis(100))
        .cloned();
    if let Some(entry) = cached {
        cache
            .hits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut response = Response::new(Body::from(entry.body.clone()));
        *response.headers_mut() = entry.headers.clone();
        return response;
    }
    let runtime = tokio::runtime::Handle::current();
    cache
        .misses
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _permit = read_permit().await;
    let response =
        match tokio::task::spawn_blocking(move || runtime.block_on(next.run(request))).await {
            Ok(response) => response,
            Err(_) => return Response::builder().status(500).body(Body::empty()).unwrap(),
        };
    if response.status() != StatusCode::OK {
        return response;
    }
    let (parts, body) = response.into_parts();
    // GET endpoints materialize JSON already; cap cache admission, not valid responses.
    let bytes = match to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(_) => return Response::builder().status(500).body(Body::empty()).unwrap(),
    };
    let cost = bytes.len() + key.len() + 1024;
    if !live_cursor && cost <= cache.budget.limit() {
        let mut entries = cache.entries.lock().unwrap();
        entries.remove(&key);
        while cost > cache.budget.limit().saturating_sub(cache.budget.used()) {
            let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, e)| e.at)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            entries.remove(&oldest);
        }
        if let Some(lease) = cache.budget.try_acquire(cost) {
            entries.insert(
                key,
                Arc::new(Entry {
                    body: bytes.clone(),
                    headers: parts.headers.clone(),
                    at: Instant::now(),
                    _lease: lease,
                }),
            );
        }
    }
    Response::from_parts(parts, Body::from(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn inject_test_auth(mut request: Request<Body>, next: Next) -> Response {
        let key_id = request
            .headers()
            .get("x-test-key")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("missing")
            .to_string();
        request.extensions_mut().insert(crate::auth::AuthContext {
            key_id: key_id.clone(),
            workforce_id: "shared-workforce".into(),
            permissions: crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: true,
                ..Default::default()
            },
        });
        next.run(request).await
    }

    #[tokio::test]
    async fn repeated_reads_use_cache_mutations_invalidate_and_many_keys_stay_bounded() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let cache = Arc::new(Cache {
            entries: Mutex::new(HashMap::new()),
            budget: Budget::new(4096),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        });
        let calls = Arc::new(AtomicU64::new(0));
        let count = calls.clone();
        let c = cache.clone();
        let app = axum::Router::new()
            .route(
                "/api/value",
                axum::routing::get(move || {
                    let n = count.fetch_add(1, Ordering::Relaxed);
                    async move { axum::Json(serde_json::json!({"n":n})) }
                }),
            )
            .route(
                "/api/change",
                axum::routing::post(|| async { StatusCode::OK }),
            )
            .layer(axum::middleware::from_fn(move |request, next| {
                let c = c.clone();
                async move { cached(&c, request, next).await }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::new();
        let a = client
            .get(format!("{url}/api/value"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let b = client
            .get(format!("{url}/api/value"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(a, b);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        client
            .post(format!("{url}/api/change"))
            .send()
            .await
            .unwrap();
        let c = client
            .get(format!("{url}/api/value"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_ne!(a, c);
        for n in 0..30 {
            client
                .get(format!("{url}/api/value?page={n}"))
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
        }
        assert!(cache.budget.used() <= 4096);
        assert!(cache.entries.lock().unwrap().len() <= 3);
        assert!(cache.hits.load(Ordering::Relaxed) >= 1);
        task.abort();
    }

    #[tokio::test]
    async fn cached_api_bodies_are_partitioned_by_authenticated_key_and_workforce() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let cache = Arc::new(Cache {
            entries: Mutex::new(HashMap::new()),
            budget: Budget::new(4096),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        });
        let calls = Arc::new(AtomicU64::new(0));
        let route_calls = calls.clone();
        let route_cache = cache.clone();
        let app =
            axum::Router::new()
                .route(
                    "/api/value",
                    axum::routing::get(
                        move |axum::extract::Extension(auth): axum::extract::Extension<
                            crate::auth::AuthContext,
                        >| {
                            let call = route_calls.fetch_add(1, Ordering::Relaxed);
                            async move {
                                axum::Json(serde_json::json!({"key_id":auth.key_id,"call":call}))
                            }
                        },
                    ),
                )
                .layer(axum::middleware::from_fn(move |request, next| {
                    let cache = route_cache.clone();
                    async move { cached(&cache, request, next).await }
                }))
                .layer(axum::middleware::from_fn(inject_test_auth));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::new();
        let request = |key: &str| {
            client
                .get(format!("{url}/api/value"))
                .header("x-test-key", key)
        };
        let first: serde_json::Value = request("key-a").send().await.unwrap().json().await.unwrap();
        let repeated: serde_json::Value =
            request("key-a").send().await.unwrap().json().await.unwrap();
        let peer: serde_json::Value = request("key-b").send().await.unwrap().json().await.unwrap();
        assert_eq!(first, repeated);
        assert_ne!(first["key_id"], peer["key_id"]);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        assert!(cache.hits.load(Ordering::Relaxed) >= 1);
        task.abort();
    }
}
