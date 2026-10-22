use std::sync::Arc;

use crate::config::Config;
use crate::portfolio::Portfolio;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub http: reqwest::Client,
    pub portfolio: Arc<Portfolio>,
    pub providers: Arc<crate::provider_gate::Registry>,
    /// External `/v1` and `/api` access is closed unless bootstrap auth is configured.
    pub external_api_enabled: bool,
    /// Verified credentials carried through HTTP and future MCP execution paths.
    pub auth: Option<crate::auth::AuthContext>,
}

impl AppState {
    /// Safe configuration projection for Foreman and future management surfaces.
    /// It contains no commands, environment values, or filesystem paths.
    pub fn sandbox_runtime_status(&self) -> serde_json::Value {
        crate::runtime::tools::sandbox_runtime_status(&crate::config::tooling_config())
    }

    pub fn with_auth(self: &Arc<Self>, context: &crate::auth::AuthContext) -> Arc<Self> {
        Arc::new(Self {
            config: self.config.clone(),
            http: self.http.clone(),
            portfolio: self.portfolio.with_scope(context),
            providers: self.providers.clone(),
            external_api_enabled: self.external_api_enabled,
            auth: Some(context.clone()),
        })
    }

    pub async fn announce(&self, request: &str, text: &str) -> Result<(), String> {
        // Dashboard evidence is unconditional; request presentation gates chat.
        self.portfolio.trace_event(
            request,
            "operation.progress",
            &serde_json::json!({"request_id":request,"text":text}),
        )?;
        crate::transparency::narrate(request, text).await
    }
}
