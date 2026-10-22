//! Explicit graph execution contract. Stage meanings are generated, not hardcoded.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Branch {
    /// JSON pointer evaluated against the current stage's parsed report.
    pub pointer: String,
    pub equals: Value,
    pub next: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stage {
    pub id: String,
    pub agent_id: String,
    pub instruction: String,
    #[serde(default)]
    pub branches: Vec<Branch>,
    /// Default transition; null terminates the workflow.
    pub next: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    #[serde(default)]
    pub id: String,
    pub name: String,
    pub purpose: String,
    pub entry: String,
    pub max_steps: u32,
    pub stages: Vec<Stage>,
}

impl Workflow {
    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() || self.purpose.trim().is_empty() || self.stages.is_empty() {
            return Err("workflow requires name, purpose and stages".into());
        }
        if self.max_steps == 0 || self.max_steps > 100 || self.stages.len() > 100 {
            return Err("workflow bounds must be 1..100".into());
        }
        let ids: HashSet<_> = self.stages.iter().map(|s| s.id.as_str()).collect();
        if ids.len() != self.stages.len() || !ids.contains(self.entry.as_str()) {
            return Err("duplicate stage or unknown entry".into());
        }
        for stage in &self.stages {
            if stage.id.trim().is_empty()
                || stage.agent_id.trim().is_empty()
                || stage.instruction.trim().is_empty()
            {
                return Err("stage requires id, agent_id and instruction".into());
            }
            for next in std::iter::once(&stage.next)
                .chain(stage.branches.iter().map(|b| &b.next))
                .flatten()
            {
                if !ids.contains(next.as_str()) {
                    return Err(format!("unknown transition {next}"));
                }
            }
            for branch in &stage.branches {
                if !branch.pointer.is_empty() && !branch.pointer.starts_with('/') {
                    return Err("branch pointer must be a JSON pointer".into());
                }
            }
        }
        Ok(())
    }

    pub fn transition(&self, stage: &Stage, report: &str) -> Result<Option<String>, String> {
        if !stage.branches.is_empty() {
            let value: Value = serde_json::from_str(report)
                .map_err(|_| "branch stage must return a JSON report")?;
            for branch in &stage.branches {
                if value.pointer(&branch.pointer) == Some(&branch.equals) {
                    return Ok(branch.next.clone());
                }
            }
        }
        Ok(stage.next.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn workflow() -> Workflow {
        serde_json::from_value(json!({"name":"revision","purpose":"iterate", "entry":"work","max_steps":3,"stages":[
        {"id":"work","agent_id":"a","instruction":"produce JSON","branches":[{"pointer":"/done","equals":true,"next":null}],"next":"work"}
    ]})).unwrap()
    }
    #[test]
    fn branches_and_cycles_follow_definition() {
        let w = workflow();
        w.validate().unwrap();
        assert_eq!(
            w.transition(&w.stages[0], r#"{"done":true}"#).unwrap(),
            None
        );
        assert_eq!(
            w.transition(&w.stages[0], r#"{"done":false}"#).unwrap(),
            Some("work".into())
        );
        assert!(w.transition(&w.stages[0], "not JSON").is_err());
    }
    #[test]
    fn invalid_graphs_are_rejected() {
        let mut w = workflow();
        w.stages[0].next = Some("unknown".into());
        assert!(w.validate().is_err());
        let mut w = workflow();
        w.max_steps = 0;
        assert!(w.validate().is_err());
        let mut w = workflow();
        w.stages.push(w.stages[0].clone());
        assert!(w.validate().is_err());
    }
}
