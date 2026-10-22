use serde_json::{Value, json};

pub fn model_entry(model: &str) -> Value {
    json!({
        "id": model,
        "object": "model",
        "created": 0,
        "owned_by": "ccm"
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_entry_shape() {
        let value = model_entry("deepseek-v4.1-flash");
        assert_eq!(value["id"], "deepseek-v4.1-flash");
        assert_eq!(value["object"], "model");
        assert_eq!(value["owned_by"], "ccm");
    }
}
