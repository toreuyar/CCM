use crate::config::ToolingConfig;
use serde::Deserialize;
use serde_json::Value;
use std::path::PathBuf;

const PLUGIN_ABI: &str = "ccm_run(i32, i32) -> i64; ccm_alloc(i32) -> i32";

#[derive(Clone, PartialEq)]
pub(crate) struct PluginManifest {
    pub name: String,
    pub description: String,
    pub module_path: PathBuf,
    pub input_schema: Value,
    pub capabilities: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PluginManifestFile {
    name: String,
    description: String,
    wasm: String,
    input_schema: Value,
    #[serde(default)]
    capabilities: Vec<String>,
}

pub(super) fn function_name(plugin_name: &str) -> String {
    format!("ccm_plugin_{plugin_name}")
}

pub(super) fn load_manifests(config: &ToolingConfig) -> Result<Vec<PluginManifest>, String> {
    load_manifests_matching(config, |_| true, false)
}

/// Load and validate only plugins that are eligible for one invocation. Invalid
/// manifests that cannot be selected must not prevent unrelated agents from
/// running; selected manifests still pass every validation below.
pub(super) fn load_manifests_for(
    config: &ToolingConfig,
    mut selected: impl FnMut(&str) -> bool,
) -> Result<Vec<PluginManifest>, String> {
    load_manifests_matching(config, |name| selected(name), true)
}

fn load_manifests_matching(
    config: &ToolingConfig,
    mut selected: impl FnMut(&str) -> bool,
    skip_unselected_invalid: bool,
) -> Result<Vec<PluginManifest>, String> {
    let root = match std::fs::canonicalize(&config.plugin_dir) {
        Ok(root) => root,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("cannot read configured plugin directory: {error}")),
    };
    let entries = std::fs::read_dir(&root).map_err(|error| error.to_string())?;
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| error.to_string())?;
        if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            files.push(entry.path());
        }
    }
    files.sort();
    if files.len() > 1024 {
        return Err("plugin directory exceeds the manifest scan limit".into());
    }
    if !skip_unselected_invalid && files.len() > config.max_tools {
        return Err("plugin directory contains more manifests than tools.max_tools".into());
    }
    let mut manifests = Vec::with_capacity(files.len());
    let mut names = std::collections::HashSet::new();
    let mut scanned_bytes = 0usize;
    for file in files {
        let metadata = match std::fs::metadata(&file) {
            Ok(metadata) => metadata,
            Err(_error) if skip_unselected_invalid => continue,
            Err(error) => return Err(error.to_string()),
        };
        let manifest_limit = config.max_schema_bytes.saturating_add(16 * 1024);
        if !metadata.is_file() || metadata.len() > manifest_limit as u64 {
            if skip_unselected_invalid {
                continue;
            }
            return Err(format!(
                "plugin manifest {} exceeds its limit",
                file.display()
            ));
        }
        scanned_bytes = scanned_bytes.saturating_add(metadata.len() as usize);
        if scanned_bytes > 16 * 1024 * 1024 {
            return Err("plugin manifests exceed the directory scan size limit".into());
        }
        let text = match std::fs::read_to_string(&file) {
            Ok(text) => text,
            Err(_error) if skip_unselected_invalid => continue,
            Err(error) => return Err(error.to_string()),
        };
        let value: Value = match serde_json::from_str(&text) {
            Ok(value) => value,
            Err(error) if skip_unselected_invalid => {
                tracing::warn!(path = %file.display(), %error, "ignoring unselected invalid plugin manifest");
                continue;
            }
            Err(error) => {
                return Err(format!(
                    "invalid plugin manifest {}: {error}",
                    file.display()
                ));
            }
        };
        let candidate_name = value.get("name").and_then(Value::as_str);
        if skip_unselected_invalid
            && candidate_name.is_none_or(|name| !valid_plugin_name(name) || !selected(name))
        {
            continue;
        }
        let manifest: PluginManifestFile = serde_json::from_value(value)
            .map_err(|error| format!("invalid plugin manifest {}: {error}", file.display()))?;
        if !valid_plugin_name(&manifest.name)
            || !names.insert(manifest.name.clone())
            || manifest.description.trim().is_empty()
            || manifest.description.len() > 2048
        {
            return Err(format!(
                "invalid or duplicate plugin metadata in {}",
                file.display()
            ));
        }
        let schema_bytes =
            serde_json::to_vec(&manifest.input_schema).map_err(|error| error.to_string())?;
        if schema_bytes.len() > config.max_schema_bytes {
            return Err(format!(
                "plugin schema {} exceeds tools.max_schema_bytes",
                manifest.name
            ));
        }
        super::validate_schema(&manifest.input_schema)?;
        if manifest.capabilities.len() > 8
            || manifest.capabilities.iter().any(|capability| {
                !["file.read", "file.write", "shell.run", "http.request"]
                    .contains(&capability.as_str())
            })
        {
            return Err(format!(
                "plugin {} declares an unsupported capability",
                manifest.name
            ));
        }
        let wasm_name = PathBuf::from(&manifest.wasm);
        if wasm_name.is_absolute()
            || wasm_name.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir | std::path::Component::RootDir
                )
            })
        {
            return Err(format!(
                "plugin {} WASM path must stay within its plugin directory",
                manifest.name
            ));
        }
        let module_path = std::fs::canonicalize(root.join(wasm_name)).map_err(|error| {
            format!("plugin {} WASM file is unavailable: {error}", manifest.name)
        })?;
        if !module_path.starts_with(&root) {
            return Err(format!(
                "plugin {} WASM file escapes its plugin directory",
                manifest.name
            ));
        }
        let metadata = std::fs::metadata(&module_path).map_err(|error| error.to_string())?;
        if !metadata.is_file() || metadata.len() > config.wasm_max_bytes as u64 {
            return Err(format!(
                "plugin {} WASM file exceeds tools.wasm_max_bytes",
                manifest.name
            ));
        }
        manifests.push(PluginManifest {
            name: manifest.name,
            description: manifest.description,
            module_path,
            input_schema: manifest.input_schema,
            capabilities: manifest.capabilities,
        });
        if manifests.len() > config.max_tools {
            return Err("eligible plugins exceed tools.max_tools".into());
        }
    }
    Ok(manifests)
}

fn valid_plugin_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 48
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

pub(super) async fn invoke(
    config: &ToolingConfig,
    manifest: &PluginManifest,
    arguments: Value,
) -> Result<Value, String> {
    let wasm = std::fs::read(&manifest.module_path).map_err(|error| error.to_string())?;
    if wasm.len() > config.wasm_max_bytes {
        return Err("plugin WASM file exceeds tools.wasm_max_bytes".into());
    }
    let input = serde_json::to_vec(&arguments).map_err(|error| error.to_string())?;
    if input.len() > config.max_input_bytes {
        return Err("plugin input exceeds tools.max_input_bytes".into());
    }
    let max_output = config.max_result_bytes;
    let memory = config.wasm_memory_bytes;
    let fuel = config.wasm_fuel;
    tokio::task::spawn_blocking(move || invoke_wasm(wasm, input, max_output, memory, fuel))
        .await
        .map_err(|error| format!("plugin runtime task failed: {error}"))?
}

fn invoke_wasm(
    wasm: Vec<u8>,
    input: Vec<u8>,
    max_output: usize,
    max_memory: usize,
    fuel: u64,
) -> Result<Value, String> {
    if wasm.len() < 8 || wasm.get(..4) != Some(b"\0asm") {
        return Err("plugin is not a valid WebAssembly module".into());
    }
    let mut engine_config = wasmi::Config::default();
    engine_config.consume_fuel(true);
    let engine = wasmi::Engine::new(&engine_config);
    let module = wasmi::Module::new(&engine, &mut &wasm[..])
        .map_err(|error| format!("invalid WebAssembly module: {error}"))?;
    if module.imports().next().is_some() {
        return Err("WASM plugins may not import host functions or WASI".into());
    }
    let limits = wasmi::StoreLimitsBuilder::new()
        .memory_size(max_memory)
        .instances(1)
        .memories(1)
        .tables(0)
        .build();
    let mut store = wasmi::Store::new(&engine, limits);
    store.limiter(|limits| limits);
    store
        .set_fuel(fuel)
        .map_err(|error| format!("cannot set WASM fuel limit: {error}"))?;
    let linker = wasmi::Linker::<wasmi::StoreLimits>::new(&engine);
    let instance = linker
        .instantiate(&mut store, &module)
        .and_then(|pre| pre.start(&mut store))
        .map_err(|error| format!("WASM plugin initialization failed: {error}"))?;
    let memory = instance
        .get_memory(&store, "memory")
        .ok_or("WASM plugin must export memory")?;
    let alloc = instance
        .get_typed_func::<i32, i32>(&store, "ccm_alloc")
        .map_err(|_| format!("WASM plugin must export {PLUGIN_ABI}"))?;
    let run = instance
        .get_typed_func::<(i32, i32), i64>(&store, "ccm_run")
        .map_err(|_| format!("WASM plugin must export {PLUGIN_ABI}"))?;
    let input_ptr = alloc
        .call(&mut store, input.len() as i32)
        .map_err(|error| format!("WASM plugin allocation failed: {error}"))?;
    if input_ptr < 0 {
        return Err("WASM plugin returned an invalid input pointer".into());
    }
    memory
        .write(&mut store, input_ptr as usize, &input)
        .map_err(|error| format!("WASM plugin input buffer is invalid: {error}"))?;
    let packed = run
        .call(&mut store, (input_ptr, input.len() as i32))
        .map_err(|error| format!("WASM plugin execution failed: {error}"))? as u64;
    let output_ptr = (packed >> 32) as usize;
    let output_len = (packed & 0xffff_ffff) as usize;
    if output_len > max_output {
        return Err("WASM plugin output exceeds tools.max_result_bytes".into());
    }
    let mut output = vec![0; output_len];
    memory
        .read(&store, output_ptr, &mut output)
        .map_err(|error| format!("WASM plugin output buffer is invalid: {error}"))?;
    let value: Value = serde_json::from_slice(&output)
        .map_err(|error| format!("WASM plugin returned malformed JSON: {error}"))?;
    if serde_json::to_vec(&value)
        .map_err(|error| error.to_string())?
        .len()
        > max_output
    {
        return Err("WASM plugin output exceeds tools.max_result_bytes".into());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn malformed_or_over_limit_plugin_metadata_is_rejected() {
        let root = std::env::temp_dir().join(format!("ccm-plugins-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("bad.json"), "not json").unwrap();
        let config = ToolingConfig {
            plugin_dir: root.to_string_lossy().into_owned(),
            ..Default::default()
        };
        assert!(load_manifests(&config).is_err());
        std::fs::remove_file(root.join("bad.json")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    #[test]
    fn malformed_wasm_is_rejected_without_instantiation() {
        assert!(invoke_wasm(b"not wasm".to_vec(), b"{}".to_vec(), 100, 65_536, 10_000).is_err());
        assert!(
            invoke_wasm(
                b"\0asm\x01\0\0\0".to_vec(),
                serde_json::to_vec(&json!({})).unwrap(),
                100,
                65_536,
                10_000
            )
            .is_err()
        );
    }

    fn unsigned_leb(mut value: u32, output: &mut Vec<u8>) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            output.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn signed_leb(mut value: i64, output: &mut Vec<u8>) {
        loop {
            let mut byte = (value as u8) & 0x7f;
            value >>= 7;
            let done = (value == 0 && byte & 0x40 == 0) || (value == -1 && byte & 0x40 != 0);
            if !done {
                byte |= 0x80;
            }
            output.push(byte);
            if done {
                break;
            }
        }
    }

    fn section(id: u8, data: &[u8], output: &mut Vec<u8>) {
        output.push(id);
        unsigned_leb(data.len() as u32, output);
        output.extend_from_slice(data);
    }

    fn test_module(run_body: Vec<u8>) -> Vec<u8> {
        let mut module = b"\0asm\x01\0\0\0".to_vec();
        section(
            1,
            &[2, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7e],
            &mut module,
        );
        section(3, &[2, 0, 1], &mut module);
        section(5, &[1, 0, 1], &mut module);
        let mut exports = vec![3];
        for (name, kind, index) in [("memory", 2, 0), ("ccm_alloc", 0, 0), ("ccm_run", 0, 1)] {
            unsigned_leb(name.len() as u32, &mut exports);
            exports.extend_from_slice(name.as_bytes());
            exports.push(kind);
            unsigned_leb(index, &mut exports);
        }
        section(7, &exports, &mut module);
        let alloc = [0, 0x41, 0, 0x0b];
        let mut code = vec![2];
        unsigned_leb(alloc.len() as u32, &mut code);
        code.extend_from_slice(&alloc);
        unsigned_leb(run_body.len() as u32, &mut code);
        code.extend_from_slice(&run_body);
        section(10, &code, &mut module);
        module
    }

    #[test]
    fn wasm_fuel_memory_and_output_limits_are_enforced() {
        let input = serde_json::to_vec(&json!({})).unwrap();
        let loop_module = test_module(vec![0, 0x03, 0x40, 0x0c, 0, 0x0b, 0x0b]);
        assert!(invoke_wasm(loop_module.clone(), input.clone(), 100, 65_536, 1_000).is_err());
        assert!(invoke_wasm(loop_module, input.clone(), 100, 1024, 10_000).is_err());

        let mut output_body = vec![0, 0x42];
        signed_leb(65_537, &mut output_body);
        output_body.push(0x0b);
        assert!(invoke_wasm(test_module(output_body), input.clone(), 16, 65_536, 10_000).is_err());

        let malformed_output = test_module(vec![0, 0x42, 0, 0x0b]);
        assert!(invoke_wasm(malformed_output, input, 100, 65_536, 10_000).is_err());
    }

    #[test]
    fn manifest_paths_cannot_escape_plugin_directory() {
        let root = std::env::temp_dir().join(format!("ccm-plugins-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("escape.json"),
            r#"{"name":"escape","description":"bad path","wasm":"../outside.wasm","input_schema":{"type":"object","properties":{},"additionalProperties":false},"capabilities":[]}"#,
        )
        .unwrap();
        let config = ToolingConfig {
            plugin_dir: root.to_string_lossy().into_owned(),
            ..Default::default()
        };
        assert!(load_manifests(&config).is_err());
        std::fs::remove_file(root.join("escape.json")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }
}
