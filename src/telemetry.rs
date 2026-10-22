//! Environment flag parsing and historical SSE scanner regressions.

/// Parses an on/off-ish env value ("1", "true", "yes", "on" → true).
pub fn parse_flag(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

#[cfg(test)]
mod legacy_scanner {
    /// Incrementally scans SSE bytes as they pass through, without buffering the
    /// stream: counts `data:` lines, captures completion id/created and the final
    /// `finish_reason`, and reports the byte offset just after the `data: [DONE]`
    /// line so the caller can inject telemetry before the terminator.
    pub struct SseScanner {
        tail: Vec<u8>,
        pub data_lines: u64,
        pub completion_id: Option<String>,
        pub created: i64,
        pub finish_reason: Option<String>,
        pub done_seen: bool,
        pub usage: Option<serde_json::Value>,
    }

    /// Result of feeding bytes: if the `[DONE]` terminator line completed during
    /// this feed, `done_at` is the offset within the fed slice where the
    /// `[DONE]` line starts (everything before it can go out first, then injected
    /// telemetry, then the terminator and the remainder).
    pub struct FeedResult {
        pub done_at: Option<usize>,
    }

    impl SseScanner {
        pub fn new() -> Self {
            Self {
                tail: Vec::new(),
                data_lines: 0,
                completion_id: None,
                created: 0,
                finish_reason: None,
                done_seen: false,
                usage: None,
            }
        }

        pub fn feed(&mut self, bytes: &[u8]) -> FeedResult {
            let base = self.tail.len();
            self.tail.extend_from_slice(bytes);

            let mut cursor = 0;
            let mut done_at: Option<usize> = None;

            while let Some(rel) = self.tail[cursor..].iter().position(|b| *b == b'\n') {
                let line_start = cursor;
                let line = self.tail[cursor..cursor + rel].to_vec();
                cursor += rel + 1;
                let line = strip_trailing_cr(&line);

                if is_data_line(line) {
                    self.data_lines += 1;
                    if !is_done_line(line) {
                        if let Some(raw) = line.strip_prefix(b"data:")
                            && let Ok(value) = serde_json::from_slice::<serde_json::Value>(raw)
                            && let Some(usage) = value.get("usage").filter(|v| !v.is_null())
                        {
                            self.usage = Some(usage.clone());
                        }
                        if self.completion_id.is_none() {
                            self.completion_id = extract_string_field(line, "id");
                        }
                        if self.created == 0 {
                            self.created = extract_i64_field(line, "created").unwrap_or(0);
                        }
                    }
                    if self.finish_reason.is_none() {
                        self.finish_reason = extract_finish_reason(line);
                    }
                }

                if is_done_line(line) && done_at.is_none() {
                    // The line may have started in an earlier feed ("data: [D" |
                    // "ONE]\n"); clamp the split offset to the fed slice start.
                    done_at = Some(line_start.saturating_sub(base));
                    self.done_seen = true;
                }
            }

            self.tail.drain(..cursor);
            FeedResult { done_at }
        }
    }

    impl Default for SseScanner {
        fn default() -> Self {
            Self::new()
        }
    }

    fn strip_trailing_cr(line: &[u8]) -> &[u8] {
        match line.last() {
            Some(b'\r') => &line[..line.len() - 1],
            _ => line,
        }
    }

    pub fn is_data_line(line: &[u8]) -> bool {
        line.starts_with(b"data:")
    }

    pub fn is_done_line(line: &[u8]) -> bool {
        let line = strip_trailing_cr(line);
        let rest = line.strip_prefix(b"data:").unwrap_or(line);
        let rest = strip_leading_space(rest);
        rest == b"[DONE]"
    }

    fn strip_leading_space(mut rest: &[u8]) -> &[u8] {
        while let Some((first, remainder)) = rest.split_first() {
            if *first == b' ' {
                rest = remainder;
            } else {
                break;
            }
        }
        rest
    }

    fn string_needle(field: &str) -> Vec<u8> {
        format!("\"{field}\":\"").into_bytes()
    }

    fn number_needle(field: &str) -> Vec<u8> {
        format!("\"{field}\":").into_bytes()
    }

    /// Extracts a JSON string field's value from a line: `"field":"value"`.
    pub fn extract_string_field(line: &[u8], field: &str) -> Option<String> {
        let needle = string_needle(field);
        let pos = line
            .windows(needle.len())
            .position(|w| w == needle.as_slice())?;
        let rest = &line[pos + needle.len()..];
        let end = rest.iter().position(|b| *b == b'"')?;
        Some(String::from_utf8_lossy(&rest[..end]).into_owned())
    }

    /// Extracts a JSON number field's value from a line: `"field":123`.
    pub fn extract_i64_field(line: &[u8], field: &str) -> Option<i64> {
        let needle = number_needle(field);
        let pos = line
            .windows(needle.len())
            .position(|w| w == needle.as_slice())?;
        let rest = &line[pos + needle.len()..];
        let end = rest
            .iter()
            .position(|b| !b.is_ascii_digit())
            .unwrap_or(rest.len());
        if end == 0 {
            return None;
        }
        std::str::from_utf8(&rest[..end]).ok()?.parse().ok()
    }

    const FINISH_NEEDLE: &[u8] = b"\"finish_reason\":\"";

    /// Extracts the value of `"finish_reason":"..."` from a line, if present.
    pub fn extract_finish_reason(line: &[u8]) -> Option<String> {
        extract_string_field_raw(line, FINISH_NEEDLE)
    }

    fn extract_string_field_raw(line: &[u8], needle: &[u8]) -> Option<String> {
        let pos = line.windows(needle.len()).position(|w| w == needle)?;
        let rest = &line[pos + needle.len()..];
        let end = rest.iter().position(|b| *b == b'"')?;
        Some(String::from_utf8_lossy(&rest[..end]).into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::legacy_scanner::*;
    use super::*;

    #[test]
    fn flag_parsing() {
        assert!(parse_flag("1"));
        assert!(parse_flag(" true "));
        assert!(parse_flag("ON"));
        assert!(!parse_flag("0"));
        assert!(!parse_flag("false"));
        assert!(!parse_flag("off"));
        assert!(!parse_flag("garbage"));
    }

    #[test]
    fn scanner_tracks_stream_across_fragments() {
        let mut scanner = SseScanner::new();
        let chunk1 = b"data: {\"id\":\"chatcmpl-9\",\"created\":123,\"model\":\"m\",\"choices\":[{\"delta\":{\"content\":\"He\"}}]}\n\n";
        let chunk2 = b"data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [D";
        let chunk3 = b"ONE]\n\n";

        let feed1 = scanner.feed(chunk1);
        assert!(feed1.done_at.is_none());
        let feed2 = scanner.feed(chunk2);
        assert!(feed2.done_at.is_none());
        let feed3 = scanner.feed(chunk3);

        // "ONE]\n\n": the "data: [DONE]" line starts at fed-slice index 0
        // (its head "data: [D" was buffered from the previous chunk).
        assert_eq!(feed3.done_at, Some(0));
        assert_eq!(scanner.data_lines, 3);
        assert_eq!(scanner.completion_id.as_deref(), Some("chatcmpl-9"));
        assert_eq!(scanner.created, 123);
        assert_eq!(scanner.finish_reason.as_deref(), Some("stop"));
        assert!(scanner.done_seen);
    }

    #[test]
    fn done_line_variants() {
        assert!(is_done_line(b"data: [DONE]"));
        assert!(is_done_line(b"data: [DONE]\r"));
        assert!(is_done_line(b"data:  [DONE]"));
        assert!(!is_done_line(b"data: {\"x\":1}"));
        assert!(!is_done_line(b": comment"));
    }

    #[test]
    fn field_extraction() {
        let line =
            b"data: {\"id\":\"chatcmpl-7\",\"created\":1789650519,\"choices\":[],\"finish_reason\":\"tool_calls\"}";
        assert_eq!(
            extract_string_field(line, "id").as_deref(),
            Some("chatcmpl-7")
        );
        assert_eq!(extract_i64_field(line, "created"), Some(1789650519));
        assert_eq!(extract_finish_reason(line).as_deref(), Some("tool_calls"));
        assert_eq!(extract_string_field(b"data: {}", "id"), None);
        assert_eq!(extract_i64_field(b"data: {\"created\":}", "created"), None);
    }
}
