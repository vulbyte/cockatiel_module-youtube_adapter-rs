use cockatiel_client::proto::container_for_engine::Payload as EnginePayload;
use cockatiel_client::proto::container_for_module::Payload as ModulePayload;
use cockatiel_client::proto::*;
use cockatiel_client::{CockatielClient, PromptKind};
use futures_util::{SinkExt, StreamExt as _};
use prost::Message as ProstMessage;
use serde::{Deserialize, Serialize};
use serde_json::json;


use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tonic::transport::ClientTlsConfig;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    WsMessage,
>;

/// The module's engine-session identity (auth token + assigned instance + name).
/// Held in a shared Mutex so a reconnect can swap it in place and every other
/// task (read loop, platform send path) always uses the CURRENT session's
/// credentials — a stale token after a reconnect would be rejected by the
/// engine and the module would look dead.
#[derive(Clone, Default)]
struct EngineIdentity {
    auth: String,
    instance: String,
    module: String,
}

/// Re-register the adapter's chat commands with the engine (called on the
/// initial connect AND after every reconnect — the engine forgets a session's
/// commands when the socket drops).
async fn register_commands(
    write: &Arc<tokio::sync::Mutex<WsWriteHalf>>,
    identity: &Arc<tokio::sync::Mutex<EngineIdentity>>,
) {
    let (auth, module, instance) = {
        let id = identity.lock().await;
        (id.auth.clone(), id.module.clone(), id.instance.clone())
    };
    let commands = ContainerForEngine {
        version: 2,
        auth_token: auth.clone(),
        module_name: module.clone(),
        module_instance_uuid7: instance.clone(),
        payload: Some(EnginePayload::Commands(Commands {
            commands: vec![
                Command {
                    command_name: "ban".to_string(),
                    command_flag: "!".to_string(),
                    command_description: "ban a user".to_string(),
                    command_flags: vec![],
                },
                Command {
                    command_name: "timeout".to_string(),
                    command_flag: "!".to_string(),
                    command_description: "timeout a user".to_string(),
                    command_flags: vec![],
                },
            ],
            alert_on_unknown_command: false,
        })),
    };
    let mut cbuf = Vec::new();
    if commands.encode(&mut cbuf).is_ok() {
        let mut w = write.lock().await;
        let _ = w.send(WsMessage::Binary(cbuf.into())).await;
    }
    info!("registered !ban / !timeout commands");
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct YoutubeAdapterConfig {
    channel_id: Option<String>,
    api_key: Option<String>,
    #[serde(default)]
    api_keys: Option<Vec<String>>,
    #[serde(default)]
    unlisted_video_ids: Option<Vec<String>>,
    #[serde(default)]
    google_oauth_client_id: Option<String>,
    #[serde(default)]
    google_oauth_client_secret: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    // Runtime tuning knobs. Each `#[serde(default = ...)]` supplies the
    // previous hardcoded constant when the key is missing from config.json,
    // so an absent setting behaves exactly as before.
    #[serde(default = "YoutubeAdapterConfig::default_timeout_secs")]
    default_timeout_secs: i32,
    #[serde(default = "YoutubeAdapterConfig::http_timeout_secs")]
    http_timeout_secs: u32,
    #[serde(default = "YoutubeAdapterConfig::token_expiry_margin_secs")]
    token_expiry_margin_secs: u32,
    #[serde(default = "YoutubeAdapterConfig::token_ttl_fallback_secs")]
    token_ttl_fallback_secs: u32,
    #[serde(default = "YoutubeAdapterConfig::chat_open_max_attempts")]
    chat_open_max_attempts: u32,
    #[serde(default = "YoutubeAdapterConfig::chat_open_backoff_first_secs")]
    chat_open_backoff_first_secs: u32,
    #[serde(default = "YoutubeAdapterConfig::chat_open_backoff_later_secs")]
    chat_open_backoff_later_secs: u32,
    #[serde(default = "YoutubeAdapterConfig::dedup_cap")]
    dedup_cap: usize,
    #[serde(default = "YoutubeAdapterConfig::quota_backoff_secs")]
    quota_backoff_secs: u32,
    #[serde(default = "YoutubeAdapterConfig::poll_retry_delay_ms")]
    poll_retry_delay_ms: u32,
    #[serde(default = "YoutubeAdapterConfig::polling_interval_ms")]
    polling_interval_ms: u32,
    #[serde(default = "YoutubeAdapterConfig::chat_poll_retry_secs")]
    chat_poll_retry_secs: u32,
    #[serde(default = "YoutubeAdapterConfig::oauth_capture_timeout_secs")]
    oauth_capture_timeout_secs: u32,
    #[serde(default = "YoutubeAdapterConfig::outbound_queue_cap")]
    outbound_queue_cap: usize,
    #[serde(default = "YoutubeAdapterConfig::reconnect_base_secs")]
    reconnect_base_secs: u32,
    #[serde(default = "YoutubeAdapterConfig::reconnect_max_secs")]
    reconnect_max_secs: u32,
    #[serde(default = "YoutubeAdapterConfig::stream_fetch_retry_secs")]
    stream_fetch_retry_secs: u32,
    #[serde(default = "YoutubeAdapterConfig::stream_scan_interval_secs")]
    stream_scan_interval_secs: u32,
    /// Passive mode: when no stream is found, re-scan on a slow cadence
    /// (`passive_scan_interval_secs`) instead of the regular
    /// `stream_scan_interval_secs`, so an off-air channel stops polling often.
    #[serde(default)]
    passive: bool,
    #[serde(default = "YoutubeAdapterConfig::passive_scan_interval_secs")]
    passive_scan_interval_secs: u32,
    #[serde(default = "YoutubeAdapterConfig::prompt_timeout_secs")]
    prompt_timeout_secs: u32,
    #[serde(default = "YoutubeAdapterConfig::stream_control_poll_secs")]
    stream_control_poll_secs: u32,
}

impl YoutubeAdapterConfig {
    fn default_timeout_secs() -> i32 {
        300
    }
    fn http_timeout_secs() -> u32 {
        15
    }
    fn token_expiry_margin_secs() -> u32 {
        60
    }
    fn token_ttl_fallback_secs() -> u32 {
        3600
    }
    fn chat_open_max_attempts() -> u32 {
        60
    }
    fn chat_open_backoff_first_secs() -> u32 {
        10
    }
    fn chat_open_backoff_later_secs() -> u32 {
        30
    }
    fn dedup_cap() -> usize {
        10_000
    }
    fn quota_backoff_secs() -> u32 {
        60
    }
    fn poll_retry_delay_ms() -> u32 {
        2000
    }
    fn polling_interval_ms() -> u32 {
        5000
    }
    fn chat_poll_retry_secs() -> u32 {
        5
    }
    fn oauth_capture_timeout_secs() -> u32 {
        120
    }
    fn outbound_queue_cap() -> usize {
        64
    }
    fn reconnect_base_secs() -> u32 {
        1
    }
    fn reconnect_max_secs() -> u32 {
        30
    }
    fn stream_fetch_retry_secs() -> u32 {
        15
    }
    fn stream_scan_interval_secs() -> u32 {
        30
    }
    fn passive_scan_interval_secs() -> u32 {
        300
    }
    fn prompt_timeout_secs() -> u32 {
        300
    }
    fn stream_control_poll_secs() -> u32 {
        10
    }
}

impl Default for YoutubeAdapterConfig {
    fn default() -> Self {
        Self {
            channel_id: None,
            api_key: None,
            api_keys: None,
            unlisted_video_ids: None,
            google_oauth_client_id: None,
            google_oauth_client_secret: None,
            refresh_token: None,
            default_timeout_secs: Self::default_timeout_secs(),
            http_timeout_secs: Self::http_timeout_secs(),
            token_expiry_margin_secs: Self::token_expiry_margin_secs(),
            token_ttl_fallback_secs: Self::token_ttl_fallback_secs(),
            chat_open_max_attempts: Self::chat_open_max_attempts(),
            chat_open_backoff_first_secs: Self::chat_open_backoff_first_secs(),
            chat_open_backoff_later_secs: Self::chat_open_backoff_later_secs(),
            dedup_cap: Self::dedup_cap(),
            quota_backoff_secs: Self::quota_backoff_secs(),
            poll_retry_delay_ms: Self::poll_retry_delay_ms(),
            polling_interval_ms: Self::polling_interval_ms(),
            chat_poll_retry_secs: Self::chat_poll_retry_secs(),
            oauth_capture_timeout_secs: Self::oauth_capture_timeout_secs(),
            outbound_queue_cap: Self::outbound_queue_cap(),
            reconnect_base_secs: Self::reconnect_base_secs(),
            reconnect_max_secs: Self::reconnect_max_secs(),
            stream_fetch_retry_secs: Self::stream_fetch_retry_secs(),
            stream_scan_interval_secs: Self::stream_scan_interval_secs(),
            passive: false,
            passive_scan_interval_secs: Self::passive_scan_interval_secs(),
            prompt_timeout_secs: Self::prompt_timeout_secs(),
            stream_control_poll_secs: Self::stream_control_poll_secs(),
        }
    }
}

/// Runtime tuning knobs read from `config.json` (`module_specific`). A small
/// snapshot keeps the values easy to thread into the functions that previously
/// held the hardcoded literals.
#[derive(Debug, Clone)]
struct Tuning {
    default_timeout_secs: i32,
    http_timeout_secs: u32,
    token_expiry_margin_secs: u32,
    token_ttl_fallback_secs: u32,
    chat_open_max_attempts: u32,
    chat_open_backoff_first_secs: u32,
    chat_open_backoff_later_secs: u32,
    dedup_cap: usize,
    quota_backoff_secs: u32,
    poll_retry_delay_ms: u32,
    polling_interval_ms: u32,
    chat_poll_retry_secs: u32,
    oauth_capture_timeout_secs: u32,
    outbound_queue_cap: usize,
    reconnect_base_secs: u32,
    reconnect_max_secs: u32,
    stream_fetch_retry_secs: u32,
    stream_scan_interval_secs: u32,
    passive: bool,
    passive_scan_interval_secs: u32,
    prompt_timeout_secs: u32,
    stream_control_poll_secs: u32,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            default_timeout_secs: YoutubeAdapterConfig::default_timeout_secs(),
            http_timeout_secs: YoutubeAdapterConfig::http_timeout_secs(),
            token_expiry_margin_secs: YoutubeAdapterConfig::token_expiry_margin_secs(),
            token_ttl_fallback_secs: YoutubeAdapterConfig::token_ttl_fallback_secs(),
            chat_open_max_attempts: YoutubeAdapterConfig::chat_open_max_attempts(),
            chat_open_backoff_first_secs: YoutubeAdapterConfig::chat_open_backoff_first_secs(),
            chat_open_backoff_later_secs: YoutubeAdapterConfig::chat_open_backoff_later_secs(),
            dedup_cap: YoutubeAdapterConfig::dedup_cap(),
            quota_backoff_secs: YoutubeAdapterConfig::quota_backoff_secs(),
            poll_retry_delay_ms: YoutubeAdapterConfig::poll_retry_delay_ms(),
            polling_interval_ms: YoutubeAdapterConfig::polling_interval_ms(),
            chat_poll_retry_secs: YoutubeAdapterConfig::chat_poll_retry_secs(),
            oauth_capture_timeout_secs: YoutubeAdapterConfig::oauth_capture_timeout_secs(),
            outbound_queue_cap: YoutubeAdapterConfig::outbound_queue_cap(),
            reconnect_base_secs: YoutubeAdapterConfig::reconnect_base_secs(),
            reconnect_max_secs: YoutubeAdapterConfig::reconnect_max_secs(),
            stream_fetch_retry_secs: YoutubeAdapterConfig::stream_fetch_retry_secs(),
            stream_scan_interval_secs: YoutubeAdapterConfig::stream_scan_interval_secs(),
            passive: false,
            passive_scan_interval_secs: YoutubeAdapterConfig::passive_scan_interval_secs(),
            prompt_timeout_secs: YoutubeAdapterConfig::prompt_timeout_secs(),
            stream_control_poll_secs: YoutubeAdapterConfig::stream_control_poll_secs(),
        }
    }
}

impl From<&YoutubeAdapterConfig> for Tuning {
    fn from(cfg: &YoutubeAdapterConfig) -> Self {
        Self {
            default_timeout_secs: cfg.default_timeout_secs,
            http_timeout_secs: cfg.http_timeout_secs,
            token_expiry_margin_secs: cfg.token_expiry_margin_secs,
            token_ttl_fallback_secs: cfg.token_ttl_fallback_secs,
            chat_open_max_attempts: cfg.chat_open_max_attempts,
            chat_open_backoff_first_secs: cfg.chat_open_backoff_first_secs,
            chat_open_backoff_later_secs: cfg.chat_open_backoff_later_secs,
            dedup_cap: cfg.dedup_cap,
            quota_backoff_secs: cfg.quota_backoff_secs,
            poll_retry_delay_ms: cfg.poll_retry_delay_ms,
            polling_interval_ms: cfg.polling_interval_ms,
            chat_poll_retry_secs: cfg.chat_poll_retry_secs,
            oauth_capture_timeout_secs: cfg.oauth_capture_timeout_secs,
            outbound_queue_cap: cfg.outbound_queue_cap,
            reconnect_base_secs: cfg.reconnect_base_secs,
            reconnect_max_secs: cfg.reconnect_max_secs,
            stream_fetch_retry_secs: cfg.stream_fetch_retry_secs,
            stream_scan_interval_secs: cfg.stream_scan_interval_secs,
            passive: cfg.passive,
            passive_scan_interval_secs: cfg.passive_scan_interval_secs,
            prompt_timeout_secs: cfg.prompt_timeout_secs,
            stream_control_poll_secs: cfg.stream_control_poll_secs,
        }
    }
}

#[derive(Debug, Clone)]
struct StreamInfo {
    video_id: String,
    title: String,
    status: String, // "live" or "upcoming"
    published_at: String,
    /// The stream's scheduled start time (ISO 8601) when known. Used to order
    /// the selection prompt chronologically (soonest first). Falls back to
    /// `published_at` when the start time isn't available.
    scheduled_start: String,
}

#[derive(Debug, Clone)]
struct ApiKeyManager {
    keys: Vec<String>,
    current_idx: Arc<AtomicUsize>,
}

impl ApiKeyManager {
    fn new(keys: Vec<String>) -> Self {
        Self {
            keys,
            current_idx: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn current_key(&self) -> String {
        if self.keys.is_empty() {
            return String::new();
        }
        let idx = self.current_idx.load(Ordering::Relaxed) % self.keys.len();
        self.keys[idx].clone()
    }

    fn rotate_to_next(&self) -> String {
        if self.keys.is_empty() {
            return String::new();
        }
        let prev_idx = self.current_idx.load(Ordering::Relaxed) % self.keys.len();
        let next = self.current_idx.fetch_add(1, Ordering::SeqCst) + 1;
        let new_idx = next % self.keys.len();
        let key_snippet = self.keys[new_idx].chars().take(8).collect::<String>();
        warn!(
            "YouTube API key {} hit quota or limit. Rotating to key {} of {} (key: {}...)",
            prev_idx + 1,
            new_idx + 1,
            self.keys.len(),
            key_snippet
        );
        self.keys[new_idx].clone()
    }

    fn key_count(&self) -> usize {
        self.keys.len()
    }

    fn get_all_keys(&self) -> &[String] {
        &self.keys
    }

    fn is_quota_error(json: &serde_json::Value) -> bool {
        if let Some(err) = json.get("error") {
            if let Some(errors) = err.get("errors").and_then(|e| e.as_array()) {
                for e in errors {
                    let reason = e.get("reason").and_then(|r| r.as_str()).unwrap_or("");
                    let domain = e.get("domain").and_then(|d| d.as_str()).unwrap_or("");
                    if reason == "quotaExceeded"
                        || reason == "rateLimitExceeded"
                        || reason == "dailyLimitExceeded"
                        || domain == "youtube.quota"
                    {
                        return true;
                    }
                }
            }
            if let Some(msg) = err.get("message").and_then(|m| m.as_str()) {
                let lower = msg.to_lowercase();
                if lower.contains("quota") || lower.contains("exceeded") {
                    return true;
                }
            }
        }
        false
    }

    fn is_key_error(json: &serde_json::Value) -> bool {
        const NON_KEY_REASONS: [&str; 5] = [
            "livechatended",
            "livechatdisabled",
            "livechatnotfound",
            "videonotfound",
            "livestreamingnotenabled",
        ];
        const KEY_REASONS: [&str; 8] = [
            "quotaexceeded",
            "ratelimitexceeded",
            "dailylimitexceeded",
            "userratelimitexceeded",
            "keyinvalid",
            "api_key_invalid",
            "accessnotconfigured",
            "iprefererblocked",
        ];
        if let Some(err) = json.get("error") {
            if let Some(errors) = err.get("errors").and_then(|e| e.as_array()) {
                for e in errors {
                    let reason = e
                        .get("reason")
                        .and_then(|r| r.as_str())
                        .unwrap_or("")
                        .to_lowercase();
                    if NON_KEY_REASONS.contains(&reason.as_str()) {
                        return false;
                    }
                    let domain = e.get("domain").and_then(|d| d.as_str()).unwrap_or("");
                    if domain == "youtube.quota" || domain == "usageLimits" {
                        return true;
                    }
                    if KEY_REASONS.contains(&reason.as_str()) {
                        return true;
                    }
                }
            }
            if let Some(status) = err.get("status").and_then(|s| s.as_str()) {
                if KEY_REASONS.contains(&status.to_lowercase().as_str()) {
                    return true;
                }
            }
        }
        Self::is_quota_error(json)
    }
}

/// Rebuilds the key ring after a failed startup validation: drops the offending
/// key (and any duplicate of the replacement), then prepends the new key so
/// rotation keeps working over the remaining keys. An empty `new` is a no-op.
fn replace_current_key(existing: &[String], current: &str, new: &str) -> Vec<String> {
    let trimmed = new.trim();
    if trimmed.is_empty() {
        return existing.to_vec();
    }
    let mut out: Vec<String> = existing
        .iter()
        .filter(|k| k.as_str() != current && k.as_str() != trimmed)
        .cloned()
        .collect();
    out.insert(0, trimmed.to_string());
    out
}

/// Extracts a rotated refresh token from a token response when Google returns a
/// new, non-empty one that differs from the token we sent.
fn rotated_refresh_token(resp: &serde_json::Value, current: &str) -> Option<String> {
    resp.get("refresh_token")
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty() && *s != current)
        .map(|s| s.to_string())
}

/// Result of probing an API key. `Unknown` means the probe never produced a
/// definitive answer (network/TLS failure, timeout, non-JSON body) — the key is
/// NOT known to be invalid, so callers must not rotate or re-prompt for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyValidity {
    Valid,
    Invalid,
    Unknown,
}

/// Tests whether the current YouTube API key is valid by making a lightweight videos.list request (1 quota unit).
async fn is_api_key_valid(client: &reqwest::Client, keys: &ApiKeyManager) -> KeyValidity {
    let api_key = keys.current_key();
    if api_key.is_empty() {
        return KeyValidity::Invalid;
    }
    let url = format!(
        "https://www.googleapis.com/youtube/v3/videos?part=id&id=dQw4w9WgXcQ&key={}",
        api_key
    );
    match client.get(&url).send().await {
        Ok(res) => {
            let status = res.status();
            match res.json::<serde_json::Value>().await {
                Ok(json) => {
                    if json.get("error").is_none() {
                        KeyValidity::Valid
                    } else if status.is_client_error() {
                        // A 4xx with a Google API error body is a definitive
                        // rejection (bad key, API not enabled, quota, etc.).
                        KeyValidity::Invalid
                    } else {
                        // Error body on a 5xx/3xx: server-side problem, not the
                        // key's fault. Treat as unknown so we retry later.
                        KeyValidity::Unknown
                    }
                }
                Err(e) => {
                    warn!("Could not parse YouTube API key validation response: {}", e);
                    KeyValidity::Unknown
                }
            }
        }
        Err(e) => {
            warn!(
                "Could not reach YouTube API to validate key ({}); treating key as unknown.",
                e
            );
            KeyValidity::Unknown
        }
    }
}

/// True if ANY configured key reaches the API. Rotates past the current key so a
/// single exhausted/invalid key (e.g. key 1 at its daily quota) does not make the
/// startup check declare the whole ring invalid and spuriously re-prompt for a
/// key that is already there. Returns `Unknown` when no key was definitively
/// rejected but at least one probe failed for transport/parse reasons.
async fn any_api_key_valid(client: &reqwest::Client, keys: &ApiKeyManager) -> KeyValidity {
    let attempts = keys.key_count().max(1);
    let mut saw_unknown = false;
    for _ in 0..attempts {
        match is_api_key_valid(client, keys).await {
            KeyValidity::Valid => return KeyValidity::Valid,
            KeyValidity::Unknown => saw_unknown = true,
            KeyValidity::Invalid => {}
        }
        if keys.key_count() > 1 {
            keys.rotate_to_next();
        }
    }
    if saw_unknown {
        KeyValidity::Unknown
    } else {
        KeyValidity::Invalid
    }
}

/// Build a moderator query from the engine-routed command. The engine already
/// parsed + routed `!ban`/`!timeout`; here we map command_name -> query and
/// extract the target/reason from the message args (no re-parsing).
fn build_mod_query(
    command_name: &str,
    message: &str,
    author: &str,
    default_timeout_secs: i32,
) -> Option<(String, serde_json::Value)> {
    let mut tokens = message.trim().split_whitespace();
    let _cmd = tokens.next()?;
    match command_name {
        "ban" => {
            let target = tokens.next()?.trim_start_matches('@').to_string();
            if target.is_empty() {
                return None;
            }
            return Some((
                "mod_ban".to_string(),
                serde_json::json!({
                    "platform": "youtube",
                    "handle": target,
                    "reason": tokens.collect::<Vec<_>>().join(" "),
                    "actor": { "platform": "youtube", "handle": author },
                }),
            ));
        }

        "timeout" => {
            let target = tokens.next()?.trim_start_matches('@').to_string();
            if target.is_empty() {
                return None;
            }
            let mut duration_secs = default_timeout_secs;
            let mut reason = String::new();
            if let Some(d) = tokens.next() {
                if let Ok(secs) = d.parse::<i32>() {
                    duration_secs = secs;
                } else {
                    reason = d.to_string();
                }
            }
            let rest: Vec<&str> = tokens.collect();
            if !rest.is_empty() {
                if !reason.is_empty() {
                    reason = format!("{} {}", reason, rest.join(" "));
                } else {
                    reason = rest.join(" ");
                }
            }
            return Some((
                "mod_timeout".to_string(),
                serde_json::json!({
                    "platform": "youtube",
                    "handle": target,
                    "duration_secs": duration_secs,
                    "reason": reason,
                    "actor": { "platform": "youtube", "handle": author },
                }),
            ));
        }

        _ => None,
    }
}

/// Backfill any missing tuning keys into `module_specific` with their defaults
/// (creating config.json if it does not exist yet), so every setting is always
/// present and editable in place. Leaves channel/api_keys/oauth/servers keys
/// untouched. Best-effort: a read/write failure is ignored.
fn backfill_tuning_defaults() {
    backfill_tuning_defaults_at(&std::path::PathBuf::from("config.json"));
}

fn backfill_tuning_defaults_at(path: &std::path::Path) {
    let mut json_val = std::fs::read_to_string(path)
        .ok()
        .and_then(|data| serde_json::from_str::<serde_json::Value>(&data).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    let mut ms = json_val
        .get("module_specific")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let before = ms.clone();
    let defaults: [(&str, i32); 20] = [
        ("default_timeout_secs", 300),
        ("http_timeout_secs", 15),
        ("token_expiry_margin_secs", 60),
        ("token_ttl_fallback_secs", 3600),
        ("chat_open_max_attempts", 60),
        ("chat_open_backoff_first_secs", 10),
        ("chat_open_backoff_later_secs", 30),
        ("dedup_cap", 10_000),
        ("quota_backoff_secs", 60),
        ("poll_retry_delay_ms", 2000),
        ("polling_interval_ms", 5000),
        ("chat_poll_retry_secs", 5),
        ("oauth_capture_timeout_secs", 120),
        ("outbound_queue_cap", 64),
        ("reconnect_base_secs", 1),
        ("reconnect_max_secs", 30),
        ("stream_fetch_retry_secs", 15),
        ("stream_scan_interval_secs", 30),
        ("passive_scan_interval_secs", 300),
        ("prompt_timeout_secs", 300),
    ];
    for (key, value) in defaults {
        if ms.get(key).is_none() {
            ms[key] = serde_json::json!(value);
        }
    }
    // `passive` is a bool, so it is backfilled separately from the numeric table.
    if ms.get("passive").is_none() {
        ms["passive"] = serde_json::json!(false);
    }
    // Migrate the old poll-retry default (500ms — it hammered the API under
    // quota pressure) to the new less-aggressive base. Only rewritten while it
    // still holds the OLD default, so an explicit operator choice survives.
    if ms.get("poll_retry_delay_ms").and_then(|v| v.as_i64()) == Some(500) {
        ms["poll_retry_delay_ms"] = serde_json::json!(2000);
    }
    if ms != before {
        json_val["module_specific"] = ms;
        if let Ok(pretty) = serde_json::to_string_pretty(&json_val) {
            let _ = std::fs::write(path, pretty);
        }
    }
}

/// Expected `.env` keys for this adapter. Each is a SECRET with no meaningful
/// default, so it is written EMPTY as a placeholder — the operator (or the TUI
/// credential flow) fills the value in. A real environment variable always wins
/// over the file at load (load_env_file only sets unset vars).
const ENV_KEYS: &[&str] = &[
    "YOUTUBE_API_KEY",
    "google_oauth_client_id",
    "google_oauth_client_secret",
    "refresh_token",
];

/// Create `.env` if missing and ensure every expected key is present (empty
/// `KEY=` line). Existing keys/values are NEVER overwritten. The file is kept
/// owner-only (0o600 on unix) since it holds secrets.
fn ensure_env_file() {
    ensure_env_file_at(std::path::Path::new(".env"));
}

fn ensure_env_file_at(path: &std::path::Path) {
    let mut lines: Vec<String> = std::fs::read_to_string(path)
        .map(|c| c.lines().map(|l| l.to_string()).collect())
        .unwrap_or_default();
    for key in ENV_KEYS {
        let prefix = format!("{}=", key);
        if !lines.iter().any(|l| l.trim().starts_with(&prefix)) {
            lines.push(prefix);
        }
    }
    let mut content = lines.join("\n");
    if !content.ends_with('\n') {
        content.push('\n');
    }
    if std::fs::write(path, content).is_ok() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }
    }
}

fn load_adapter_config() -> Option<YoutubeAdapterConfig> {
    // Secrets live in `.env` (loaded into env at startup); channel/settings
    // come from config.json. Ensure the tuning knobs exist on disk.
    backfill_tuning_defaults();
    let settings: YoutubeAdapterConfig = std::fs::read_to_string("config.json")
        .ok()
        .and_then(|data| serde_json::from_str::<serde_json::Value>(&data).ok())
        .and_then(|v| v.get("module_specific").cloned())
        .and_then(|s| serde_json::from_value(s).ok())
        .unwrap_or_default();

    let mut cfg = YoutubeAdapterConfig {
        channel_id: settings.channel_id.clone().filter(|s| !s.is_empty()),
        api_keys: settings.api_keys.clone().filter(|k| !k.is_empty()),
        unlisted_video_ids: settings.unlisted_video_ids.clone(),
        default_timeout_secs: settings.default_timeout_secs,
        http_timeout_secs: settings.http_timeout_secs,
        token_expiry_margin_secs: settings.token_expiry_margin_secs,
        token_ttl_fallback_secs: settings.token_ttl_fallback_secs,
        chat_open_max_attempts: settings.chat_open_max_attempts,
        chat_open_backoff_first_secs: settings.chat_open_backoff_first_secs,
        chat_open_backoff_later_secs: settings.chat_open_backoff_later_secs,
        dedup_cap: settings.dedup_cap,
        quota_backoff_secs: settings.quota_backoff_secs,
        poll_retry_delay_ms: settings.poll_retry_delay_ms,
        polling_interval_ms: settings.polling_interval_ms,
        chat_poll_retry_secs: settings.chat_poll_retry_secs,
        oauth_capture_timeout_secs: settings.oauth_capture_timeout_secs,
        outbound_queue_cap: settings.outbound_queue_cap,
        reconnect_base_secs: settings.reconnect_base_secs,
        reconnect_max_secs: settings.reconnect_max_secs,
        stream_fetch_retry_secs: settings.stream_fetch_retry_secs,
        stream_scan_interval_secs: settings.stream_scan_interval_secs,
        prompt_timeout_secs: settings.prompt_timeout_secs,
        ..Default::default()
    };
    if cfg.channel_id.is_none() {
        cfg.channel_id = Some(std::env::var("YOUTUBE_CHANNEL_ID").unwrap_or_default()).filter(|s| !s.is_empty());
    }
    if cfg.api_keys.is_none() {
        if let Ok(keys) = std::env::var("YOUTUBE_API_KEY") {
            let parts: Vec<String> = keys
                .split('\n')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !parts.is_empty() {
                cfg.api_keys = Some(parts);
            }
        }
    }
    // OAuth secrets from `.env` (key-named env vars, no adapter prefix).
    for (field, env) in [
        (&mut cfg.google_oauth_client_id, "google_oauth_client_id"),
        (&mut cfg.google_oauth_client_secret, "google_oauth_client_secret"),
        (&mut cfg.refresh_token, "refresh_token"),
    ] {
        let v = std::env::var(env).unwrap_or_default();
        if !v.is_empty() {
            *field = Some(v);
        }
    }
    Some(cfg)
}

fn save_adapter_config(
    channel_id: &str,
    api_keys: &[String],
    unlisted_ids: &[String],
    oauth_client_id: &str,
    oauth_client_secret: &str,
    refresh_token: &str,
) {
    // Preserve existing OAuth values unless a new one is supplied.
    let existing = load_adapter_config();
    let oauth_client_id = if oauth_client_id.is_empty() {
        existing.as_ref().and_then(|c| c.google_oauth_client_id.clone()).unwrap_or_default()
    } else {
        oauth_client_id.to_string()
    };
    let oauth_client_secret = if oauth_client_secret.is_empty() {
        existing.as_ref().and_then(|c| c.google_oauth_client_secret.clone()).unwrap_or_default()
    } else {
        oauth_client_secret.to_string()
    };
    let refresh_token = if refresh_token.is_empty() {
        existing.as_ref().and_then(|c| c.refresh_token.clone()).unwrap_or_default()
    } else {
        refresh_token.to_string()
    };

    // Secrets → `.env`; settings (channel, unlisted ids) → config.json.
    cockatiel_client::write_env_file(
        ".env",
        &[
            ("YOUTUBE_CHANNEL_ID", channel_id),
            ("YOUTUBE_API_KEY", &serde_json::to_string(api_keys).unwrap_or_default()),
            ("google_oauth_client_id", &oauth_client_id),
            ("google_oauth_client_secret", &oauth_client_secret),
            ("refresh_token", &refresh_token),
        ],
    );

    let path = PathBuf::from("config.json");
    let mut json_val = if let Ok(data) = std::fs::read_to_string(&path) {
        serde_json::from_str::<serde_json::Value>(&data).unwrap_or_else(|_| json!({}))
    } else {
        json!({})
    };
    // Preserve any existing `module_specific` keys (tuning knobs, OAuth
    // settings) — only channel/api/unlisted are (re)written, so a save never
    // drops a configured tuning value.
    let mut spec = json_val
        .get("module_specific")
        .cloned()
        .and_then(|v| v.as_object().cloned())
        .map(serde_json::Value::Object)
        .unwrap_or_else(|| json!({}));
    spec["channel_id"] = json!(channel_id);
    spec["api_key"] = json!(api_keys.first().cloned().unwrap_or_default());
    spec["api_keys"] = json!(api_keys);
    if !unlisted_ids.is_empty() {
        spec["unlisted_video_ids"] = json!(unlisted_ids);
    }
    json_val["module_specific"] = spec;
    if let Ok(pretty) = serde_json::to_string_pretty(&json_val) {
        let _ = std::fs::write(&path, pretty);
        info!("Successfully saved YouTube configuration (secrets → .env)");
    }
}

/// The operator's choice when a configured channel/video target is invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TieChoice {
    /// Re-test the same target (the failure may be transient).
    Retry,
    /// Keep the target but skip it for now (noted in the log).
    Ignore,
    /// Remove the target from the config.
    Remove,
    /// Prompt for a replacement target and re-test.
    Edit,
}

fn parse_tie_choice(answer: &str) -> Option<TieChoice> {
    match answer.trim().to_ascii_lowercase().as_str() {
        "t" | "try" | "retry" | "try again" => Some(TieChoice::Retry),
        "i" | "ignore" => Some(TieChoice::Ignore),
        "r" | "remove" => Some(TieChoice::Remove),
        "e" | "edit" => Some(TieChoice::Edit),
        _ => None,
    }
}

fn tie_choices_help() -> String {
    "Enter one of: (t)ry again, (i)gnore, (r)emove, (e)dit".to_string()
}

/// Ask the operator how to handle an invalid channel/video target: (t)ry /
/// (i)gnore / (r)emove / (e)dit. Reprompts until a valid choice (or None on
/// cancel).
async fn prompt_tie_choice(
    write_ws: &tokio::sync::Mutex<WsWriteHalf>,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
    subject: &str,
    prompt_timeout_secs: u32,
) -> Option<TieChoice> {
    loop {
        let answer = prompt_for_input(
            write_ws,
            prompt_rx,
            auth_token,
            module_name,
            instance_uuid,
            "Invalid YouTube Entry",
            &format!("{}\n\n{}", subject, tie_choices_help()),
            "t / i / r / e",
            PromptKind::String,
            prompt_timeout_secs,
        )
        .await;
        match answer.as_deref().and_then(parse_tie_choice) {
            Some(c) => return Some(c),
            None if answer.is_some() => {
                warn!("Unrecognized choice — expected t / i / r / e.");
            }
            None => return None, // cancelled
        }
    }
}

// ── OAuth2 token management (for sending to live chat) ────────────────

/// Persist only the OAuth refresh token (a secret → `.env`), leaving
/// channel/api keys untouched.
fn save_oauth_refresh_token(refresh_token: &str) {
    cockatiel_client::write_env_file(".env", &[("refresh_token", refresh_token)]);
}

const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const YT_FORCE_SSL: &str = "https://www.googleapis.com/auth/youtube.force-ssl";

/// Loopback port for the OAuth redirect listener. Read from the top-level
/// `oauth_redirect_port` key in config.json (default 3000, so existing
/// platform-console registrations keep working).
fn load_oauth_redirect_port() -> u16 {
    std::fs::read_to_string("config.json")
        .ok()
        .and_then(|data| serde_json::from_str::<serde_json::Value>(&data).ok())
        .and_then(|v| v.get("oauth_redirect_port").cloned())
        .and_then(|p| p.as_u64())
        .filter(|p| (1..=u16::MAX as u64).contains(p))
        .map(|p| p as u16)
        .unwrap_or(3000)
}

/// Google's `expires_in` → access-token TTL in seconds. Google may return a
/// value < 60 (or 0/missing); computing `expires_in - margin` raw would
/// underflow to ~5.8e11 seconds and cache a token that effectively never
/// refreshes. Saturate instead and fall back to the configured default TTL
/// when `expires_in` is absent/zero so the token is always refreshed early
/// enough.
fn access_token_ttl_secs(expires_in: i32, margin_secs: u32, fallback_secs: u32) -> u32 {
    if expires_in <= 0 {
        fallback_secs
    } else {
        (expires_in as u32).saturating_sub(margin_secs)
    }
}

/// Manages a Google OAuth2 token for `youtube.force-ssl`: acquires a refresh
/// token via the browser flow (or accepts one pasted into the config), then
/// mints/refreshes short-lived access tokens on demand.
#[derive(Clone)]
struct OAuthManager {
    client_id: String,
    client_secret: String,
    oauth_redirect_port: u16,
    token_expiry_margin_secs: u32,
    token_ttl_fallback_secs: u32,
    refresh_token: Arc<Mutex<Option<String>>>,
    access_token: Arc<Mutex<Option<(String, Instant)>>>,
}

impl OAuthManager {
    fn new(
        client_id: &str,
        client_secret: &str,
        refresh_token: Option<String>,
        oauth_redirect_port: u16,
        token_expiry_margin_secs: u32,
        token_ttl_fallback_secs: u32,
    ) -> Self {
        Self {
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            oauth_redirect_port,
            token_expiry_margin_secs,
            token_ttl_fallback_secs,
            refresh_token: Arc::new(Mutex::new(refresh_token)),
            access_token: Arc::new(Mutex::new(None)),
        }
    }

    fn redirect_uri(&self) -> String {
        format!("http://localhost:{}", self.oauth_redirect_port)
    }

    fn has_creds(&self) -> bool {
        !self.client_id.is_empty() && !self.client_secret.is_empty()
    }

    fn set_refresh_token(&self, token: &str) {
        *self.refresh_token.lock().unwrap() = Some(token.to_string());
    }

    /// Open the browser and capture the OAuth authorization code back on the
    /// configured loopback redirect port (Google uses a code, not a fragment).
    async fn capture_code(&self, auth_url: &str) -> Result<String, String> {
        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", self.oauth_redirect_port))
            .await
            .map_err(|e| format!("could not bind localhost:{}: {}", self.oauth_redirect_port, e))?;
        let _ = open::that(auth_url);

        let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
        let mut buf = [0; 8192];
        let n = socket.read(&mut buf).await.map_err(|e| e.to_string())?;
        let request = String::from_utf8_lossy(&buf[..n]).to_string();

        let ok_html = "<html><body style='background:#0e0e10;color:#efeff1;font-family:system-ui,sans-serif;text-align:center;padding-top:120px;'><h1 style='color:#a970ff;'>YouTube authorization successful!</h1><p>You can close this window.</p></body></html>";
        let error_html = "<html><body style='background:#0e0e10;color:#efeff1;font-family:system-ui,sans-serif;text-align:center;padding-top:120px;'><h1 style='color:#ff4f4f;'>Authorization failed</h1></body></html>";

        if request.contains("error=") {
            let resp = format!("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n{}", error_html);
            let _ = socket.write_all(resp.as_bytes()).await;
            return Err("Google OAuth returned an error".to_string());
        }
        // Extract ?code= from the GET request line.
        if let Some(pos) = request.find("?code=") {
            let start = pos + 6;
            let end = request[start..].find(' ').unwrap_or(request[start..].len());
            let code: String = request[start..start + end].chars().take_while(|c| *c != '&').collect();
            if !code.is_empty() {
                let resp = format!("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n{}", ok_html);
                let _ = socket.write_all(resp.as_bytes()).await;
                return Ok(code);
            }
        }
        let resp = format!("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n{}", error_html);
        let _ = socket.write_all(resp.as_bytes()).await;
        Err("No authorization code in the redirect".to_string())
    }

    /// Run the browser flow to obtain a refresh token (and the first access
    /// token). Requires `access_type=offline` + `prompt=consent` so a refresh
    /// token is always returned.
    async fn acquire_refresh_token(&self, client: &reqwest::Client) -> Result<String, String> {
        let auth_url = format!(
            "https://accounts.google.com/o/oauth2/v2/auth?client_id={}&redirect_uri={}&response_type=code&scope={}&access_type=offline&prompt=consent",
            self.client_id,
            self.redirect_uri(),
            urlencode(YT_FORCE_SSL)
        );
        let code = self.capture_code(&auth_url).await?;

        let resp: serde_json::Value = client
            .post(GOOGLE_TOKEN_URL)
            .form(&[
                ("code", code.as_str()),
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("redirect_uri", self.redirect_uri().as_str()),
                ("grant_type", "authorization_code"),
            ])
            .send()
            .await
            .map_err(|e| format!("token exchange failed: {}", e))?
            .json()
            .await
            .map_err(|e| format!("token exchange parse failed: {}", e))?;

        let refresh = resp
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("no refresh_token in response: {:?}", resp))?
            .to_string();
        let access = resp.get("access_token").and_then(|v| v.as_str()).unwrap_or("");
        let expires_in = resp
            .get("expires_in")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32)
            .unwrap_or(self.token_ttl_fallback_secs as i32);

        self.set_refresh_token(&refresh);
        if !access.is_empty() {
            *self.access_token.lock().unwrap() = Some((
                access.to_string(),
                Instant::now()
                    + std::time::Duration::from_secs(access_token_ttl_secs(
                        expires_in,
                        self.token_expiry_margin_secs,
                        self.token_ttl_fallback_secs,
                    ) as u64),
            ));
        }
        Ok(refresh)
    }

    /// Return a valid access token, refreshing from the refresh token when the
    /// cached one is missing or about to expire.
    async fn ensure_access_token(&self, client: &reqwest::Client) -> Result<String, String> {
        if let Some((token, expiry)) = self.access_token.lock().unwrap().clone() {
            if Instant::now() < expiry {
                return Ok(token);
            }
        }

        let refresh = self
            .refresh_token
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| "no refresh token (configure Google OAuth client id/secret or a refresh token)".to_string())?;

        let resp: serde_json::Value = client
            .post(GOOGLE_TOKEN_URL)
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("refresh_token", refresh.as_str()),
            ])
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(|e| format!("token refresh failed: {}", e))?
            .json()
            .await
            .map_err(|e| format!("token refresh parse failed: {}", e))?;

        if let Some(err) = resp.get("error").and_then(|e| e.as_str()) {
            if err == "invalid_grant" {
                *self.access_token.lock().unwrap() = None;
                *self.refresh_token.lock().unwrap() = None;
                return Err(
                    "Google OAuth refresh token is invalid or revoked (invalid_grant); re-authorize the module".to_string(),
                );
            }
        }

        let access = resp
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("no access_token in refresh response: {:?}", resp))?
            .to_string();

        if let Some(rotated) = rotated_refresh_token(&resp, &refresh) {
            self.set_refresh_token(&rotated);
            save_oauth_refresh_token(&rotated);
            info!("Google OAuth refresh token rotated and persisted.");
        }
        let expires_in = resp
            .get("expires_in")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32)
            .unwrap_or(self.token_ttl_fallback_secs as i32);
        *self.access_token.lock().unwrap() = Some((
            access.clone(),
            Instant::now()
                + std::time::Duration::from_secs(access_token_ttl_secs(
                    expires_in,
                    self.token_expiry_margin_secs,
                    self.token_ttl_fallback_secs,
                ) as u64),
        ));
        Ok(access)
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

/// Extracts an 11-character YouTube video ID from a raw ID or standard YouTube URL format
fn extract_video_id(input: &str) -> Option<String> {
    let s = input.trim();
    if s.is_empty() {
        return None;
    }

    // Direct 11-character video ID (YouTube video IDs are 11 chars: [a-zA-Z0-9_-])
    // Channel IDs are 24 chars starting with UC, so they will not match this.
    if s.len() == 11 && s.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
        return Some(s.to_string());
    }

    // URL with query parameter: https://www.youtube.com/watch?v=VIDEO_ID...
    if let Some(pos) = s.find("v=") {
        let after_v = &s[pos + 2..];
        let id: String = after_v
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
            .collect();
        if id.len() == 11 {
            return Some(id);
        }
    }

    // Shortened URL: https://youtu.be/VIDEO_ID...
    if let Some(pos) = s.find("youtu.be/") {
        let after = &s[pos + 9..];
        let id: String = after
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
            .collect();
        if id.len() == 11 {
            return Some(id);
        }
    }

    // Live URL: https://www.youtube.com/live/VIDEO_ID...
    if let Some(pos) = s.find("/live/") {
        let after = &s[pos + 6..];
        let id: String = after
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
            .collect();
        if id.len() == 11 {
            return Some(id);
        }
    }

    // Shorts URL: https://www.youtube.com/shorts/VIDEO_ID...
    if let Some(pos) = s.find("/shorts/") {
        let after = &s[pos + 8..];
        let id: String = after
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
            .collect();
        if id.len() == 11 {
            return Some(id);
        }
    }

    None
}

/// Fetches stream metadata directly by video ID via the YouTube Data API v3 videos endpoint.
/// Works for both public and unlisted streams with automatic key rotation on quota exceeded.
async fn fetch_video_stream(
    client: &reqwest::Client,
    video_id: &str,
    keys: &ApiKeyManager,
) -> Result<Option<StreamInfo>, Box<dyn std::error::Error>> {
    let max_attempts = keys.key_count().max(1);
    let mut found: Option<StreamInfo> = None;

    for attempt in 0..max_attempts {
        let api_key = keys.current_key();
        let url = format!(
            "https://www.googleapis.com/youtube/v3/videos?part=snippet,liveStreamingDetails,status&id={}&key={}",
            video_id, api_key
        );

        let res = client.get(&url).send().await?;
        let json = res.json::<serde_json::Value>().await?;

        if ApiKeyManager::is_key_error(&json) {
            warn!(
                "YouTube API quota exceeded for key {} while fetching video {}.",
                attempt + 1,
                video_id
            );
            if attempt + 1 < max_attempts {
                keys.rotate_to_next();
                continue;
            }
            break;
        }

        if let Some(err) = json.get("error") {
            if let Some(msg) = err.get("message").and_then(|m| m.as_str()) {
                error!("YouTube API error fetching video {}: {}", video_id, msg);
            }
            break;
        }

        if let Some(items) = json.get("items").and_then(|i| i.as_array()) {
            if let Some(item) = items.first() {
                let title = item
                    .get("snippet")
                    .and_then(|s| s.get("title"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("Untitled Stream")
                    .to_string();

                let published_at = item
                    .get("snippet")
                    .and_then(|s| s.get("publishedAt"))
                    .and_then(|p| p.as_str())
                    .unwrap_or("")
                    .to_string();

                let broadcast_content = item
                    .get("snippet")
                    .and_then(|s| s.get("liveBroadcastContent"))
                    .and_then(|b| b.as_str())
                    .unwrap_or("");

                let details = item.get("liveStreamingDetails");
                let is_ended = details.and_then(|l| l.get("actualEndTime")).is_some();
                if is_ended {
                    info!("Stream {} has already ended.", video_id);
                } else {
                    let status = if broadcast_content == "live" {
                        "live".to_string()
                    } else if broadcast_content == "upcoming" || details.is_some() {
                        "upcoming".to_string()
                    } else {
                        "live".to_string()
                    };

                    found = Some(StreamInfo {
                        video_id: video_id.to_string(),
                        title,
                        status,
                        published_at,
                        scheduled_start: details
                            .and_then(|l| l.get("scheduledStartTime"))
                            .and_then(|s| s.as_str())
                            .unwrap_or("")
                            .to_string(),
                    });
                }
            }
        }
        break;
    }

    Ok(found)
}

/// Resolves a handle (e.g., @vulbyte or vulbyte), channel URL, or channel ID into an exact YouTube Channel ID (UC...)
async fn get_channel_id(
    client: &reqwest::Client,
    input: &str,
    keys: &ApiKeyManager,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut clean_input = input.trim();

    // If it's a URL like https://www.youtube.com/channel/UC..., extract the channel ID
    if let Some(pos) = clean_input.find("/channel/") {
        let after = &clean_input[pos + 9..];
        let cid: String = after
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
            .collect();
        if cid.starts_with("UC") && cid.len() == 24 {
            return Ok(cid);
        }
    }

    // If it's a URL like https://www.youtube.com/@vulbyte, extract the handle
    if let Some(pos) = clean_input.find("/@") {
        clean_input = &clean_input[pos + 1..];
    }

    if clean_input.starts_with("UC") && clean_input.len() == 24 {
        return Ok(clean_input.to_string());
    }

    let raw_handle = clean_input.strip_prefix('@').unwrap_or(clean_input);

    let max_rotations = keys.key_count().max(1);

    for _ in 0..max_rotations {
        let api_key = keys.current_key();

        // 1. Try YouTube Data API v3 channels.list with forHandle using URL-encoded %40 (official handle requirement)
        let encoded_handle = format!("%40{}", raw_handle);
        let url_with_at = format!(
            "https://www.googleapis.com/youtube/v3/channels?part=id&forHandle={}&key={}",
            encoded_handle, api_key
        );

        if let Ok(res) = client.get(&url_with_at).send().await {
            if let Ok(json) = res.json::<serde_json::Value>().await {
                if ApiKeyManager::is_key_error(&json) {
                    warn!("API key quota exceeded resolving handle with @. Rotating key...");
                    keys.rotate_to_next();
                    continue;
                }
                if let Some(items) = json.get("items").and_then(|i| i.as_array()) {
                    if let Some(first) = items.first() {
                        if let Some(id) = first.get("id").and_then(|i| i.as_str()) {
                            info!("Resolved handle '@{}' to Channel ID: {}", raw_handle, id);
                            return Ok(id.to_string());
                        }
                    }
                }
            }
        }

        // 2. Try YouTube Data API v3 channels.list with forHandle without @
        let url_without_at = format!(
            "https://www.googleapis.com/youtube/v3/channels?part=id&forHandle={}&key={}",
            raw_handle, api_key
        );
        if let Ok(res) = client.get(&url_without_at).send().await {
            if let Ok(json) = res.json::<serde_json::Value>().await {
                if ApiKeyManager::is_key_error(&json) {
                    warn!("API key quota exceeded resolving handle without @. Rotating key...");
                    keys.rotate_to_next();
                    continue;
                }
                if let Some(items) = json.get("items").and_then(|i| i.as_array()) {
                    if let Some(first) = items.first() {
                        if let Some(id) = first.get("id").and_then(|i| i.as_str()) {
                            info!("Resolved handle '{}' to Channel ID: {}", raw_handle, id);
                            return Ok(id.to_string());
                        }
                    }
                }
            }
        }

        // 3. Try legacy forUsername
        let url_username = format!(
            "https://www.googleapis.com/youtube/v3/channels?part=id&forUsername={}&key={}",
            raw_handle, api_key
        );
        if let Ok(res) = client.get(&url_username).send().await {
            if let Ok(json) = res.json::<serde_json::Value>().await {
                if ApiKeyManager::is_key_error(&json) {
                    warn!("API key quota exceeded resolving legacy username. Rotating key...");
                    keys.rotate_to_next();
                    continue;
                }
                if let Some(items) = json.get("items").and_then(|i| i.as_array()) {
                    if let Some(first) = items.first() {
                        if let Some(id) = first.get("id").and_then(|i| i.as_str()) {
                            info!("Resolved username '{}' to Channel ID: {}", raw_handle, id);
                            return Ok(id.to_string());
                        }
                    }
                }
            }
        }
        break;
    }

    Err(format!(
        "Failed to resolve channel handle/ID '{}'. YouTube requires a 24-character Channel ID starting with 'UC'. Please enter your exact Channel ID (from YouTube Studio -> Customization -> Basic Info) or handle.",
        clean_input
    ).into())
}

async fn fetch_streams(
    client: &reqwest::Client,
    channel_id: &str,
    keys: &ApiKeyManager,
    unlisted_video_ids: &[String],
) -> Result<Vec<StreamInfo>, Box<dyn std::error::Error>> {
    let mut all_streams = Vec::new();

    // 1. Fetch any configured unlisted/direct stream IDs first
    for vid in unlisted_video_ids {
        match fetch_video_stream(client, vid, keys).await {
            Ok(Some(info)) => {
                if !all_streams.iter().any(|s: &StreamInfo| s.video_id == info.video_id) {
                    all_streams.push(info);
                }
            }
            Ok(None) => {}
            Err(e) => {
                error!("Failed to fetch stream {}: {}", vid, e);
            }
        }
    }

    // 2. Query YouTube Data API v3 Search endpoint for live and upcoming streams
    if !channel_id.is_empty() {
        let event_types = vec!["live", "upcoming"];

        for event_type in event_types {
            let max_key_rotations = keys.key_count().max(1);
            for _ in 0..max_key_rotations {
                let api_key = keys.current_key();
                let url = format!(
                    "https://www.googleapis.com/youtube/v3/search?part=snippet&channelId={}&eventType={}&type=video&key={}",
                    channel_id, event_type, api_key
                );

                match client.get(&url).send().await {
                    Ok(res) => {
                        if let Ok(json) = res.json::<serde_json::Value>().await {
                            if ApiKeyManager::is_key_error(&json) {
                                warn!(
                                    "YouTube Search API quota exceeded for eventType={}. Rotating API key...",
                                    event_type
                                );
                                keys.rotate_to_next();
                                continue;
                            }
                            if let Some(err) = json.get("error") {
                                if let Some(msg) = err.get("message").and_then(|m| m.as_str()) {
                                    error!(
                                        "YouTube Search API error ({}) for channel {}: {}",
                                        event_type, channel_id, msg
                                    );
                                }
                            }
                            if let Some(items) = json.get("items").and_then(|i| i.as_array()) {
                                for item in items {
                                    if let Some(id_obj) = item.get("id") {
                                        if let Some(video_id) =
                                            id_obj.get("videoId").and_then(|v| v.as_str())
                                        {
                                            if let Some(snippet) = item.get("snippet") {
                                                let title = snippet
                                                    .get("title")
                                                    .and_then(|t| t.as_str())
                                                    .unwrap_or("Untitled Stream")
                                                    .to_string();

                                                let published_at = snippet
                                                    .get("publishedAt")
                                                    .and_then(|p| p.as_str())
                                                    .unwrap_or("")
                                                    .to_string();

                                                let status = event_type.to_string();

                                                if !all_streams
                                                    .iter()
                                                    .any(|s: &StreamInfo| s.video_id == video_id)
                                                {
                                                    all_streams.push(StreamInfo {
                                                        video_id: video_id.to_string(),
                                                        title,
                                                        status,
                                                        published_at: published_at.clone(),
                                                        // Search items carry no scheduledStartTime; the publish date is the
                                                        // closest ordering signal and works for the chronological prompt.
                                                        scheduled_start: published_at,
                                                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to fetch {} streams via Search API: {}", event_type, e);
                    }
                }
                break;
            }
        }

    }

    // 3. Enrich every discovered stream with its REAL scheduled start time (and
    // the chat id) via the Data API `videos` endpoint. Search items carry the
    // publish date, not the scheduled start — ordering a prompt by publish date
    // puts an old test stream ahead of the one actually starting soonest. The
    // `videos` call is cheap (a handful of streams) and replaces the record in
    // place so the chronological sort below sees true start times.
    for stream in all_streams.iter_mut() {
        if let Ok(Some(enriched)) = fetch_video_stream(client, &stream.video_id, keys).await {
            stream.status = enriched.status;
            if !enriched.scheduled_start.is_empty() {
                stream.scheduled_start = enriched.scheduled_start;
            }
            if !enriched.title.is_empty() {
                stream.title = enriched.title;
            }
        }
    }

    Ok(all_streams)
}

/// Send a Prompt to the engine (forwarded to connected UIs) and wait for the
/// operator's response (`PromptResponse.reason`). Returns None on cancel/timeout.
///
/// The write lock is taken ONLY to send the prompt and released before waiting
/// for the answer — holding it across the wait would block the read loop's
/// AuthVerify reply and make the module look unresponsive to the engine's
/// liveness probe (which severs it).
/// Inform the operator (via a Boolean prompt) that every configured YouTube API
/// key is quota-exhausted, so chat monitoring is paused until the quota resets.
/// Fires once per monitored session (`notified` guards it). The prompt requires
/// acknowledgment — it does not block the poll loop (a timeout just continues).
async fn notify_all_keys_exhausted(
    write_ws: &tokio::sync::Mutex<WsWriteHalf>,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
    timeout_secs: u32,
    notified: &mut bool,
) {
    if *notified {
        return;
    }
    *notified = true;
    let _ = prompt_for_input(
        write_ws,
        prompt_rx,
        auth_token,
        module_name,
        instance_uuid,
        "YouTube API quota exhausted",
        "All configured YouTube Data API keys have hit their quota, so live chat \
         monitoring is paused.\n\n\
         The quota resets automatically (typically every 24h, or per-request budget). \
         The adapter will keep trying and resume chat automatically once a key is \
         available again.\n\n\
         Press OK to acknowledge. If this keeps happening, add more API keys (module \
         row > C > 'API Keys — one per line').",
        "Acknowledge",
        PromptKind::Boolean,
        timeout_secs,
    )
    .await;
}
async fn prompt_for_input(
    write_ws: &tokio::sync::Mutex<WsWriteHalf>,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
    title: &str,
    details: &str,
    input_label: &str,
    kind: PromptKind,
    timeout: u32,
) -> Option<String> {
    let prompt_id = uuid::Uuid::now_v7().to_string();
    let prompt_type = match kind {
        PromptKind::Boolean => PromptType::Boolean,
        PromptKind::String => PromptType::String,
        PromptKind::Credential => PromptType::Credential,
    };
    let prompt = Prompt {
        prompt_id_uuid7: prompt_id.clone(),
        prompt: title.to_string(),
        details: details.to_string(),
        yes_dialog: "Submit".to_string(),
        no_dialog: "Cancel".to_string(),
        timeout,
        origin: module_name.to_string(),
        origin_uuid7: String::new(),
        instructions: String::new(),
        link: String::new(),
        input_label: input_label.to_string(),
        prompt_type: prompt_type as i32,
    };
    let container = ContainerForEngine {
        version: 2,
        auth_token: auth_token.to_string(),
        module_name: module_name.to_string(),
        module_instance_uuid7: instance_uuid.to_string(),
        payload: Some(EnginePayload::Prompt(prompt)),
    };
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_err() {
        error!(
            "Could not encode '{}' prompt for the engine — no UI can answer it.",
            title
        );
        return None;
    }
    {
        let mut w = write_ws.lock().await;
        if w.send(WsMessage::Binary(buf.into())).await.is_err() {
            warn!(
                "Could not send '{}' prompt to the engine — no UI can answer it. \
                 Is the TUI connected?",
                title
            );
            return None;
        }
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout as u64 + 10);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(10), prompt_rx.recv()).await {
            Ok(Some(resp)) if resp.prompt_id_uuid7 == prompt_id => {
                return if resp.accepted {
                    Some(resp.reason)
                } else {
                    None
                };
            }
            Ok(Some(_)) => continue, // a different prompt's response
            Ok(None) => {
                warn!(
                    "Prompt '{}' could not be delivered — the engine closed the prompt \
                     channel with no UI response. Connect the TUI to enter \
                     credentials/values.",
                    title
                );
                return None;
            }
            // The 10s poll interval elapsed with no response yet: keep waiting
            // until the real deadline (the `timeout` seconds above), rather than
            // bailing out 10 seconds in and auto-cancelling every prompt.
            Err(_) => continue,
        }
    }
    warn!(
        "Prompt '{}' timed out after {}s with no response — no UI answered it. \
         The TUI must be connected to enter credentials/values; the adapter cannot \
         continue setup without an answer.",
        title,
        timeout as u64 + 10
    );
    None
}

/// Prompts user to select a stream with a 30-second timeout fallback.
/// Also provides guidance and input prompt for unlisted streams.
/// Order streams chronologically for the selection prompt: live first, then
/// upcoming sorted by scheduled start (soonest first), falling back to the
/// publish date when the start time isn't known. ISO 8601 timestamps compare
/// lexically, which is the same as chronologically.
fn sort_streams_chronologically(streams: &[StreamInfo]) -> Vec<StreamInfo> {
    let mut sorted: Vec<StreamInfo> = streams.to_vec();
    sorted.sort_by(|a, b| {
        let a_live = a.status == "live";
        let b_live = b.status == "live";
        match (a_live, b_live) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => {
                let a_start = if !a.scheduled_start.is_empty() {
                    &a.scheduled_start
                } else {
                    &a.published_at
                };
                let b_start = if !b.scheduled_start.is_empty() {
                    &b.scheduled_start
                } else {
                    &b.published_at
                };
                a_start.cmp(b_start)
            }
        }
    });
    sorted
}

async fn select_stream(
    streams: &[StreamInfo],
    client: &reqwest::Client,
    keys: &ApiKeyManager,
    unlisted_ids: &mut Vec<String>,
    channel_id: &str,
    write_ws: &tokio::sync::Mutex<WsWriteHalf>,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
    prompt_timeout_secs: u32,
) -> Option<StreamInfo> {
    // Order the selection prompt chronologically: the soonest scheduled stream
    // first, the furthest-out last. Live streams sort ahead of upcoming (they
    // are already broadcasting). The prompt and the auto-select fallback both
    // read this sorted list, so the number the operator picks maps to the
    // stream they expect.
    let sorted = sort_streams_chronologically(streams);
    if streams.is_empty() {
        println!("\n==================================================");
        println!("        No Streams Currently Found                ");
        println!("==================================================");
        if !channel_id.is_empty() {
            println!("  Channel ID: {}", channel_id);
        }
        println!("  Active Keys: {} API key(s) configured", keys.key_count());
        println!("  WHY A STREAM MAY NOT SHOW UP:");
        println!("  * The stream must be FULLY SCHEDULED with a start time in");
        println!("    YouTube Studio (Live > Manage) before it becomes visible.");
        println!("  * Visibility must be Public or Unlisted. DRAFT and PRIVATE");
        println!("    streams are invisible to the Data API's Search results.");
        println!("  * Unlisted streams CANNOT be found via YouTube Search —");
        println!("    monitor them directly by entering a Video ID or URL.");
        println!("  * Public scheduled streams may take a moment to appear.");
        println!("  You can also monitor a stream directly by entering its");
        println!("  Video ID or URL below.");
        println!("==================================================\n");
        // Ask the operator (via a Prompt) for an unlisted video id.
        let input = prompt_for_input(
            write_ws,
            prompt_rx,
            auth_token,
            module_name,
            instance_uuid,
            "No active streams found",
            "No live or scheduled stream was found for this channel.\n\nWhy? The stream must be fully scheduled (with a start time) AND Public or Unlisted in YouTube Studio. Draft and Private streams are invisible to the Data API's Search results. Unlisted streams won't appear in Search either, so monitor them directly with a Video ID or URL.\n\nEnter the Video ID or URL to monitor it directly, or cancel to keep scanning.",
            "Video ID or URL",
            PromptKind::String,
            prompt_timeout_secs,
        )
        .await;
        if let Some(input) = input {
            if let Some(vid) = extract_video_id(&input) {
                info!("Fetching unlisted stream metadata for Video ID: {}", vid);
                match fetch_video_stream(client, &vid, keys).await {
                    Ok(Some(stream)) => {
                        if !unlisted_ids.contains(&vid) {
                            unlisted_ids.push(vid);
                            save_adapter_config(channel_id, keys.get_all_keys(), unlisted_ids, "", "", "");
                        }
                        return Some(stream);
                    }
                    Ok(None) => {
                        warn!("Video ID {} was not found or is private.", vid);
                    }
                    Err(e) => {
                        error!("Error fetching video {}: {}", vid, e);
                    }
                }
            } else {
                warn!("Invalid YouTube Video ID or URL format: {}", input);
            }
        }

        return None;
    }

    println!("\n==================================================");
    println!("        Available YouTube Streams                 ");
    println!("==================================================");
    for (i, stream) in sorted.iter().enumerate() {
        let tag = if stream.status == "live" {
            "[LIVE]"
        } else {
            "[UPCOMING]"
        };
        println!(
            "    [{}] {} {} (ID: {})",
            i + 1,
            tag,
            stream.title,
            stream.video_id
        );
    }
    println!("\n    > Enter a stream number [1-{}].", sorted.len());
    println!("    > Or enter 'u' to monitor an Unlisted Stream URL/Video ID.");
    println!("    > If no selection is made, the newest stream will be auto-selected.\n");

    // Ask the operator (via a Prompt) for a selection.
    let mut details = format!("Select a stream to monitor.\n\n");
    for (i, stream) in sorted.iter().enumerate() {
        details.push_str(&format!("{}: {} ({})\n", i + 1, stream.title, stream.video_id));
    }
    details.push_str("\nEnter a stream number, 'u' for an unlisted video, or leave empty to auto-select.");
    let input_res = prompt_for_input(
        write_ws,
        prompt_rx,
        auth_token,
        module_name,
        instance_uuid,
        "Select a stream",
        &details,
        "Stream number (or 'u')",
        PromptKind::String,
        prompt_timeout_secs,
    )
    .await;

    if let Some(user_input) = input_res {
        let user_input = user_input.trim().to_string();
        if user_input.to_lowercase() == "u" {
            let unlisted_input = prompt_for_input(
                write_ws,
                prompt_rx,
                auth_token,
                module_name,
                instance_uuid,
                "Monitor an unlisted video",
                "Enter an Unlisted Video ID or URL to monitor it.",
                "Unlisted Video ID or URL",
                PromptKind::String,
                prompt_timeout_secs,
            )
            .await;
            if let Some(unlisted_input) = unlisted_input {
                if let Some(vid) = extract_video_id(&unlisted_input) {
                    info!("Fetching unlisted stream metadata for Video ID: {}", vid);
                    match fetch_video_stream(client, &vid, keys).await {
                        Ok(Some(stream)) => {
                            if !unlisted_ids.contains(&vid) {
                                unlisted_ids.push(vid);
                                save_adapter_config(channel_id, keys.get_all_keys(), unlisted_ids, "", "", "");
                            }
                            return Some(stream);
                        }
                        Ok(None) => {
                            warn!("Video ID {} was not found or is private.", vid);
                        }
                        Err(e) => {
                            error!("Error fetching video {}: {}", vid, e);
                        }
                    }
                } else {
                    warn!("Invalid YouTube Video ID or URL format: {}", unlisted_input);
                }
            }
        } else if let Ok(num) = user_input.parse::<usize>() {
            if num > 0 && num <= sorted.len() {
                return Some(sorted[num - 1].clone());
            }
        }
    }

    // Fallback: pick the first live stream, or the soonest upcoming stream.
    // `sorted` is already chronological (soonest first), so the first match is
    // the right choice for both.
    if let Some(live_stream) = sorted.iter().find(|s| s.status == "live") {
        println!("    > Auto-selected live stream: {}", live_stream.title);
        return Some(live_stream.clone());
    }
    if let Some(upcoming_stream) = sorted.iter().find(|s| s.status == "upcoming") {
        println!("    > Auto-selected upcoming stream: {}", upcoming_stream.title);
        return Some(upcoming_stream.clone());
    }

    sorted.first().cloned()
}

/// Polls live chat for the selected stream until it ends, with automatic key rotation on quota limits
/// Post a message to the active live chat using the OAuth access token.
async fn send_to_youtube(
    oauth: &OAuthManager,
    client: &reqwest::Client,
    live_chat: &Arc<Mutex<Option<String>>>,
    msg: &str,
) -> Result<(), String> {
    let chat_id = live_chat
        .lock()
        .unwrap()
        .clone()
        .ok_or_else(|| "no active live chat to send to".to_string())?;
    let access = oauth.ensure_access_token(client).await?;

    let body = json!({
        "snippet": {
            "liveChatId": chat_id,
            "type": "textMessageEvent",
            "textMessageDetails": { "messageText": msg },
        }
    });
    let resp = client
        .post("https://www.googleapis.com/youtube/v3/liveChat/messages?part=snippet")
        .bearer_auth(&access)
        .json(&body)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|e| format!("send request failed: {}", e))?;

    let status = resp.status();
    if status.is_success() {
        Ok(())
    } else {
        let text = resp.text().await.unwrap_or_default();
        Err(format!("YouTube API {}: {}", status, text))
    }
}

/// The MIME type to declare for a thumbnail file, from its extension. YouTube
/// accepts PNG and JPEG; anything else is declared JPEG and the API rejects it
/// with a clear error.
fn thumbnail_mime(path: &str) -> &'static str {
    if path.to_lowercase().ends_with(".png") {
        "image/png"
    } else {
        "image/jpeg"
    }
}

/// Merge the desired title / scheduled start into a video snippet, preserving
/// the fields the Data API requires (notably `categoryId`) and everything else
/// the caller did not set. Pure so the `videos.update` body is testable.
fn merge_video_snippet(
    snippet: &serde_json::Value,
    title: &str,
    scheduled_start_ms: i64,
) -> serde_json::Value {
    let mut merged = snippet.clone();
    if !title.is_empty() {
        merged["title"] = json!(title);
    }
    if scheduled_start_ms != 0 {
        // YouTube wants RFC3339 (UTC): epoch ms -> whole seconds.
        merged["scheduledStartTime"] =
            json!(format_utc_iso((scheduled_start_ms / 1000).max(0) as u64));
    }
    merged
}

/// Apply the stream control to `video_id` via the YouTube Data API:
/// `videos.update` for the title / scheduled start (read-modify-write, because
/// `videos.update` REPLACES the snippet), and `thumbnails.set` for the
/// thumbnail (the local file is uploaded by this service). Returns a short
/// human summary of what was applied.
async fn apply_youtube_stream_control(
    oauth: &OAuthManager,
    client: &reqwest::Client,
    video_id: &str,
    control: &serde_json::Value,
) -> Result<String, String> {
    if video_id.is_empty() {
        return Err("no active YouTube video to apply the stream control to".to_string());
    }
    let title = control.get("title").and_then(|v| v.as_str()).unwrap_or("");
    let scheduled_start_ms = control
        .get("scheduled_start_ms")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let thumbnail = control
        .get("thumbnail_path")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let access = oauth.ensure_access_token(client).await?;
    let mut applied: Vec<String> = Vec::new();

    if !title.is_empty() || scheduled_start_ms != 0 {
        // Read the current snippet first: `videos.update` replaces the whole
        // snippet, so the required `categoryId` (and anything else) must be
        // carried over or the API rejects the update.
        let get = client
            .get("https://www.googleapis.com/youtube/v3/videos")
            .query(&[("part", "snippet"), ("id", video_id)])
            .bearer_auth(&access)
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(|e| format!("videos.list failed: {e}"))?;
        let status = get.status();
        let body: serde_json::Value = get
            .json()
            .await
            .map_err(|e| format!("videos.list parse failed ({status}): {e}"))?;
        if !status.is_success() {
            return Err(format!("videos.list {status}: {body}"));
        }
        let snippet = body
            .get("items")
            .and_then(|a| a.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.get("snippet"))
            .cloned()
            .ok_or_else(|| format!("video {video_id} has no snippet to update"))?;
        let merged = merge_video_snippet(&snippet, title, scheduled_start_ms);
        let put = client
            .put("https://www.googleapis.com/youtube/v3/videos")
            .query(&[("part", "snippet")])
            .bearer_auth(&access)
            .json(&json!({ "id": video_id, "snippet": merged }))
            .timeout(Duration::from_secs(15))
            .send()
            .await
            .map_err(|e| format!("videos.update failed: {e}"))?;
        if !put.status().is_success() {
            let s = put.status();
            let text = put.text().await.unwrap_or_default();
            return Err(format!("videos.update {s}: {text}"));
        }
        let mut what = Vec::new();
        if !title.is_empty() {
            what.push("title");
        }
        if scheduled_start_ms != 0 {
            what.push("start");
        }
        applied.push(format!("set {}", what.join("+")));
    }

    if !thumbnail.is_empty() {
        let bytes = std::fs::read(thumbnail)
            .map_err(|e| format!("thumbnail '{thumbnail}' unreadable: {e}"))?;
        let set = client
            .post("https://www.googleapis.com/upload/youtube/v3/thumbnails/set")
            .query(&[("videoId", video_id)])
            .bearer_auth(&access)
            .header("Content-Type", thumbnail_mime(thumbnail))
            .body(bytes)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| format!("thumbnails.set failed: {e}"))?;
        if !set.status().is_success() {
            let s = set.status();
            let text = set.text().await.unwrap_or_default();
            return Err(format!("thumbnails.set {s}: {text}"));
        }
        applied.push("thumbnail".to_string());
    }

    if applied.is_empty() {
        Ok("nothing to apply".to_string())
    } else {
        Ok(applied.join("; "))
    }
}

/// Bounded set of recently-ingested live-chat message ids. Guards against
/// re-ingesting the same messages when a poll request times out AFTER the
/// server already processed it — the retry re-fetches the same page with the
/// same `next_page_token` and would otherwise feed duplicates into the
/// pipeline as brand-new empty-uuid MessagePreProcess messages. Keyed by the
/// platform's per-message `id` from the poll items; pruned by insertion
/// sequence to keep the set bounded (last ~10k ids).
struct SeenMessageIds {
    ids: HashMap<String, u64>,
    next_seq: u64,
    cap: usize,
}

impl SeenMessageIds {
    fn new(cap: usize) -> Self {
        Self {
            ids: HashMap::new(),
            next_seq: 0,
            cap,
        }
    }

    /// Record `id`; returns true if it was already seen (i.e. already
    /// ingested in a prior poll of the same page).
    fn is_seen(&mut self, id: &str) -> bool {
        if self.ids.contains_key(id) {
            return true;
        }
        self.ids.insert(id.to_string(), self.next_seq);
        self.next_seq += 1;
        // Prune the oldest entries by insertion sequence to stay bounded.
        if self.ids.len() > self.cap {
            let threshold = self.next_seq.saturating_sub(self.cap as u64);
            self.ids.retain(|_, seq| *seq > threshold);
        }
        false
    }
}

/// Exact engine-event message emitted when the monitored stream goes live.
fn stream_start_message(video_id: &str, started_at: &str, title: &str) -> String {
    format!(
        "[stream-start] youtube: video '{}' went live at {} — title: \"{}\"",
        video_id, started_at, title
    )
}

/// Atomically claim the once-per-session stream-start event. Returns `true`
/// for exactly one caller (the first to observe the stream live); every later
/// caller gets `false` and must stay silent.
fn claim_stream_start(fired: &AtomicBool) -> bool {
    !fired.swap(true, Ordering::SeqCst)
}

/// Format a unix timestamp as an ISO-8601 UTC string (`YYYY-MM-DDTHH:MM:SSZ`).
fn format_utc_iso(secs: u64) -> String {
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let hh = rem / 3_600;
    let mm = (rem % 3_600) / 60;
    let ss = rem % 60;
    // Howard Hinnant's civil-from-days conversion (proleptic Gregorian).
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y } as u64;
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m, d, hh, mm, ss)
}

/// Broadcast a Log container to the engine using the CURRENT session identity
/// (read at send time so reconnects are honored) — the engine surfaces Logs to
/// the TUI and persists them to the timeline.
async fn send_stream_start_log(
    write_ws: &tokio::sync::Mutex<WsWriteHalf>,
    identity: &Arc<tokio::sync::Mutex<EngineIdentity>>,
    log: &str,
) {
    let (auth, module, instance) = {
        let id = identity.lock().await;
        (id.auth.clone(), id.module.clone(), id.instance.clone())
    };
    let container = ContainerForEngine {
        version: 2,
        auth_token: auth,
        module_name: module,
        module_instance_uuid7: instance,
        payload: Some(EnginePayload::Log(cockatiel_client::proto::Log {
            log: log.to_string(),
            blob: vec![],
        })),
    };
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_ok() {
        let mut w = write_ws.lock().await;
        if let Err(e) = w.send(WsMessage::Binary(buf)).await {
            error!("Failed to send to engine: {}", e);
        }
    }
}

/// A donation (Super Chat / Super Sticker / membership / gift) parsed out of a
/// live-chat message. `amount_micros` is the only 64-bit value here — the
/// external-protocol exception: YouTube reports Super Chat amounts as
/// micro-currency strings that a 32-bit int cannot hold.
#[derive(Debug, Clone, PartialEq)]
struct Donation {
    kind: &'static str,
    amount_micros: i64,
    currency: String,
    display: String,
    message: String,
}

impl Donation {
    /// The readable line forwarded as `raw_message`: the user's comment when
    /// present, otherwise the synthesized display line.
    fn raw_message(&self) -> String {
        if self.message.is_empty() {
            self.display.clone()
        } else {
            self.message.clone()
        }
    }

    /// The `raw_data` payload the engine and term-chat parse. UTF-8 JSON.
    fn raw_data_json(&self) -> Vec<u8> {
        json!({
            "donation": {
                "kind": self.kind,
                "amount_micros": self.amount_micros,
                "currency": self.currency,
                "display": self.display,
                "message": self.message,
            }
        })
        .to_string()
        .into_bytes()
    }
}

/// Read a string field, defaulting to "".
fn json_str(v: Option<&serde_json::Value>) -> String {
    v.and_then(|v| v.as_str()).unwrap_or("").to_string()
}

/// Parse an amount from a JSON number or numeric string; 0 on absence/overflow.
fn parse_amount_micros(v: Option<&serde_json::Value>) -> i64 {
    match v {
        Some(serde_json::Value::String(s)) => s.parse::<i64>().unwrap_or(0),
        Some(serde_json::Value::Number(n)) => n.as_i64().unwrap_or(0),
        _ => 0,
    }
}

/// Detect and parse a donation from a REST `liveChatMessages` snippet. Returns
/// `None` for ordinary chat (or any snippet without a donation `type`), so
/// callers keep the existing drop-when-empty behaviour for non-donations.
fn donation_from_rest_snippet(snippet: &serde_json::Value) -> Option<Donation> {
    let ty = snippet.get("type").and_then(|t| t.as_str())?;
    let null = serde_json::Value::Null;
    let details = |key: &str| snippet.get(key).unwrap_or(&null);
    let donation = match ty {
        "superChatEvent" | "superStickerEvent" => {
            let sticker = ty == "superStickerEvent";
            let d = details(if sticker {
                "superStickerDetails"
            } else {
                "superChatDetails"
            });
            let display = json_str(d.get("amountDisplayString"));
            Donation {
                kind: if sticker { "supersticker" } else { "superchat" },
                amount_micros: parse_amount_micros(d.get("amountMicros")),
                currency: json_str(d.get("currency")),
                display: if display.is_empty() {
                    if sticker {
                        "Super Sticker".into()
                    } else {
                        "Super Chat".into()
                    }
                } else {
                    display
                },
                message: json_str(d.get("userComment")),
            }
        }
        "newSponsorEvent" => {
            let d = details("newSponsorDetails");
            let level = json_str(d.get("memberLevelName"));
            Donation {
                kind: "membership",
                amount_micros: 0,
                currency: String::new(),
                display: if level.is_empty() {
                    "New member".into()
                } else {
                    level
                },
                message: String::new(),
            }
        }
        "memberMilestoneChatEvent" => {
            let d = details("memberMilestoneChatDetails");
            let level = json_str(d.get("memberLevelName"));
            let month = d.get("memberMonth").and_then(|m| m.as_u64()).unwrap_or(0);
            let display = match (level.is_empty(), month) {
                (false, m) if m > 0 => format!("{level} ({m} months)"),
                (false, _) => level,
                (true, m) if m > 0 => format!("{m} months"),
                _ => "Member milestone".into(),
            };
            Donation {
                kind: "membership",
                amount_micros: 0,
                currency: String::new(),
                display,
                message: json_str(d.get("userComment")),
            }
        }
        "membershipGiftingEvent" => {
            let d = details("membershipGiftingDetails");
            let count = d
                .get("giftMembershipsCount")
                .and_then(|c| c.as_i64())
                .unwrap_or(0);
            let level = json_str(d.get("giftMembershipsLevelName"));
            let display = if level.is_empty() {
                format!("Gifted {count} memberships")
            } else {
                format!("Gifted {count} x {level}")
            };
            Donation {
                kind: "gift",
                amount_micros: 0,
                currency: String::new(),
                display,
                message: String::new(),
            }
        }
        "giftMembershipReceivedEvent" => {
            let d = details("giftMembershipReceivedDetails");
            let level = json_str(d.get("memberLevelName"));
            let display = if level.is_empty() {
                "Received a gifted membership".into()
            } else {
                format!("Received a gifted membership ({level})")
            };
            Donation {
                kind: "gift",
                amount_micros: 0,
                currency: String::new(),
                display,
                message: String::new(),
            }
        }
        "giftEvent" => {
            let d = details("giftDetails");
            let alt = json_str(d.get("altText"));
            let name = json_str(d.get("giftName"));
            let display = if !alt.is_empty() {
                alt
            } else if !name.is_empty() {
                name
            } else {
                "Gift".into()
            };
            Donation {
                kind: "gift",
                amount_micros: 0,
                currency: String::new(),
                display,
                message: String::new(),
            }
        }
        _ => return None,
    };
    Some(donation)
}

/// Push this video's current viewer count to the engine via a `ChannelStats`
/// payload. The engine stores it and serves it to other modules through the
/// `channel_viewers` virtual query. Reads the session identity at send time.
async fn push_channel_stats(
    write_ws: &tokio::sync::Mutex<WsWriteHalf>,
    identity: &Arc<tokio::sync::Mutex<EngineIdentity>>,
    platform: &str,
    channel: &str,
    viewers: i32,
    is_live: bool,
    title: &str,
) {
    let (auth, module, instance) = {
        let id = identity.lock().await;
        (id.auth.clone(), id.module.clone(), id.instance.clone())
    };
    let container = ContainerForEngine {
        version: 2,
        auth_token: auth,
        module_name: module,
        module_instance_uuid7: instance,
        payload: Some(EnginePayload::ChannelStats(cockatiel_client::proto::ChannelStats {
            platform: platform.to_string(),
            channel: channel.to_string(),
            viewers,
            is_live,
            title: title.to_string(),
            updated_at: now_unix_millis(),
        })),
    };
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_ok() {
        let mut w = write_ws.lock().await;
        let _ = w.send(WsMessage::Binary(buf)).await;
    }
}

/// Surface a non-blocking `StatusNotice` to the engine, which relays it to every
/// connected module for display. An empty `text` clears the notice for
/// `source`. Reads the session identity at send time so reconnects are honored.
async fn send_status_notice(
    write_ws: &tokio::sync::Mutex<WsWriteHalf>,
    identity: &Arc<tokio::sync::Mutex<EngineIdentity>>,
    source: &str,
    level: &str,
    text: &str,
    ttl_secs: i32,
) {
    let (auth, module, instance) = {
        let id = identity.lock().await;
        (id.auth.clone(), id.module.clone(), id.instance.clone())
    };
    let container = ContainerForEngine {
        version: 2,
        auth_token: auth,
        module_name: module,
        module_instance_uuid7: instance,
        payload: Some(EnginePayload::StatusNotice(cockatiel_client::proto::StatusNotice {
            source: source.to_string(),
            level: level.to_string(),
            text: text.to_string(),
            ttl_secs,
        })),
    };
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_ok() {
        let mut w = write_ws.lock().await;
        let _ = w.send(WsMessage::Binary(buf)).await;
    }
}

fn now_unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[allow(clippy::too_many_arguments)]
async fn monitor_stream_chat(
    write_ws: &tokio::sync::Mutex<WsWriteHalf>,
    identity: &Arc<tokio::sync::Mutex<EngineIdentity>>,
    client: &reqwest::Client,
    video_id: &str,
    keys: &ApiKeyManager,
    live_chat: &Arc<Mutex<Option<String>>>,
    tuning: &Tuning,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
) {
    let mut attempt = 0u32;
    // Fires the stream-start event exactly once per monitored session: the
    // first poll to observe the stream live claims it atomically; every later
    // poll is a no-op.
    let stream_start_fired = AtomicBool::new(false);
    // Whether the "all API keys exhausted" acknowledgment prompt has been shown
    // this session. Shown once so the operator isn't spammed every backoff.
    let mut notified_exhausted = false;
    let chat_id = 'found_chat: loop {
        attempt += 1;
        if attempt > tuning.chat_open_max_attempts {
            info!("Live chat failed to open after extended monitoring. Returning to stream discovery...");
            return;
        }

        // Resolve the stream's `activeLiveChatId` via the Data API. The same
        // call also yields the live status (for the stream-start event and the
        // viewer count) and the scheduled-wait state for an upcoming stream.
        let api_key = keys.current_key();
        let video_url = format!(
            "https://www.googleapis.com/youtube/v3/videos?part=snippet,liveStreamingDetails,status&id={}&key={}",
            video_id, api_key
        );

        match client.get(&video_url).send().await {
            Ok(res) => {
                if let Ok(json) = res.json::<serde_json::Value>().await {
                    if ApiKeyManager::is_key_error(&json) {
                        // The current key is exhausted: rotate to the next one so
                        // a healthy key can resolve the stream's activeLiveChatId.
                        // Without this the adapter sticks on the first (exhausted)
                        // key forever and can never enter live-chat monitoring.
                        if attempt == 1 {
                            warn!("Data API quota exceeded for video status — rotating to the next key.");
                        }
                        keys.rotate_to_next();
                    } else {
                        if let Some(err) = json.get("error") {
                            if attempt == 1 {
                                warn!("Data API unavailable for video status: {:?}", err.get("message").and_then(|m|m.as_str()).unwrap_or("unknown"));
                            }
                        }
                        if let Some(items) = json.get("items").and_then(|i| i.as_array()) {
                            if let Some(item) = items.first() {
                                if let Some(status) = item
                                    .get("status")
                                    .and_then(|s| s.get("uploadStatus"))
                                    .and_then(|u| u.as_str())
                                {
                                    if status == "processed" || status == "deleted" || status == "rejected" {
                                        info!("Stream has ended.");
                                        return;
                                    }
                                }
                                if let Some(details) = item.get("liveStreamingDetails") {
                                    if let Some(id) = details.get("activeLiveChatId").and_then(|c| c.as_str()) {
                                        *live_chat.lock().unwrap() = Some(id.to_string());
                                        // Push the current viewer count to the engine
                                        // so other modules can read it via the
                                        // channel_viewers query.
                                        let viewers = details
                                            .get("concurrentViewers")
                                            .and_then(|v| v.as_str())
                                            .and_then(|s| s.parse::<i32>().ok())
                                            .unwrap_or(0);
                                        let title = item
                                            .get("snippet")
                                            .and_then(|s| s.get("title"))
                                            .and_then(|t| t.as_str())
                                            .unwrap_or("");
                                        push_channel_stats(
                                            write_ws,
                                            identity,
                                            "youtube",
                                            video_id,
                                            viewers,
                                            true,
                                            title,
                                        )
                                        .await;
                                        // Stream went live: emit the start event once,
                                        // carrying YouTube's actualStartTime verbatim.
                                        if claim_stream_start(&stream_start_fired) {
                                            let started_at = details
                                                .get("actualStartTime")
                                                .and_then(|t| t.as_str())
                                                .unwrap_or("unknown");
                                            let title = item
                                                .get("snippet")
                                                .and_then(|s| s.get("title"))
                                                .and_then(|t| t.as_str())
                                                .unwrap_or("Untitled Stream");
                                            let log = stream_start_message(video_id, started_at, title);
                                            info!("{}", log);
                                            send_stream_start_log(write_ws, identity, &log).await;
                                        }
                                        break 'found_chat id.to_string();
                                    }
                                    if details.get("actualEndTime").is_some() {
                                        info!("Stream has concluded.");
                                        return;
                                    }
                                }
                                // No chat id yet: a scheduled stream is not
                                // broadcasting. Surface the wait state once (the
                                // keyed Data API is the only liveness source) and
                                // keep polling until it goes live.
                                let broadcast = item
                                    .get("snippet")
                                    .and_then(|s| s.get("liveBroadcastContent"))
                                    .and_then(|b| b.as_str())
                                    .unwrap_or("");
                                if broadcast == "upcoming" && attempt % 6 == 1 {
                                    let title = item
                                        .get("snippet")
                                        .and_then(|s| s.get("title"))
                                        .and_then(|t| t.as_str())
                                        .unwrap_or("Untitled Stream");
                                    info!(
                                        "Stream '{}' is upcoming. Waiting for it to go live...",
                                        title
                                    );
                                    if attempt == 1 {
                                        send_stream_start_log(
                                            write_ws,
                                            identity,
                                            &format!(
                                                "[youtube] monitoring scheduled stream '{}' ({}) — chat will appear when it goes live",
                                                title, video_id
                                            ),
                                        )
                                        .await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(_) => {}
        }

        // Exponential-ish backoff: a short delay for the first few attempts,
        // then a longer one.
        let delay = if attempt < 6 {
            tuning.chat_open_backoff_first_secs
        } else {
            tuning.chat_open_backoff_later_secs
        };
        tokio::time::sleep(tokio::time::Duration::from_secs(delay as u64)).await;
    };

    poll_keyed_chat(
        write_ws,
        identity,
        client,
        keys,
        live_chat,
        tuning,
        prompt_rx,
        auth_token,
        module_name,
        instance_uuid,
        &mut notified_exhausted,
        chat_id,
    )
    .await;
}

/// Live-chat poll interval bounds (ms). The Data API's `pollingIntervalMillis`
/// can hand back absurd values under load; clamp both it and the config
/// fallback to a sane [1s, 60s] range before any jitter is applied.
const POLL_INTERVAL_MIN_MS: u64 = 1_000;
const POLL_INTERVAL_MAX_MS: u64 = 60_000;

/// Clamp a poll interval to [`POLL_INTERVAL_MIN_MS`, `POLL_INTERVAL_MAX_MS`]
/// then spread it by ±10%. The jitter comes from a cheap time-seeded xorshift
/// (no RNG dependency) so independent pollers don't align on the same cadence.
fn jittered_poll_interval_ms(interval_ms: u64) -> u64 {
    let clamped = interval_ms.clamp(POLL_INTERVAL_MIN_MS, POLL_INTERVAL_MAX_MS);
    let span = clamped / 10;
    if span == 0 {
        return clamped;
    }
    let mut x = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0)
        .wrapping_add(0x9E37_79B9_7F4A_7C15)
        .max(1);
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    let offset = (x % (2 * span + 1)) as i64 - span as i64;
    (clamped as i64 + offset).clamp(POLL_INTERVAL_MIN_MS as i64, POLL_INTERVAL_MAX_MS as i64)
        as u64
}

/// Poll the KEYED YouTube Data API live-chat path until the chat closes. The
/// continuation (`nextPageToken`), key rotation and quota backoff live here.
#[allow(clippy::too_many_arguments)]
async fn poll_keyed_chat(
    write_ws: &tokio::sync::Mutex<WsWriteHalf>,
    identity: &Arc<tokio::sync::Mutex<EngineIdentity>>,
    client: &reqwest::Client,
    keys: &ApiKeyManager,
    live_chat: &Arc<Mutex<Option<String>>>,
    tuning: &Tuning,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
    notified_exhausted: &mut bool,
    chat_id: String,
) {
    info!(
        "Connected to live chat ID: {}. Polling messages...",
        chat_id
    );
    let mut next_page_token: Option<String> = None;
    // Consecutive quota failures on the poll path. Each failure backs off
    // exponentially; once every configured key has failed in a row the whole
    // ring is exhausted, so wait the full quota backoff and only resume fast
    // polling once a key answers non-quota again.
    let mut quota_failures: u32 = 0;
    // Whether the quota-exhausted StatusNotice is currently showing, so it is
    // published once per exhausted episode and cleared on the next success.
    let mut notice_exhausted = false;
    // Live-chat message ids already forwarded to the engine this session, so a
    // timed-out poll retried with the same page token never re-ingests them.
    let mut seen_ids = SeenMessageIds::new(tuning.dedup_cap);
    // Token of the last page successfully processed. If we re-poll that same
    // token (a retry after a timeout) AND the items carry no usable ids, the
    // page was already forwarded — skip it instead of re-ingesting.
    let mut last_processed_token: Option<String> = None;
    // Log the effective (clamped + jittered) poll interval once so the operator
    // can see the rate the adapter actually settled into.
    let mut interval_logged = false;

    loop {
        let api_key = keys.current_key();
        let mut chat_url = format!(
            "https://www.googleapis.com/youtube/v3/liveChat/messages?liveChatId={}&part=snippet,authorDetails&key={}",
            chat_id, api_key
        );
        let requested_token = next_page_token.clone();
        if let Some(ref token) = requested_token {
            chat_url.push_str(&format!("&pageToken={}", token));
        }

        match client.get(&chat_url).send().await {
            Ok(res) => {
                let status = res.status();

                if let Ok(json) = res.json::<serde_json::Value>().await {
                    if ApiKeyManager::is_key_error(&json) {
                        if ApiKeyManager::is_quota_error(&json) {
                            quota_failures += 1;
                            let key_count = keys.key_count().max(1) as u32;
                            warn!(
                                "API key quota exceeded while polling live chat messages (failure {}/{}) — rotating API key...",
                                quota_failures, key_count
                            );
                            keys.rotate_to_next();
                            // Full ring rotated with no success: back off instead of
                            // spinning against a quota-exhausted API.
                            let delay = if quota_failures >= key_count {
                                info!("All configured API keys are quota-exhausted; backing off {}s before polling again.", tuning.quota_backoff_secs);
                                // Surface a non-blocking banner so the UI shows
                                // chat is paused on API limits (published once
                                // per exhausted episode).
                                if !notice_exhausted {
                                    notice_exhausted = true;
                                    send_status_notice(
                                        write_ws,
                                        identity,
                                        "youtube",
                                        "warning",
                                        "YouTube API quota exhausted — all configured keys are at \
                                         their limit, so live chat monitoring is paused. The quota \
                                         resets automatically (typically every 24h); the adapter \
                                         will resume chat as soon as a key is available again.",
                                        0,
                                    )
                                    .await;
                                }
                                // Surface it to the operator ONCE per session: a
                                // Boolean prompt they must acknowledge so they know
                                // chat monitoring is paused on API limits (the keys
                                // will recover on their own when quota resets).
                                notify_all_keys_exhausted(
                                    write_ws,
                                    prompt_rx,
                                    auth_token,
                                    module_name,
                                    instance_uuid,
                                    tuning.prompt_timeout_secs,
                                    notified_exhausted,
                                )
                                .await;
                                std::time::Duration::from_secs(tuning.quota_backoff_secs as u64)
                            } else {
                                // Per-key consecutive quota failures: back off
                                // exponentially from the poll retry base
                                // (2s, 4s, 8s, ...), capped at the full
                                // all-keys-exhausted wait.
                                let exp = 1u64 << quota_failures.saturating_sub(1).min(31);
                                let backoff_ms = (tuning.poll_retry_delay_ms as u64)
                                    .saturating_mul(exp)
                                    .min(tuning.quota_backoff_secs as u64 * 1000);
                                std::time::Duration::from_millis(backoff_ms)
                            };
                            tokio::time::sleep(delay).await;
                        } else {
                            // Invalid/disabled/blocked key (not quota): rotate and
                            // retry shortly without counting toward the quota backoff.
                            quota_failures = 0;
                            warn!("YouTube API key is invalid or disabled while polling live chat — rotating API key...");
                            keys.rotate_to_next();
                            tokio::time::sleep(std::time::Duration::from_millis(
                                tuning.poll_retry_delay_ms as u64,
                            ))
                            .await;
                        }
                        continue;
                    }
                    // A non-key-error response means at least one key is healthy.
                    quota_failures = 0;
                    // A poll succeeded again after an exhausted episode: clear
                    // the quota banner.
                    if notice_exhausted {
                        notice_exhausted = false;
                        send_status_notice(write_ws, identity, "youtube", "info", "", 0).await;
                    }

                    if status == reqwest::StatusCode::NOT_FOUND {
                        info!("Live chat closed (Not found).");
                        break;
                    }

                    if status == reqwest::StatusCode::FORBIDDEN {
                        let is_ended = json
                            .get("error")
                            .and_then(|e| e.get("errors"))
                            .and_then(|e| e.as_array())
                            .map_or(false, |errors| {
                                errors.iter().any(|err| {
                                    let reason =
                                        err.get("reason").and_then(|r| r.as_str()).unwrap_or("");
                                    reason == "liveChatEnded" || reason == "liveChatDisabled"
                                })
                            });

                        if is_ended {
                            info!("Live chat closed (Stream ended).");
                            break;
                        } else {
                            tokio::time::sleep(tokio::time::Duration::from_secs(
                                tuning.chat_poll_retry_secs as u64,
                            ))
                            .await;
                            continue;
                        }
                    }

                    // The API's suggested interval (or the config fallback) is
                    // read as u64, clamped, then jittered. Clamp BEFORE anything
                    // narrows it: a hostile value must not wrap through u32.
                    let requested_interval_ms = json
                        .get("pollingIntervalMillis")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(tuning.polling_interval_ms as u64);
                    let interval = jittered_poll_interval_ms(requested_interval_ms);
                    if !interval_logged {
                        info!(
                            "Live-chat effective poll interval: {} ms (requested {} ms)",
                            interval, requested_interval_ms
                        );
                        interval_logged = true;
                    }

                    if let Some(new_token) = json.get("nextPageToken").and_then(|v| v.as_str()) {
                        next_page_token = Some(new_token.to_string());
                    }

                    // Re-poll of a page we already forwarded (no per-message ids
                    // to dedup against): skip it — nothing new can be on it.
                    let already_processed =
                        requested_token.is_some() && requested_token == last_processed_token;

                    if let Some(items) = json.get("items").and_then(|i| i.as_array()) {
                        for item in items {
                            // Skip messages already ingested (a timed-out poll
                            // retried with the same page token).
                            if let Some(id) = item.get("id").and_then(|i| i.as_str()) {
                                if seen_ids.is_seen(id) {
                                    continue;
                                }
                            } else if already_processed {
                                // No message ids available — fall back to the
                                // page-level guard: this exact page was already
                                // successfully forwarded, so skip its items.
                                continue;
                            }

                            let author = item
                                .get("authorDetails")
                                .and_then(|a| a.get("displayName"))
                                .and_then(|d| d.as_str())
                                .unwrap_or("Unknown");

                            let snippet = item.get("snippet");
                            let msg = snippet
                                .and_then(|s| {
                                    s.get("displayMessage").or_else(|| {
                                        s.get("textMessageDetails")
                                            .and_then(|t| t.get("messageText"))
                                    })
                                })
                                .and_then(|m| m.as_str())
                                .unwrap_or("");

                            // Donations (Super Chat / membership / gift) carry
                            // structured `raw_data`; ordinary chat keeps the
                            // raw item JSON. A donation with an empty
                            // `displayMessage` must still be forwarded.
                            let donation = snippet.and_then(donation_from_rest_snippet);

                            if !msg.is_empty() || donation.is_some() {
                                let (raw_message, raw_data) = match &donation {
                                    Some(d) => (d.raw_message(), d.raw_data_json()),
                                    None => (msg.to_string(), item.to_string().as_bytes().to_vec()),
                                };
                                info!("[YouTube Chat] {}: {}", author, raw_message);
                                let pre_process = MessagePreProcess {
                                    audio: vec![],
                                    audio_type: String::new(),
                                    message_uuid7: String::new(),
                                    raw_message: Some(ChatMessage {
                                        platform: "youtube".into(),
                                        raw_data,
                                        raw_message,
                                        user_uuid7: author.to_string(),
                                        command: None,
                                        channel_id: live_chat
                                            .lock()
                                            .unwrap()
                                            .clone()
                                            .unwrap_or_default(),
                                        user_data: None,
                                    }),
                                };
                                let (auth, module, instance) = {
                                    let id = identity.lock().await;
                                    (id.auth.clone(), id.module.clone(), id.instance.clone())
                                };
                                let container = ContainerForEngine {
                                    version: 2,
                                    auth_token: auth,
                                    module_name: module,
                                    module_instance_uuid7: instance,
                                    payload: Some(EnginePayload::MessagePreProcess(pre_process)),
                                };
                                let mut buf = Vec::new();
                                if container.encode(&mut buf).is_ok() {
                                    let mut w = write_ws.lock().await;
                                    if let Err(e) = w.send(WsMessage::Binary(buf.into())).await {
                                        error!("Failed to send to engine: {}", e);
                                    }
                                }
                            }
                        }
                    }

                    // This page (identified by its requested token) was
                    // successfully processed — a later retry of the same token
                    // (e.g. after a timeout) must not re-forward it.
                    last_processed_token = requested_token;

                    tokio::time::sleep(tokio::time::Duration::from_millis(interval)).await;
                } else {
                    if status == reqwest::StatusCode::FORBIDDEN
                        || status == reqwest::StatusCode::NOT_FOUND
                    {
                        info!("Live chat closed (Stream ended / Unparseable response).");
                        break;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_secs(
                        tuning.chat_poll_retry_secs as u64,
                    ))
                    .await;
                }
            }
            Err(e) => {
                error!("Error polling chat: {}. Retrying...", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    tuning.chat_poll_retry_secs as u64,
                ))
                .await;
            }
        }
    }
}

/// The delay before the next no-stream re-scan: the slow passive cadence when
/// passive mode is on (`passive_scan_interval_secs`), otherwise the regular
/// `stream_scan_interval_secs`.
fn no_stream_scan_interval_secs(tuning: &Tuning) -> u32 {
    if tuning.passive {
        tuning.passive_scan_interval_secs
    } else {
        tuning.stream_scan_interval_secs
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    info!("Starting YouTube Adapter Module...");

    // Secrets (`YOUTUBE_API_KEY`, OAuth client id/secret, refresh token) live in
    // `.env`. Load it BEFORE anything reads credentials: `load_adapter_config`
    // and the OAuth manager both read process env vars, so loading it later left
    // them empty (sending disabled, browser flow never started).
    ensure_env_file();
    cockatiel_client::load_env_file(".env");

    let cockatiel = CockatielClient::connect("config.json").await?;

    let (write_ws_cockatiel, read_ws_cockatiel) = cockatiel.stream.split();
    let write_ws_cockatiel = Arc::new(tokio::sync::Mutex::new(write_ws_cockatiel));
    // Shared session identity: the read loop AND the platform send path read
    // the CURRENT token/instance here, so a reconnect (which swaps this) never
    // leaves stale credentials behind.
    let identity: Arc<tokio::sync::Mutex<EngineIdentity>> = Arc::new(tokio::sync::Mutex::new(
        EngineIdentity {
            auth: cockatiel.auth_token.clone(),
            instance: cockatiel.instance_uuid7.clone(),
            module: cockatiel.config.module_name.clone(),
        },
    ));

    // Register the mod commands with the engine command system: the engine now
    // parses `!ban` / `!timeout` and routes them back with the parsed Command.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    register_commands(&write_ws_cockatiel, &identity).await;

    // Initial identity, used by the (one-time) setup phase prompts. Runtime
    // sends read the CURRENT identity from the shared handle instead.
    let (auth_token, instance_uuid, module_name) = {
        let id = identity.lock().await;
        (id.auth.clone(), id.instance.clone(), id.module.clone())
    };

    // Adapter tuning knobs from `config.json` (`module_specific`). Every key
    // falls back to its hardcoded default when missing, so an unconfigured
    // module behaves exactly as it did with the literals.
    let saved_cfg = load_adapter_config().unwrap_or_default();
    let tuning = Tuning::from(&saved_cfg);

    // HTTP client (HTTP/1.1: reqwest's default HTTP/2 negotiation against
    // googleapis is flaky and produced "connection closed before message
    // completed" errors). A default timeout on EVERY request keeps a wedged
    // TLS connection from stalling the discovery/chat-poll ingestion loop for
    // minutes — the send path keeps its own explicit timeouts on top.
    let client = reqwest::Client::builder()
        .http1_only()
        .timeout(Duration::from_secs(tuning.http_timeout_secs as u64))
        .build()
        .unwrap_or_else(|_| {
            // Last-resort fallback: still bound every request so a wedged
            // connection can never hang the ingestion loop indefinitely.
            reqwest::Client::builder()
                .timeout(Duration::from_secs(tuning.http_timeout_secs as u64))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
        });

    // OAuth for sending: load app creds + any saved refresh token.
    let oauth_client_id = saved_cfg.google_oauth_client_id.clone().unwrap_or_default();
    let oauth_client_secret = saved_cfg.google_oauth_client_secret.clone().unwrap_or_default();
    let oauth_refresh = saved_cfg.refresh_token.clone().unwrap_or_default();
    let oauth_redirect_port = load_oauth_redirect_port();
    let oauth = OAuthManager::new(
        &oauth_client_id,
        &oauth_client_secret,
        if oauth_refresh.is_empty() {
            None
        } else {
            Some(oauth_refresh.clone())
        },
        oauth_redirect_port,
        tuning.token_expiry_margin_secs,
        tuning.token_ttl_fallback_secs,
    );

    // The live chat id currently being monitored (shared with the send task).
    let live_chat: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    // NOTE: the OAuth browser capture (which can block for up to
    // `oauth_capture_timeout_secs`) runs BELOW, after the engine read loop has
    // been spawned. Doing it here starved the engine's liveness probe for the
    // whole capture window and got the connection severed.

    // Bounded SendToPlatforms queue: the read loop enqueues (never blocking, so
    // an AuthVerify reply is never delayed by a slow `ensure_access_token`
    // refresh + liveChat POST) and a single worker task posts to the live chat.
    // Capacity — a send flood is dropped with a warning rather than spawning
    // unbounded tasks.
    let (send_tx, mut send_rx) = mpsc::channel::<String>(tuning.outbound_queue_cap);
    let oauth_worker = oauth.clone();
    let client_worker = client.clone();
    let live_chat_worker = Arc::clone(&live_chat);
    tokio::spawn(async move {
        while let Some(msg) = send_rx.recv().await {
            match send_to_youtube(&oauth_worker, &client_worker, &live_chat_worker, &msg).await {
                Ok(()) => info!("Sent to YouTube live chat: {}", msg),
                Err(e) => error!("SendToPlatforms failed: {}", e),
            }
        }
    });
    let send_tx_task = send_tx;

    // Stream-manager wiring: the video the adapter is currently targeting (set
    // by the discovery loop below), and a channel the read loop forwards
    // `stream_schedule` poll results into. The stream-control task (spawned
    // after the read loop) awaits them, applies the control via the Data API and
    // reports the result back.
    let current_video: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let (schedule_tx, mut schedule_rx) = mpsc::channel::<serde_json::Value>(8);

    // Channel carrying PromptResponses from the engine to the stream loop,
    // so `prompt_for_input` can await the operator's typed answer.
    let (prompt_tx, mut prompt_rx) = mpsc::unbounded_channel::<PromptResponse>();
    let prompt_tx_task = prompt_tx.clone();
    let write_task = write_ws_cockatiel.clone();
    let identity_task = identity.clone();
    let tuning_task = tuning.clone();
    tokio::spawn(async move {
        // Read-loop + engine-session supervisor. When the socket drops the
        // module RECONNECTS instead of going zombie on a dead socket (the old
        // behavior: the platform loop kept pushing into a dead WS forever).
        let mut read = read_ws_cockatiel;
        loop {
            // Read until the connection dies.
            while let Some(msg) = read.next().await {
            match msg {
                Ok(WsMessage::Binary(data)) => {
                    if let Ok(container) = ContainerForModule::decode(data.as_ref()) {
                        // Use the CURRENT session identity (a reconnect swaps it).
                        let (auth, instance, module) = {
                            let id = identity_task.lock().await;
                            (id.auth.clone(), id.instance.clone(), id.module.clone())
                        };

                        // Answer the engine's liveness probe with our auth token
                        // so a quiet period never severs us.
                        if let Some(ModulePayload::AuthVerify(_)) = container.payload {
                            let reply = ContainerForEngine {
                                version: 2,
                                auth_token: auth.clone(),
                                module_name: module.clone(),
                                module_instance_uuid7: instance.clone(),
                                payload: Some(EnginePayload::AuthVerify(AuthVerify {
                                    cur_auth: auth.clone(),
                                })),
                            };
                            let mut buf = Vec::new();
                            if reply.encode(&mut buf).is_ok() {
                                let mut w = write_task.lock().await;
                                let _ = w.send(WsMessage::Binary(buf.into())).await;
                            }
                        }
                        // SendToPlatforms handling: post to the active live chat. Offloaded to
                        // the bounded worker task so the read loop returns
                        // immediately — an inline `ensure_access_token` (token
                        // refresh) + liveChat POST here would delay the
                        // AuthVerify reply past the engine's 15s liveness probe
                        // and get the module killed. `try_send` never blocks:
                        // when the worker is saturated the message is dropped
                        // (best-effort) instead of a flood spawning unbounded
                        // tasks.
                        else if let Some(ModulePayload::SendToPlatforms(send)) = container.payload {
                            if send_tx_task.try_send(send.msg.clone()).is_err() {
                                warn!("SendToPlatforms queue full; dropping message: {}", send.msg);
                            }
                        } else if let Some(ModulePayload::PromptResponse(resp)) = container.payload {
                            // Forward operator answers to the awaiting prompt.
                            let _ = prompt_tx_task.send(resp);
                        }
                        // Routed chat command: the engine parsed `!ban` / `!timeout`
                        // and delivered it here with the parsed Command attached.
                        else if let Some(ModulePayload::MessagePreProcess(pre)) = container.payload {
                            // Receipt ping: confirm delivery to the engine
                            // IMMEDIATELY (pure ack, separate from the stage
                            // echo below) so the engine doesn't resend.
                            if !pre.message_uuid7.is_empty() {
                                let receipt = ContainerForEngine {
                                    version: 2,
                                    auth_token: auth.clone(),
                                    module_name: module.clone(),
                                    module_instance_uuid7: instance.clone(),
                                    payload: Some(EnginePayload::MessageAck(MessageAck {
                                        message_uuid7: pre.message_uuid7.clone(),
                                    })),
                                };
                                let mut rbuf = Vec::new();
                                if receipt.encode(&mut rbuf).is_ok() {
                                    let mut w = write_task.lock().await;
                                    let _ = w.send(WsMessage::Binary(rbuf)).await;
                                }
                            }
                            let Some(chat) = pre.raw_message else { continue };
                            let Some(cmd) = chat.command.clone() else { continue };
                            if cmd.command_name != "ban" && cmd.command_name != "timeout" {
                                continue;
                            }
                            let author = chat
                                .user_data
                                .as_ref()
                                .map(|u| u.username.clone())
                                .unwrap_or_default();
                            if let Some((qid, payload)) = build_mod_query(
                                &cmd.command_name,
                                &chat.raw_message,
                                &author,
                                tuning_task.default_timeout_secs,
                            ) {
                                let query = ContainerForEngine {
                                    version: 2,
                                    auth_token: auth.clone(),
                                    module_name: module.clone(),
                                    module_instance_uuid7: instance.clone(),
                                    payload: Some(EnginePayload::DatabaseQuery(DatabaseQuery {
                                        query_id: qid,
                                        sql: payload.to_string(),
                                        params: vec![],
                                    })),
                                };
                                let mut qbuf = Vec::new();
                                if query.encode(&mut qbuf).is_ok() {
                                    let mut w = write_task.lock().await;
                                    let _ = w.send(WsMessage::Binary(qbuf.into())).await;
                                }
                            }
                            // Stage result: echo the MessagePreProcess back with the
                            // SAME message_uuid7 so the pipeline advances — the
                            // MessageAck above is a pure receipt, NOT a result. Sent
                            // on EVERY routed-command path (query built or not).
                            // Never with an empty uuid7: the engine treats that as a
                            // NEW message ingest.
                            if !pre.message_uuid7.is_empty() {
                                let result = ContainerForEngine {
                                    version: 2,
                                    auth_token: auth,
                                    module_name: module,
                                    module_instance_uuid7: instance,
                                    payload: Some(EnginePayload::MessagePreProcess(MessagePreProcess {
                                        message_uuid7: pre.message_uuid7,
                                        raw_message: Some(chat),
                                        audio: pre.audio,
                                        audio_type: pre.audio_type,
                                    })),
                                };
                                let mut rbuf = Vec::new();
                                if result.encode(&mut rbuf).is_ok() {
                                    let mut w = write_task.lock().await;
                                    let _ = w.send(WsMessage::Binary(rbuf)).await;
                                }
                            }
                        } else if let Some(ModulePayload::DatabaseQueryResult(res)) = container.payload {
                            // A `stream_schedule` poll result: forward it to the
                            // stream-control task, which owns the apply logic.
                            if res.query_id == "stream_schedule" {
                                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&res.result_blob) {
                                    let _ = schedule_tx.try_send(v);
                                }
                            }
                        } else if let Some(ModulePayload::Shutdown(_)) = container.payload {
                            info!("Engine requested shutdown — exiting");
                            std::process::exit(0);
                        }
                    }
                }
                Ok(WsMessage::Close(_)) => {
                    info!("Engine closed connection");
                    break;
                }
                Err(e) => {
                    error!("Engine WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
            }

            // The engine connection dropped — reconnect with backoff instead of
            // leaving the platform loop pushing into a dead socket.
            info!("Engine disconnected — reconnecting...");
            let mut backoff = tuning_task.reconnect_base_secs;
            loop {
                tokio::time::sleep(Duration::from_secs(backoff as u64)).await;
                match CockatielClient::connect("config.json").await {
                    Ok(conn) => {
                        info!("Reconnected to engine");
                        let (w, r) = conn.stream.split();
                        *write_task.lock().await = w;
                        *identity_task.lock().await = EngineIdentity {
                            auth: conn.auth_token,
                            instance: conn.instance_uuid7,
                            module: conn.config.module_name,
                        };
                        // The engine forgets a session's commands when the
                        // socket drops — re-register on the fresh session.
                        register_commands(&write_task, &identity_task).await;
                        read = r;
                        break;
                    }
                    Err(e) => {
                        error!("Engine reconnect failed: {} — retrying in {}s", e, backoff);
                        backoff = (backoff * 2).min(tuning_task.reconnect_max_secs);
                    }
                }
            }
        }
    });

    // Now that the read loop is answering the engine's liveness probe, run the
    // (potentially blocking) OAuth browser capture. If we have OAuth app
    // credentials but no refresh token yet, acquire one via the browser flow
    // (bounded so it can never hang forever).
    if oauth.has_creds() && oauth_refresh.is_empty() {
        info!("No YouTube refresh token yet — opening browser to authorize sending...");
        match tokio::time::timeout(
            std::time::Duration::from_secs(tuning.oauth_capture_timeout_secs as u64),
            oauth.acquire_refresh_token(&client),
        )
        .await
        {
            Ok(Ok(refresh)) => {
                info!("Acquired YouTube refresh token (sending enabled).");
                save_oauth_refresh_token(&refresh);
            }
            Ok(Err(e)) => warn!("Could not acquire YouTube OAuth token ({}); sending disabled.", e),
            Err(_) => warn!("YouTube OAuth capture timed out; sending disabled."),
        }
    } else if oauth.has_creds() {
        info!("YouTube OAuth refresh token present (sending enabled).");
    } else if oauth_refresh.is_empty() {
        warn!("YouTube sending disabled: set Google OAuth client id/secret (or a refresh token) in the credential form.");
    }

    // ── Stream-control poll task ─────────────────────────────────────────
    // Every `stream_control_poll_secs`, read the engine's desired control
    // (`stream_schedule`) and, when the revision changed, apply it to the
    // current video via the Data API and report the result back
    // (`stream_control_status`). The read loop forwards the schedule results on
    // `schedule_rx`; this task owns the apply logic so a slow API call never
    // delays the read loop's AuthVerify replies.
    {
        let oauth = oauth.clone();
        let client = client.clone();
        let write = write_ws_cockatiel.clone();
        let identity = identity.clone();
        let current_video = Arc::clone(&current_video);
        let poll_secs = tuning.stream_control_poll_secs.max(5);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(poll_secs as u64));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut last_applied = 0u64;
            loop {
                tick.tick().await;
                let (auth, instance, module) = {
                    let id = identity.lock().await;
                    (id.auth.clone(), id.instance.clone(), id.module.clone())
                };
                // Ask the engine for the current schedule.
                let query = ContainerForEngine {
                    version: 2,
                    auth_token: auth.clone(),
                    module_name: module.clone(),
                    module_instance_uuid7: instance.clone(),
                    payload: Some(EnginePayload::DatabaseQuery(DatabaseQuery {
                        query_id: "stream_schedule".to_string(),
                        sql: "{}".to_string(),
                        params: vec![],
                    })),
                };
                let mut qbuf = Vec::new();
                if query.encode(&mut qbuf).is_err() {
                    continue;
                }
                {
                    let mut w = write.lock().await;
                    if w.send(WsMessage::Binary(qbuf.into())).await.is_err() {
                        continue;
                    }
                }
                // The read loop forwards the answer here.
                let control = match tokio::time::timeout(Duration::from_secs(10), schedule_rx.recv()).await {
                    Ok(Some(v)) => v,
                    _ => continue,
                };
                let revision = control.get("revision").and_then(|v| v.as_u64()).unwrap_or(0);
                if revision == 0 || revision == last_applied {
                    continue;
                }
                let video_id = current_video.lock().unwrap().clone().unwrap_or_default();
                let (ok, message) = match apply_youtube_stream_control(&oauth, &client, &video_id, &control).await {
                    Ok(m) => (true, m),
                    Err(e) => (false, e),
                };
                last_applied = revision;
                info!("[stream-control] rev {} applied={} — {}", revision, ok, message);
                let status = json!({
                    "platform": "youtube",
                    "revision": revision,
                    "ok": ok,
                    "message": message,
                });
                let report = ContainerForEngine {
                    version: 2,
                    auth_token: auth,
                    module_name: module,
                    module_instance_uuid7: instance,
                    payload: Some(EnginePayload::DatabaseQuery(DatabaseQuery {
                        query_id: "stream_control_status".to_string(),
                        sql: status.to_string(),
                        params: vec![],
                    })),
                };
                let mut rbuf = Vec::new();
                if report.encode(&mut rbuf).is_ok() {
                    let mut w = write.lock().await;
                    let _ = w.send(WsMessage::Binary(rbuf.into())).await;
                }
            }
        });
    }

    // ── Linear setup phase ───────────────────────────────────────────────
    // Resolve auth + API keys, validate the target (channel/video) with an
    // explicit operator choice on failure, then surface a setup summary.
    // (`.env` was already loaded at the top of `main`.)
    let mut setup_log = String::new();
    let mut channel_input = std::env::var("YOUTUBE_CHANNEL_ID").unwrap_or_default();
    let mut api_keys: Vec<String> = Vec::new();
    let mut unlisted_ids: Vec<String> = Vec::new();

    if let Ok(env_key) = std::env::var("YOUTUBE_API_KEY") {
        // load_env_file joins list values with "\n"; accept either a single
        // key or several.
        for part in env_key.split('\n') {
            let trimmed = part.trim().to_string();
            if !trimmed.is_empty() {
                api_keys.push(trimmed);
            }
        }
    }

if channel_input.is_empty() || api_keys.is_empty() {
    // Non-interactive fast path: if a complete saved config exists, use it
    // without prompting (enables the TUI to supply credentials via file).
    if let Some(saved) = load_adapter_config() {
        if let Some(saved_chan) = saved.channel_id {
            let mut saved_keys = Vec::new();
            if let Some(k_list) = saved.api_keys {
                for k in k_list {
                    let trimmed = k.trim().to_string();
                    if !trimmed.is_empty() && !saved_keys.contains(&trimmed) {
                        saved_keys.push(trimmed);
                    }
                }
            }
            if let Some(single) = saved.api_key {
                let trimmed = single.trim().to_string();
                if !trimmed.is_empty() && !saved_keys.contains(&trimmed) {
                    saved_keys.push(trimmed);
                }
            }

            if !saved_chan.is_empty() && !saved_keys.is_empty() && channel_input.is_empty() && api_keys.is_empty() {
                info!(
                    "Using complete saved YouTube configuration for channel '{}' (no prompt).",
                    saved_chan
                );
                channel_input = saved_chan;
                api_keys = saved_keys;
                if let Some(saved_unlisted) = saved.unlisted_video_ids {
                    unlisted_ids = saved_unlisted;
                }
            } else if !saved_keys.is_empty() {
                let confirm = prompt_for_input(
                    &write_ws_cockatiel,
                    &mut prompt_rx,
                    &auth_token,
                    &module_name,
                    &instance_uuid,
                    "Use Saved YouTube Configuration?",
                    &format!(
                        "A saved configuration was found:\n\n\
                         Channel: {}\n\
                         API key(s): {}\n\n\
                         Use this configuration?",
                        saved_chan,
                        saved_keys.len()
                    ),
                    // Empty input_label + boolean kind → true y/n prompt (y accepts, n/Esc
                    // cancels). The caller treats Some(..) as "yes".
                    "",
                    PromptKind::Boolean,
                    tuning.prompt_timeout_secs,
                )
                .await;
                if let Some(choice) = confirm {
                    if choice.trim().eq_ignore_ascii_case("y") || choice.trim().eq_ignore_ascii_case("yes") {
                        channel_input = saved_chan;
                        api_keys = saved_keys;
                        if let Some(saved_unlisted) = saved.unlisted_video_ids {
                            unlisted_ids = saved_unlisted;
                        }
                    }
                }
            }
        }
    }
}

if channel_input.is_empty() {
    let input = prompt_for_input(
        &write_ws_cockatiel,
        &mut prompt_rx,
        &auth_token,
        &module_name,
        &instance_uuid,
        "YouTube Channel Configuration",
        "Enter your YouTube Channel Handle (e.g. @vulbyte), Channel ID (UC...),\n\
         or a direct Video ID / Stream URL (for unlisted streams).",
        "Channel Handle, ID, or Video URL",
        PromptKind::String,
        tuning.prompt_timeout_secs,
    )
    .await;
    if let Some(val) = input {
        channel_input = val.trim().to_string();
    }
}

if api_keys.is_empty() {
    let input = prompt_for_input(
        &write_ws_cockatiel,
        &mut prompt_rx,
        &auth_token,
        &module_name,
        &instance_uuid,
        "YouTube API Key Required",
        "Enter a YouTube Data API Key for stream discovery and chat monitoring.\n\n\
         To get a key:\n\
         1. Go to console.cloud.google.com > APIs & Services > Credentials\n\
         2. Create an API key\n\
         3. Enable the YouTube Data API v3\n\n\
         You can add additional keys later for quota rotation by editing\n\
         config.json. A valid Data API key is required for both stream\n\
         discovery and chat monitoring; without it the module can only use\n\
         already-saved config or a direct video id.",
        "YouTube Data API Key",
        PromptKind::Credential,
        tuning.prompt_timeout_secs,
    )
    .await;
    if let Some(key) = input {
        let trimmed = key.trim().to_string();
        if !trimmed.is_empty() {
            api_keys.push(trimmed);
            save_adapter_config(&channel_input, &api_keys, &unlisted_ids, "", "", "");
        }
    }
    println!();
}

let mut key_manager = ApiKeyManager::new(api_keys.clone());
info!(
    "Initialized YouTube API Key Manager with {} key(s) for automatic rotation",
    key_manager.key_count()
);

// ── Validate the API keys early ──────────────────────────────────────
// Prompt only when NO configured key works. A ring whose current key is
// quota-exhausted (but another is fine) must NOT be reported as invalid — the
// adapter rotates during polling anyway, so re-prompting for a key that already
// exists just confuses the operator. Prompt only when the whole ring is dead
// (e.g. the placeholder "testkey") so they never have to edit the config by hand.
// A transport/parse failure is `Unknown`, not `Invalid`: never prompt for a new
// key just because the network was briefly down — retry during polling instead.
let key_status = any_api_key_valid(&client, &key_manager).await;
if key_status == KeyValidity::Unknown {
    warn!(
        "Could not validate YouTube API key(s) due to a network/parse error — treating \
         them as unknown and retrying during polling (not prompting for a new key)."
    );
}
if key_status == KeyValidity::Invalid {
    warn!(
        "No usable YouTube API key among {} configured. Prompting for a valid key...",
        key_manager.key_count()
    );
    let new_key = prompt_for_input(
        &write_ws_cockatiel,
        &mut prompt_rx,
        &auth_token,
        &module_name,
        &instance_uuid,
        "YouTube API Key Required",
        "Your YouTube Data API key is invalid or missing.\n\n\
         A valid Data API key is required for both stream discovery and\n\
         chat monitoring; without it the module can only use already-saved\n\
         config or a direct video id.\n\n\
         To get a key:\n\
         1. Go to console.cloud.google.com > APIs & Services > Credentials\n\
         2. Create an API key\n\
         3. Enable the YouTube Data API v3\n\n\
         Enter your API key below, or press Cancel to continue without a\n\
         key (already-saved config / direct video id only).",
        "YouTube Data API Key",
        PromptKind::Credential,
        tuning.prompt_timeout_secs,
    )
    .await;

    if let Some(key) = new_key {
        let trimmed = key.trim().to_string();
        if !trimmed.is_empty() {
            let offending = key_manager.current_key();
            api_keys = replace_current_key(&api_keys, &offending, &trimmed);
            key_manager = ApiKeyManager::new(api_keys.clone());
            save_adapter_config(&channel_input, &api_keys, &unlisted_ids, "", "", "");
            info!("YouTube API key updated and saved to config.json");
            // Re-validate immediately so we catch typos before entering the
            // discovery loop.
            match is_api_key_valid(&client, &key_manager).await {
                KeyValidity::Valid => {}
                KeyValidity::Invalid => warn!(
                    "The key you entered still fails validation. \
                     You can re-enter it by restarting the module."
                ),
                KeyValidity::Unknown => warn!(
                    "Could not validate the key you entered due to a network/parse \
                     error; it may still be valid — retrying during polling."
                ),
            }
        }
    } else {
        info!("No API key provided — continuing without a key (already-saved config / direct video id only).");
    }
}

// ── Target (channel/video) validation with an explicit operator choice ──
// The old code cleared creds + `continue 'configure` on a resolution
// failure, which could re-prompt forever. Instead, resolve/validate the
// target once and — when it can't be resolved — ask the operator to
// (t)ry, (i)gnore, (r)emove, or (e)dit it. Every path then breaks into the
// receive-only discovery loop (never an automatic re-prompt spin).
enum TargetOutcome {
    Valid(String),
    Invalid(String),
    CannotVerify,
}

let channel_id: String = loop {
    let maybe_video_id = extract_video_id(&channel_input);

    let outcome = if let Some(ref vid) = maybe_video_id {
        info!("Targeting direct video stream ID: {}", vid);
        match fetch_video_stream(&client, vid, &key_manager).await {
            Ok(Some(_)) => {
                if !unlisted_ids.contains(vid) {
                    unlisted_ids.push(vid.clone());
                }
                TargetOutcome::Valid("".to_string())
            }
            Ok(None) => TargetOutcome::Invalid(format!(
                "target '{}' could not be resolved as a YouTube channel or live stream.",
                channel_input
            )),
            Err(e) => {
                warn!("Could not verify target '{}': {}.", channel_input, e);
                if !unlisted_ids.contains(vid) {
                    unlisted_ids.push(vid.clone());
                }
                TargetOutcome::CannotVerify
            }
        }
    } else {
        match get_channel_id(&client, &channel_input, &key_manager).await {
            Ok(cid) => TargetOutcome::Valid(cid),
            Err(e) => {
                error!("Failed to resolve YouTube channel '{}': {}.", channel_input, e);
                TargetOutcome::Invalid(format!(
                    "target '{}' could not be resolved as a YouTube channel or live stream.",
                    channel_input
                ))
            }
        }
    };

    match outcome {
        TargetOutcome::Valid(cid) => {
            if cid != channel_input && cid.starts_with("UC") {
                save_adapter_config(&cid, &api_keys, &unlisted_ids, "", "", "");
            }
            setup_log.push_str(&format!("target '{}' is valid\n", channel_input));
            break cid;
        }
        TargetOutcome::CannotVerify => {
            setup_log.push_str(&format!(
                "target '{}' could not be verified — will retry at runtime\n",
                channel_input
            ));
            break String::new();
        }
        TargetOutcome::Invalid(subject) => {
            let choice = prompt_tie_choice(
                &write_ws_cockatiel,
                &mut prompt_rx,
                &auth_token,
                &module_name,
                &instance_uuid,
                &subject,
                tuning.prompt_timeout_secs,
            )
            .await;
            match choice {
                Some(TieChoice::Retry) => {
                    // Re-validate the same target.
                    continue;
                }
                Some(TieChoice::Ignore) => {
                    setup_log.push_str(&format!(
                        "target '{}' invalid — ignored (will retry at runtime)\n",
                        channel_input
                    ));
                    break String::new();
                }
                Some(TieChoice::Remove) => {
                    setup_log.push_str(&format!("target '{}' invalid — removed\n", channel_input));
                    let fresh = prompt_for_input(
                        &write_ws_cockatiel,
                        &mut prompt_rx,
                        &auth_token,
                        &module_name,
                        &instance_uuid,
                        "YouTube Channel Configuration",
                        "Enter your YouTube Channel Handle (e.g. @vulbyte), Channel ID (UC...),\n\
                         or a direct Video ID / Stream URL (for unlisted streams).",
                        "Channel Handle, ID, or Video URL",
                        PromptKind::String,
                        tuning.prompt_timeout_secs,
                    )
                    .await;
                    if let Some(val) = fresh {
                        let t = val.trim().to_string();
                        if !t.is_empty() {
                            channel_input = t;
                            continue;
                        }
                    }
                    break String::new();
                }
                Some(TieChoice::Edit) => {
                    let corrected = prompt_for_input(
                        &write_ws_cockatiel,
                        &mut prompt_rx,
                        &auth_token,
                        &module_name,
                        &instance_uuid,
                        "Edit YouTube Target",
                        "Enter the corrected YouTube channel handle, Channel ID, or Video ID / URL.",
                        "Channel Handle, ID, or Video URL",
                        PromptKind::String,
                        tuning.prompt_timeout_secs,
                    )
                    .await;
                    if let Some(val) = corrected {
                        let t = val.trim().to_string();
                        if !t.is_empty() {
                            channel_input = t;
                            continue;
                        }
                    }
                    break String::new();
                }
                None => {
                    setup_log.push_str(&format!(
                        "target '{}' invalid — skipped (cancelled)\n",
                        channel_input
                    ));
                    break String::new();
                }
            }
        }
    }
};

// Surface the setup summary to the operator (the accumulated log).
if !setup_log.trim().is_empty() {
    let log = ContainerForEngine {
        version: 2,
        auth_token: auth_token.clone(),
        module_name: module_name.clone(),
        module_instance_uuid7: instance_uuid.clone(),
        payload: Some(EnginePayload::Log(cockatiel_client::proto::Log {
            log: format!("[youtube-adapter] setup:\n{}", setup_log.trim_end()),
            blob: vec![],
        })),
    };
    let mut lbuf = Vec::new();
    if log.encode(&mut lbuf).is_ok() {
        let mut w = write_ws_cockatiel.lock().await;
        let _ = w.send(WsMessage::Binary(lbuf.into())).await;
    }
}

// Initialize gRPC transport layer connection. The channel is currently
// unused (discarded), so this is best-effort and MUST NOT block chat
// discovery — a gRPC failure just logs a warning and we continue with the
// REST API polling.
match tonic::transport::Channel::from_static("https://youtube.googleapis.com")
.tls_config(ClientTlsConfig::new())
{
Ok(ch) => {
    if let Err(e) = ch.connect().await {
        warn!(
            "YouTube gRPC transport unavailable ({}); continuing with REST API",
            e
        );
    } else {
        info!("Successfully connected to YouTube gRPC transport layer!");
    }
}
Err(e) => {
    warn!(
        "YouTube gRPC transport config failed ({}); continuing with REST API",
        e
    );
}
}

// Seamless loop: when a stream ends or disconnects, it automatically loops back to discover new streams
loop {
    let streams = if !channel_id.is_empty() {
        info!(
            "Scanning for active and scheduled streams for channel ID: {}",
            channel_id
        );
        match fetch_streams(&client, &channel_id, &key_manager, &unlisted_ids).await {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to fetch streams: {}. Retrying in {} seconds...", e, tuning.stream_fetch_retry_secs);
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    tuning.stream_fetch_retry_secs as u64,
                ))
                .await;
                continue;
            }
        }
    } else {
        let mut s = Vec::new();
        for vid in &unlisted_ids {
            if let Ok(Some(info)) = fetch_video_stream(&client, vid, &key_manager).await {
                s.push(info);
            }
        }
        s
    };

    let chosen_stream = match select_stream(
        &streams,
        &client,
        &key_manager,
        &mut unlisted_ids,
        &channel_id,
        &write_ws_cockatiel,
        &mut prompt_rx,
        &auth_token,
        &module_name,
        &instance_uuid,
        tuning.prompt_timeout_secs,
    )
    .await
    {
        Some(s) => s,
        None => {
            // Passive mode slows the off-air re-scan, so an idle channel stops
            // polling the API every `stream_scan_interval_secs`; otherwise use
            // the regular scan interval.
            let interval = no_stream_scan_interval_secs(&tuning);
            if tuning.passive {
                info!("No streams currently found. Passive mode — re-scanning in {interval} seconds...");
            } else {
                info!("No streams currently found. Re-scanning in {interval} seconds...");
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(interval as u64)).await;
            continue;
        }
    };

    info!(
        "Selected stream: {} ({})",
        chosen_stream.title, chosen_stream.video_id
    );

    // Publish the target video so the stream-control task applies the operator's
    // title / start / thumbnail to it while it is selected (live or upcoming).
    *current_video.lock().unwrap() = Some(chosen_stream.video_id.clone());

    // Monitor stream live chat until the stream ends
    monitor_stream_chat(
        &write_ws_cockatiel,
        &identity,
        &client,
        &chosen_stream.video_id,
        &key_manager,
        &live_chat,
        &tuning,
        &mut prompt_rx,
        &auth_token,
        &module_name,
        &instance_uuid,
    )
    .await;

    *current_video.lock().unwrap() = None;
    info!("Stream finished. Restarting discovery loop for seamless transition...");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tie_choice() {
        assert_eq!(parse_tie_choice("t"), Some(TieChoice::Retry));
        assert_eq!(parse_tie_choice("try"), Some(TieChoice::Retry));
        assert_eq!(parse_tie_choice("retry"), Some(TieChoice::Retry));
        assert_eq!(parse_tie_choice("try again"), Some(TieChoice::Retry));
        assert_eq!(parse_tie_choice("i"), Some(TieChoice::Ignore));
        assert_eq!(parse_tie_choice("ignore"), Some(TieChoice::Ignore));
        assert_eq!(parse_tie_choice("r"), Some(TieChoice::Remove));
        assert_eq!(parse_tie_choice("remove"), Some(TieChoice::Remove));
        assert_eq!(parse_tie_choice("e"), Some(TieChoice::Edit));
        assert_eq!(parse_tie_choice("EDIT"), Some(TieChoice::Edit));
        assert_eq!(parse_tie_choice("x"), None);
        assert_eq!(parse_tie_choice(""), None);
    }

    #[test]
    fn test_stream_start_message_format() {
        assert_eq!(
            stream_start_message("abc123", "2026-09-24T18:00:00Z", "My Stream"),
            "[stream-start] youtube: video 'abc123' went live at 2026-09-24T18:00:00Z — title: \"My Stream\""
        );
        assert_eq!(
            stream_start_message("abc123", "unknown", "My Stream"),
            "[stream-start] youtube: video 'abc123' went live at unknown — title: \"My Stream\""
        );
    }

    #[test]
    fn test_stream_start_fires_once() {
        let fired = AtomicBool::new(false);
        assert!(claim_stream_start(&fired));
        assert!(!claim_stream_start(&fired));
        assert!(!claim_stream_start(&fired));
    }

    #[test]
    fn test_stream_sort_is_chronological() {
        let mk = |id: &str, status: &str, start: &str| StreamInfo {
            video_id: id.into(),
            title: format!("Stream {id}"),
            status: status.into(),
            published_at: start.into(),
            scheduled_start: start.into(),
        };
        let streams = vec![
            mk("far", "upcoming", "2026-10-05T18:00:00Z"),
            mk("live1", "live", "2026-10-03T10:00:00Z"),
            mk("soon", "upcoming", "2026-10-03T18:00:00Z"),
            mk("mid", "upcoming", "2026-10-04T12:00:00Z"),
        ];
        let sorted = sort_streams_chronologically(&streams);
        let order: Vec<&str> = sorted.iter().map(|s| s.video_id.as_str()).collect();
        // Live first, then upcoming soonest → furthest.
        assert_eq!(order, vec!["live1", "soon", "mid", "far"], "chronological order");
    }

    #[test]
    fn test_stream_sort_uses_scheduled_start_not_publish_date() {
        // The enrichment step (fetch_streams step 5) fills in scheduled_start
        // from the Data API's scheduledStartTime. Without it the sort would use
        // the publish date — which for a stale test stream published earlier
        // can be older than the REAL soonest stream. This is the ordering that
        // matters tonight: the soonest-scheduled stream must come first.
        let mk = |id: &str, published: &str, start: &str| StreamInfo {
            video_id: id.into(),
            title: format!("Stream {id}"),
            status: "upcoming".into(),
            published_at: published.into(),
            scheduled_start: start.into(),
        };
        // "test stream" was published 2026-09-29 but schedules in 2028; PROJECT
        // PITT was published later (2026-10-03) and starts tonight at 22:30Z.
        let streams = vec![
            mk("test-stream", "2026-09-29T06:40:00Z", "2028-09-29T06:40:00Z"),
            mk("project-pitt", "2026-10-03T17:49:28Z", "2026-10-03T22:30:00Z"),
        ];
        let sorted = sort_streams_chronologically(&streams);
        assert_eq!(
            sorted[0].video_id, "project-pitt",
            "the stream starting soonest must sort first, not the one published earlier"
        );
        assert_eq!(sorted[1].video_id, "test-stream");
    }

    #[test]
    fn test_stream_sort_falls_back_to_publish_date() {
        let mk = |id: &str, start: &str| StreamInfo {
            video_id: id.into(),
            title: format!("Stream {id}"),
            status: "upcoming".into(),
            published_at: start.into(),
            scheduled_start: String::new(),
        };
        let streams = vec![mk("later", "2026-10-06T00:00:00Z"), mk("earlier", "2026-10-02T00:00:00Z")];
        let sorted = sort_streams_chronologically(&streams);
        assert_eq!(sorted[0].video_id, "earlier", "falls back to publish date");
        assert_eq!(sorted[1].video_id, "later");
    }

    #[test]
    fn test_format_utc_iso() {
        assert_eq!(format_utc_iso(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_utc_iso(86_400), "1970-01-02T00:00:00Z");
        assert_eq!(format_utc_iso(1_704_067_200), "2024-01-01T00:00:00Z");
    }

    #[test]
    fn test_merge_video_snippet_preserves_required_fields() {
        let snippet = json!({
            "title": "Old",
            "categoryId": "20",
            "description": "keep me",
            "scheduledStartTime": "2026-01-01T00:00:00Z",
        });
        let merged = merge_video_snippet(&snippet, "New Title", 0);
        assert_eq!(merged["title"], "New Title");
        // `categoryId` is REQUIRED by videos.update and must survive.
        assert_eq!(merged["categoryId"], "20");
        assert_eq!(merged["description"], "keep me");
        // scheduled_start_ms == 0 leaves the existing schedule untouched.
        assert_eq!(merged["scheduledStartTime"], "2026-01-01T00:00:00Z");

        // A non-zero start overwrites scheduledStartTime (RFC3339, UTC).
        let merged = merge_video_snippet(&snippet, "", 1_704_067_200_000);
        assert_eq!(merged["title"], "Old", "empty title leaves it unchanged");
        assert_eq!(merged["scheduledStartTime"], "2024-01-01T00:00:00Z");
    }

    #[test]
    fn test_thumbnail_mime() {
        assert_eq!(thumbnail_mime("/x/y.PNG"), "image/png");
        assert_eq!(thumbnail_mime("/x/y.png"), "image/png");
        assert_eq!(thumbnail_mime("/x/y.jpg"), "image/jpeg");
        assert_eq!(thumbnail_mime("/x/y.jpeg"), "image/jpeg");
        assert_eq!(thumbnail_mime("/x/y"), "image/jpeg");
    }

    #[test]
    fn test_routed_command_maps_to_mod_query() {
        let (qid, p) = build_mod_query("ban", "!ban @user being awful", "mod", 300).unwrap();
        assert_eq!(qid, "mod_ban");
        assert_eq!(p["handle"], "user");
        assert_eq!(p["reason"], "being awful");
        assert_eq!(p["actor"]["handle"], "mod");

        let (qid, p) = build_mod_query("timeout", "!timeout @user 300 spam", "mod", 300).unwrap();
        assert_eq!(qid, "mod_timeout");
        assert_eq!(p["duration_secs"], 300);
        assert_eq!(p["reason"], "spam");

        // No explicit duration → the configured default (300s) is used.
        let (qid, p) = build_mod_query("timeout", "!timeout @user spam", "mod", 300).unwrap();
        assert_eq!(qid, "mod_timeout");
        assert_eq!(p["duration_secs"], 300);

        assert!(build_mod_query("!help", "!help", "mod", 300).is_none());
    }

    #[test]
    fn test_extract_video_id() {
        assert_eq!(
            extract_video_id("1a2iSWQzl7I"),
            Some("1a2iSWQzl7I".to_string())
        );
        assert_eq!(
            extract_video_id("https://www.youtube.com/watch?v=1a2iSWQzl7I"),
            Some("1a2iSWQzl7I".to_string())
        );
        assert_eq!(
            extract_video_id("https://www.youtube.com/watch?v=1a2iSWQzl7I&t=10s"),
            Some("1a2iSWQzl7I".to_string())
        );
        assert_eq!(
            extract_video_id("https://youtu.be/1a2iSWQzl7I"),
            Some("1a2iSWQzl7I".to_string())
        );
        assert_eq!(
            extract_video_id("https://www.youtube.com/live/1a2iSWQzl7I"),
            Some("1a2iSWQzl7I".to_string())
        );
        assert_eq!(
            extract_video_id("https://www.youtube.com/shorts/1a2iSWQzl7I"),
            Some("1a2iSWQzl7I".to_string())
        );
        assert_eq!(extract_video_id("@vulbyte"), None);
        assert_eq!(extract_video_id("vulbyte"), None);
        assert_eq!(extract_video_id("UCKZigHbgpJG9ldxXMqmiZUg"), None);
    }

    #[test]
    fn test_api_key_manager_rotation() {
        let keys = vec![
            "KEY_A".to_string(),
            "KEY_B".to_string(),
            "KEY_C".to_string(),
        ];
        let mgr = ApiKeyManager::new(keys);
        assert_eq!(mgr.key_count(), 3);
        assert_eq!(mgr.current_key(), "KEY_A");

        assert_eq!(mgr.rotate_to_next(), "KEY_B");
        assert_eq!(mgr.current_key(), "KEY_B");

        assert_eq!(mgr.rotate_to_next(), "KEY_C");
        assert_eq!(mgr.current_key(), "KEY_C");

        // Wraps around to first key
        assert_eq!(mgr.rotate_to_next(), "KEY_A");
        assert_eq!(mgr.current_key(), "KEY_A");
    }

    #[test]
    fn test_is_quota_error() {
        let quota_err = json!({
            "error": {
                "code": 403,
                "errors": [
                    {
                        "domain": "youtube.quota",
                        "message": "The request cannot be completed because you have exceeded your quota.",
                        "reason": "quotaExceeded"
                    }
                ],
                "message": "The request cannot be completed because you have exceeded your quota."
            }
        });
        assert!(ApiKeyManager::is_quota_error(&quota_err));

        let other_err = json!({
            "error": {
                "code": 404,
                "message": "Not found"
            }
        });
        assert!(!ApiKeyManager::is_quota_error(&other_err));
    }

    #[test]
    fn test_replace_current_key_preserves_ring() {
        let existing = vec!["BAD".to_string(), "B".to_string(), "C".to_string()];
        assert_eq!(
            replace_current_key(&existing, "BAD", "NEW"),
            vec!["NEW", "B", "C"]
        );
        // Empty replacement is a no-op (keeps the whole ring untouched).
        assert_eq!(replace_current_key(&existing, "BAD", "   "), existing);
        // A replacement already present elsewhere is de-duplicated.
        assert_eq!(replace_current_key(&existing, "BAD", "B"), vec!["B", "C"]);
    }

    #[test]
    fn test_is_key_error() {
        let key_err = |reason: &str| {
            json!({
                "error": {
                    "code": 400,
                    "errors": [{ "reason": reason, "message": "x" }],
                    "message": "x"
                }
            })
        };
        assert!(ApiKeyManager::is_key_error(&key_err("keyInvalid")));
        assert!(ApiKeyManager::is_key_error(&key_err("API_KEY_INVALID")));
        assert!(ApiKeyManager::is_key_error(&key_err("accessNotConfigured")));
        assert!(ApiKeyManager::is_key_error(&key_err("ipRefererBlocked")));
        assert!(ApiKeyManager::is_key_error(&key_err("quotaExceeded")));
        // A generic 403 `forbidden` is NOT a key error: it must fall through to
        // the dedicated FORBIDDEN handling so liveChatEnded/Disabled are seen.
        assert!(!ApiKeyManager::is_key_error(&json!({
            "error": { "errors": [{ "reason": "forbidden" }] }
        })));
        assert!(!ApiKeyManager::is_key_error(&key_err("forbidden")));
        assert!(!ApiKeyManager::is_key_error(&key_err("permission_denied")));
        // Non-key 403 reasons must not trigger a pointless rotation.
        assert!(!ApiKeyManager::is_key_error(&key_err("liveChatEnded")));
        assert!(!ApiKeyManager::is_key_error(&key_err("liveChatDisabled")));
        assert!(!ApiKeyManager::is_key_error(&key_err("liveChatNotFound")));
    }

    #[test]
    fn test_rotated_refresh_token() {
        let resp = json!({ "access_token": "at", "refresh_token": "NEW_REFRESH" });
        assert_eq!(
            rotated_refresh_token(&resp, "OLD"),
            Some("NEW_REFRESH".to_string())
        );
        // Same as current, absent, or blank => no rotation.
        assert_eq!(rotated_refresh_token(&resp, "NEW_REFRESH"), None);
        assert_eq!(
            rotated_refresh_token(&json!({ "access_token": "at" }), "OLD"),
            None
        );
        assert_eq!(
            rotated_refresh_token(&json!({ "refresh_token": "   " }), "OLD"),
            None
        );
    }

    #[test]
    fn test_access_token_ttl_secs() {
        // Normal Google expiry: buffer 60s (the default margin).
        assert_eq!(access_token_ttl_secs(3600, 60, 3600), 3540);
        assert_eq!(access_token_ttl_secs(60, 60, 3600), 0);
        // expires_in < margin must saturate, not underflow into ~5.8e11.
        assert_eq!(access_token_ttl_secs(30, 60, 3600), 0);
        assert_eq!(access_token_ttl_secs(1, 60, 3600), 0);
        // Missing/zero expires_in falls back to the configured default TTL.
        assert_eq!(access_token_ttl_secs(0, 60, 3600), 3600);
        assert_eq!(access_token_ttl_secs(-5, 60, 3600), 3600);
        // Custom margin/fallback values are honored.
        assert_eq!(access_token_ttl_secs(100, 30, 900), 70);
        assert_eq!(access_token_ttl_secs(0, 30, 900), 900);
    }

    #[test]
    fn test_config_tuning_defaults() {
        // A module_specific section WITHOUT tuning keys must fall back to the
        // hardcoded defaults for every knob.
        let cfg: YoutubeAdapterConfig =
            serde_json::from_value(json!({ "channel_id": "UCx" })).unwrap();
        assert_eq!(cfg.default_timeout_secs, 300);
        assert_eq!(cfg.http_timeout_secs, 15);
        assert_eq!(cfg.token_expiry_margin_secs, 60);
        assert_eq!(cfg.token_ttl_fallback_secs, 3600);
        assert_eq!(cfg.chat_open_max_attempts, 60);
        assert_eq!(cfg.chat_open_backoff_first_secs, 10);
        assert_eq!(cfg.chat_open_backoff_later_secs, 30);
        assert_eq!(cfg.dedup_cap, 10_000);
        assert_eq!(cfg.quota_backoff_secs, 60);
        assert_eq!(cfg.poll_retry_delay_ms, 2000);
        assert_eq!(cfg.polling_interval_ms, 5000);
        assert_eq!(cfg.chat_poll_retry_secs, 5);
        assert_eq!(cfg.oauth_capture_timeout_secs, 120);
        assert_eq!(cfg.outbound_queue_cap, 64);
        assert_eq!(cfg.reconnect_base_secs, 1);
        assert_eq!(cfg.reconnect_max_secs, 30);
        assert_eq!(cfg.stream_fetch_retry_secs, 15);
        assert_eq!(cfg.stream_scan_interval_secs, 30);
        assert_eq!(cfg.prompt_timeout_secs, 300);
        // Explicit keys override the defaults.
        let cfg: YoutubeAdapterConfig =
            serde_json::from_value(json!({ "http_timeout_secs": 7, "prompt_timeout_secs": 90 }))
                .unwrap();
        assert_eq!(cfg.http_timeout_secs, 7);
        assert_eq!(cfg.prompt_timeout_secs, 90);
        assert_eq!(cfg.default_timeout_secs, 300);
    }

    #[test]
    fn test_jittered_poll_interval_clamps_and_stays_in_bounds() {
        // Absurd API values must clamp into [1s, 60s] and never escape, however
        // large the requested value (regression: a u64 truncated to u32 first).
        for requested in [0u64, 1, 999, 1_000, 5_000, 60_000, 60_001, 600_000, u64::MAX] {
            for _ in 0..64 {
                let out = jittered_poll_interval_ms(requested);
                assert!(
                    (POLL_INTERVAL_MIN_MS..=POLL_INTERVAL_MAX_MS).contains(&out),
                    "requested {requested} -> {out} escaped bounds"
                );
            }
        }
        // An in-range value jitters by at most ±10%.
        for _ in 0..64 {
            let out = jittered_poll_interval_ms(5_000);
            assert!(
                (4_500..=5_500).contains(&out),
                "5000 -> {out} exceeded ±10%"
            );
        }
    }

    #[test]
    fn test_seen_message_ids_dedup_and_prune() {
        let mut seen = SeenMessageIds::new(3);
        assert!(!seen.is_seen("m1"));
        assert!(!seen.is_seen("m2"));
        assert!(!seen.is_seen("m3"));
        // Duplicates (a timed-out poll retried with the same page token).
        assert!(seen.is_seen("m1"));
        assert!(seen.is_seen("m2"));
        // New message still passes.
        assert!(!seen.is_seen("m4"));
        // The set pruned the oldest entry — m1 is no longer remembered, so a
        // re-poll after a full 10k-rotate would re-ingest it. Verify the bound
        // holds and the newest ids survive.
        assert!(!seen.is_seen("m1"));
        assert!(seen.is_seen("m4"));
        assert!(seen.is_seen("m3"));
    }

    #[test]
    fn passive_mode_slows_the_no_stream_rescan() {
        let mut t = Tuning::default();
        assert!(!t.passive, "passive is off by default");
        assert_eq!(t.passive_scan_interval_secs, 300, "passive cadence is 5 minutes");
        // Default: the regular scan interval drives the off-air re-scan.
        assert_eq!(no_stream_scan_interval_secs(&t), t.stream_scan_interval_secs);
        assert_eq!(no_stream_scan_interval_secs(&t), 30);
        // Passive: the slow cadence.
        t.passive = true;
        assert_eq!(no_stream_scan_interval_secs(&t), t.passive_scan_interval_secs);
        assert_eq!(no_stream_scan_interval_secs(&t), 300);
        // The serde config carries the same defaults.
        let c = YoutubeAdapterConfig::default();
        assert!(!c.passive);
        assert_eq!(c.passive_scan_interval_secs, 300);
    }

    #[test]
    fn test_backfill_tuning_defaults_creates_and_preserves() {
        let dir = std::env::temp_dir().join(format!("yt_backfill_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.json");

        // Missing file: backfill creates it with every tuning default.
        backfill_tuning_defaults_at(&path);
        let root: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        let ms = root["module_specific"].as_object().unwrap();
        assert_eq!(ms["default_timeout_secs"], 300);
        assert_eq!(ms["http_timeout_secs"], 15);
        assert_eq!(ms["dedup_cap"], 10_000);
        assert_eq!(ms["prompt_timeout_secs"], 300);
        assert_eq!(ms.len(), 21, "all tuning defaults present");

        // Existing keys are preserved, missing ones are added, top-level fields survive.
        std::fs::write(
            &path,
            r#"{"ip":"127.0.0.1","module_specific":{"default_timeout_secs":900}}"#,
        )
        .unwrap();
        backfill_tuning_defaults_at(&path);
        let root: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(root["ip"], "127.0.0.1", "top-level field preserved");
        let ms = root["module_specific"].as_object().unwrap();
        assert_eq!(ms["default_timeout_secs"], 900, "existing value preserved");
        assert_eq!(ms["http_timeout_secs"], 15, "missing key backfilled");
        assert_eq!(ms.len(), 21);

        // The OLD poll-retry default (500) migrates to the new 2000 base...
        std::fs::write(
            &path,
            r#"{"module_specific":{"poll_retry_delay_ms":500}}"#,
        )
        .unwrap();
        backfill_tuning_defaults_at(&path);
        let root: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(root["module_specific"]["poll_retry_delay_ms"], 2000);

        // ...but an explicit non-default value is left untouched.
        std::fs::write(
            &path,
            r#"{"module_specific":{"poll_retry_delay_ms":750}}"#,
        )
        .unwrap();
        backfill_tuning_defaults_at(&path);
        let root: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(root["module_specific"]["poll_retry_delay_ms"], 750);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ensure_env_file_creates_and_merges_placeholders() {
        let dir = std::env::temp_dir().join(format!("yt_env_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(".env");
        let _ = std::fs::remove_file(&path);

        // Missing file: created with every expected key as an EMPTY placeholder.
        ensure_env_file_at(&path);
        let created = std::fs::read_to_string(&path).unwrap();
        for key in ENV_KEYS {
            assert!(created.lines().any(|l| l == format!("{}=", key)), "{} missing", key);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, ".env must be owner-only");
        }

        // Existing non-empty values preserved; missing keys appended once.
        let first = format!("{}=", ENV_KEYS[0]);
        std::fs::write(&path, format!("{}{}\nSOME_OTHER=keep\n", first, "abc")).unwrap();
        ensure_env_file_at(&path);
        let merged = std::fs::read_to_string(&path).unwrap();
        assert!(merged.contains(&format!("{}{}", first, "abc")), "existing value overwritten");
        assert!(merged.contains("SOME_OTHER=keep"), "unrelated key dropped");
        assert_eq!(merged.matches(&first).count(), 1, "existing key line duplicated");
        for key in ENV_KEYS {
            assert!(
                merged.lines().any(|l| l.starts_with(&format!("{}=", key))),
                "{} missing after merge",
                key
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn donation_json(snippet: &serde_json::Value) -> serde_json::Value {
        let d = donation_from_rest_snippet(snippet).expect("expected a donation");
        serde_json::from_slice(&d.raw_data_json()).expect("raw_data must be valid UTF-8 JSON")
    }

    #[test]
    fn test_rest_superchat_donation_raw_data() {
        let snippet = json!({
            "type": "superChatEvent",
            "superChatDetails": {
                "amountMicros": "5000000",
                "currency": "USD",
                "amountDisplayString": "$5.00",
                "userComment": "great stream",
                "tier": 2
            }
        });
        let d = donation_from_rest_snippet(&snippet).expect("donation");
        assert_eq!(d.kind, "superchat");
        assert_eq!(d.amount_micros, 5_000_000);
        assert_eq!(d.currency, "USD");
        assert_eq!(d.display, "$5.00");
        assert_eq!(d.message, "great stream");
        assert_eq!(d.raw_message(), "great stream");
        assert_eq!(
            donation_json(&snippet),
            json!({"donation": {
                "kind": "superchat",
                "amount_micros": 5_000_000,
                "currency": "USD",
                "display": "$5.00",
                "message": "great stream"
            }})
        );
    }

    #[test]
    fn test_rest_supersticker_and_empty_comment_not_dropped() {
        // A Super Chat with no comment must still forward (display line).
        let snippet = json!({
            "type": "superChatEvent",
            "superChatDetails": {
                "amountMicros": "1000000",
                "currency": "EUR",
                "amountDisplayString": "€1.00",
                "userComment": ""
            }
        });
        let d = donation_from_rest_snippet(&snippet).expect("donation");
        assert_eq!(d.raw_message(), "€1.00");
        assert_eq!(d.amount_micros, 1_000_000);

        let sticker = json!({
            "type": "superStickerEvent",
            "superStickerDetails": {
                "amountMicros": "2000000",
                "currency": "JPY",
                "amountDisplayString": "¥300"
            }
        });
        assert_eq!(donation_json(&sticker)["donation"]["kind"], "supersticker");
        assert_eq!(
            donation_json(&sticker)["donation"]["amount_micros"],
            2_000_000
        );
    }

    #[test]
    fn test_rest_membership_donation_raw_data() {
        let milestone = json!({
            "type": "memberMilestoneChatEvent",
            "memberMilestoneChatDetails": {
                "memberLevelName": "Tier 2",
                "memberMonth": 6,
                "userComment": "six months!"
            }
        });
        let d = donation_from_rest_snippet(&milestone).expect("donation");
        assert_eq!(d.kind, "membership");
        assert_eq!(d.amount_micros, 0);
        assert_eq!(d.display, "Tier 2 (6 months)");
        assert_eq!(d.message, "six months!");
        assert_eq!(d.raw_message(), "six months!");
        assert_eq!(
            donation_json(&milestone),
            json!({"donation": {
                "kind": "membership",
                "amount_micros": 0,
                "currency": "",
                "display": "Tier 2 (6 months)",
                "message": "six months!"
            }})
        );

        let new_sponsor = json!({
            "type": "newSponsorEvent",
            "newSponsorDetails": { "memberLevelName": "Member" }
        });
        let d = donation_from_rest_snippet(&new_sponsor).expect("donation");
        assert_eq!(d.kind, "membership");
        assert_eq!(d.display, "Member");
        assert_eq!(d.raw_message(), "Member");
    }

    #[test]
    fn test_rest_gift_donation_raw_data() {
        let gifting = json!({
            "type": "membershipGiftingEvent",
            "membershipGiftingDetails": {
                "giftMembershipsCount": 5,
                "giftMembershipsLevelName": "Tier 1"
            }
        });
        let d = donation_from_rest_snippet(&gifting).expect("donation");
        assert_eq!(d.kind, "gift");
        assert_eq!(d.display, "Gifted 5 x Tier 1");
        assert_eq!(
            donation_json(&gifting),
            json!({"donation": {
                "kind": "gift",
                "amount_micros": 0,
                "currency": "",
                "display": "Gifted 5 x Tier 1",
                "message": ""
            }})
        );

        let jewels = json!({
            "type": "giftEvent",
            "giftDetails": {
                "giftName": "Gold coin",
                "jewelsAmount": 10,
                "altText": "Gold coin",
                "comboCount": 1
            }
        });
        assert_eq!(donation_json(&jewels)["donation"]["kind"], "gift");
        assert_eq!(donation_json(&jewels)["donation"]["display"], "Gold coin");
    }

    #[test]
    fn test_plain_chat_has_no_donation_raw_data() {
        // A normal text message has a `type` but no donation fields.
        let text = json!({
            "type": "textMessageEvent",
            "displayMessage": "hello world",
            "textMessageDetails": { "messageText": "hello world" }
        });
        assert!(donation_from_rest_snippet(&text).is_none());

        // A snippet with no `type` at all is never a donation.
        assert!(donation_from_rest_snippet(&json!({"displayMessage": "hi"})).is_none());
    }
}
