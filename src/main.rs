use cockatiel_client::{proto::container::Payload, proto::*, CockatielClient, PromptKind};
use futures_util::{SinkExt, StreamExt as _};
use prost::Message as ProstMessage;
use serde::{Deserialize, Serialize};
use serde_json::json;


use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
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

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
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
}

#[derive(Debug, Clone)]
struct StreamInfo {
    video_id: String,
    title: String,
    status: String, // "live" or "upcoming"
    published_at: String,
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
}

/// Tests whether the current YouTube API key is valid by making a lightweight Search API request.
async fn is_api_key_valid(client: &reqwest::Client, keys: &ApiKeyManager) -> bool {
    let api_key = keys.current_key();
    if api_key.is_empty() {
        return false;
    }
    let url = format!(
        "https://www.googleapis.com/youtube/v3/search?part=snippet&q=cockatiel&type=video&maxResults=1&key={}",
        api_key
    );
    match client.get(&url).send().await {
        Ok(res) => match res.json::<serde_json::Value>().await {
            Ok(json) => !json.get("error").is_some(),
            Err(_) => false,
        },
        Err(_) => false,
    }
}

/// Parse a moderator command (!ban / !timeout) from a chat message.
fn parse_mod_command(message: &str, author: &str) -> Option<(String, serde_json::Value)> {
    let trimmed = message.trim();
    let lower = trimmed.to_lowercase();

    if lower.starts_with("!ban") {
        let args = trimmed[5..].trim();
        let (target, rest) = match args.split_once(char::is_whitespace) {
            Some((t, r)) => (t, r),
            None => (args, ""),
        };
        let target = target.trim_start_matches('@').to_string();
        if target.is_empty() {
            return None;
        }
        return Some((
            "mod_ban".to_string(),
            serde_json::json!({
                "platform": "youtube",
                "handle": target,
                "reason": rest.trim().to_string(),
                "actor": { "platform": "youtube", "handle": author },
            }),
        ));
    }

    if lower.starts_with("!timeout") {
        let args = trimmed[9..].trim();
        let mut parts = args.split_whitespace();
        let target = parts.next().unwrap_or("").trim_start_matches('@').to_string();
        if target.is_empty() {
            return None;
        }
        let mut duration_secs = 300i64;
        let mut reason = String::new();
        if let Some(d) = parts.next() {
            if let Ok(secs) = d.parse::<i64>() {
                duration_secs = secs;
            } else {
                reason = d.to_string();
            }
        }
        let rest: Vec<&str> = parts.collect();
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

    None
}

fn load_adapter_config() -> Option<YoutubeAdapterConfig> {
    // Secrets live in `.env` (loaded into env at startup); channel/settings
    // come from config.json.
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
    let mut spec = json!({
        "channel_id": channel_id,
        "api_key": api_keys.first().cloned().unwrap_or_default(),
        "api_keys": api_keys,
    });
    if !unlisted_ids.is_empty() {
        spec["unlisted_video_ids"] = json!(unlisted_ids);
    }
    json_val["module_specific"] = spec;
    if let Ok(pretty) = serde_json::to_string_pretty(&json_val) {
        let _ = std::fs::write(&path, pretty);
        info!("Successfully saved YouTube configuration (secrets → .env)");
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

/// Manages a Google OAuth2 token for `youtube.force-ssl`: acquires a refresh
/// token via the browser flow (or accepts one pasted into the config), then
/// mints/refreshes short-lived access tokens on demand.
#[derive(Clone)]
struct OAuthManager {
    client_id: String,
    client_secret: String,
    oauth_redirect_port: u16,
    refresh_token: Arc<Mutex<Option<String>>>,
    access_token: Arc<Mutex<Option<(String, Instant)>>>,
}

impl OAuthManager {
    fn new(
        client_id: &str,
        client_secret: &str,
        refresh_token: Option<String>,
        oauth_redirect_port: u16,
    ) -> Self {
        Self {
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            oauth_redirect_port,
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
        let expires_in = resp.get("expires_in").and_then(|v| v.as_i64()).unwrap_or(3600);

        self.set_refresh_token(&refresh);
        if !access.is_empty() {
            *self.access_token.lock().unwrap() = Some((access.to_string(), Instant::now() + std::time::Duration::from_secs(expires_in as u64 - 60)));
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
            .send()
            .await
            .map_err(|e| format!("token refresh failed: {}", e))?
            .json()
            .await
            .map_err(|e| format!("token refresh parse failed: {}", e))?;

        let access = resp
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("no access_token in refresh response: {:?}", resp))?
            .to_string();
        let expires_in = resp.get("expires_in").and_then(|v| v.as_i64()).unwrap_or(3600);
        *self.access_token.lock().unwrap() = Some((
            access.clone(),
            Instant::now() + std::time::Duration::from_secs(expires_in as u64 - 60),
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

        if ApiKeyManager::is_quota_error(&json) {
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
                    });
                }
            }
        }
        break;
    }

    // Keyless fallback: scrape the watch page. Works without a valid Data API
    // key and for Unlisted streams (Private/Draft remain unavailable even here).
    if found.is_none() {
        warn!(
            "Data API lookup for {} failed (invalid/quota key?) — trying keyless watch-page scrape.",
            video_id
        );
        found = fetch_video_from_watch_page(client, video_id).await?;
    }

    Ok(found)
}

/// Keyless fallback: parse the watch page's `ytInitialPlayerResponse` for the
/// video's title + live/upcoming status. No Data API key required.
async fn fetch_video_from_watch_page(
    client: &reqwest::Client,
    video_id: &str,
) -> Result<Option<StreamInfo>, Box<dyn std::error::Error>> {
    let url = format!("https://www.youtube.com/watch?v={}", video_id);
    let res = client
        .get(&url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        .send()
        .await?;
    let html = res.text().await?;

    let marker = "var ytInitialPlayerResponse = ";
    let Some(start) = html.find(marker) else {
        info!(
            "Watch page for {} has no ytInitialPlayerResponse (private/unavailable?).",
            video_id
        );
        return Ok(None);
    };
    // Parse just the first JSON value after the marker; the page appends more
    // JS (`;var meta = ...`), so a plain from_str would fail on the trailing data.
    let json_str = &html[start + marker.len()..];
    let mut de = serde_json::Deserializer::from_str(json_str);
    let data: serde_json::Value = match serde_json::Value::deserialize(&mut de) {
        Ok(d) => d,
        Err(e) => {
            warn!("ytInitialPlayerResponse parse failed for {}: {}", video_id, e);
            return Ok(None);
        }
    };

    if let Some(status) = data
        .get("playabilityStatus")
        .and_then(|p| p.get("status"))
        .and_then(|s| s.as_str())
    {
        if status != "OK" {
            info!(
                "Video {} is not playable (status={}); not a public/unlisted stream.",
                video_id, status
            );
            return Ok(None);
        }
    }

    let Some(vd) = data.get("videoDetails") else {
        return Ok(None);
    };
    let title = vd
        .get("title")
        .and_then(|t| t.as_str())
        .unwrap_or("Untitled Stream")
        .to_string();
    let is_live = vd.get("isLive").and_then(|v| v.as_bool()).unwrap_or(false);
    let has_live_details = vd.get("liveBroadcastDetails").is_some();
    let is_upcoming = vd
        .get("isUpcoming")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
        || (has_live_details && !is_live);
    let published_at = vd
        .get("publishDate")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if !is_live && !is_upcoming {
        info!("Video {} is a regular VOD, not a live/upcoming stream.", video_id);
        return Ok(None);
    }

    Ok(Some(StreamInfo {
        video_id: video_id.to_string(),
        title,
        status: if is_live { "live" } else { "upcoming" }.to_string(),
        published_at,
    }))
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
                if ApiKeyManager::is_quota_error(&json) {
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
                if ApiKeyManager::is_quota_error(&json) {
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
                if ApiKeyManager::is_quota_error(&json) {
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

    // 4. Fallback: Fetch public channel page and extract channel ID directly from HTML metadata
    info!(
        "Attempting web fallback to resolve Channel ID for '@{}'...",
        raw_handle
    );
    let page_url = format!("https://www.youtube.com/@{}", raw_handle);
    if let Ok(res) = client
        .get(&page_url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        .send()
        .await
    {
        if let Ok(html) = res.text().await {
            if let Some(pos) = html.find("itemprop=\"identifier\" content=\"UC") {
                let id = &html[pos + 31..pos + 55];
                if id.starts_with("UC") && id.len() == 24 {
                    info!(
                        "Successfully resolved '@{}' to Channel ID {} via channel page metadata!",
                        raw_handle, id
                    );
                    return Ok(id.to_string());
                }
            }
            if let Some(pos) = html.find("\"channelId\":\"UC") {
                let id = &html[pos + 13..pos + 37];
                if id.starts_with("UC") && id.len() == 24 {
                    info!(
                        "Successfully resolved '@{}' to Channel ID {} via channel page data!",
                        raw_handle, id
                    );
                    return Ok(id.to_string());
                }
            }
            if let Some(pos) = html.find("youtube.com/channel/UC") {
                let id = &html[pos + 20..pos + 44];
                if id.starts_with("UC") && id.len() == 24 {
                    info!(
                        "Successfully resolved '@{}' to Channel ID {} via channel link!",
                        raw_handle, id
                    );
                    return Ok(id.to_string());
                }
            }
        }
    }

    Err(format!(
        "Failed to resolve channel handle/ID '{}'. YouTube requires a 24-character Channel ID starting with 'UC'. Please enter your exact Channel ID (from YouTube Studio -> Customization -> Basic Info) or handle.",
        clean_input
    ).into())
}

fn extract_streams_from_json(val: &serde_json::Value, out: &mut Vec<StreamInfo>) {
    match val {
        serde_json::Value::Object(map) => {
            if let Some(content_id) = map.get("contentId").and_then(|c| c.as_str()) {
                if content_id.len() == 11 {
                    let text_dump = serde_json::to_string(val).unwrap_or_default();
                    let is_live = text_dump.contains("\"LIVE\"");
                    let is_upcoming = text_dump.contains("\"Upcoming\"")
                        || text_dump.contains("scheduledStartTime");

                    if is_live || is_upcoming {
                        let title = map
                            .get("rendererContext")
                            .and_then(|r| r.get("accessibilityContext"))
                            .and_then(|a| a.get("label"))
                            .and_then(|l| l.as_str())
                            .or_else(|| {
                                map.get("metadata")
                                    .and_then(|m| m.get("lockupMetadataViewModel"))
                                    .and_then(|l| l.get("title"))
                                    .and_then(|t| t.get("content"))
                                    .and_then(|c| c.as_str())
                            })
                            .unwrap_or("Untitled Stream")
                            .to_string();

                        let status = if is_live { "live" } else { "upcoming" }.to_string();
                        if !out.iter().any(|s| s.video_id == content_id) {
                            out.push(StreamInfo {
                                video_id: content_id.to_string(),
                                title,
                                status,
                                published_at: "".to_string(),
                            });
                        }
                    }
                }
            }

            if let Some(video_id) = map.get("videoId").and_then(|v| v.as_str()) {
                if video_id.len() == 11 {
                    let text_dump = serde_json::to_string(val).unwrap_or_default();
                    let is_live = text_dump.contains("\"LIVE\"")
                        || text_dump.contains("BADGE_STYLE_TYPE_LIVE_NOW");
                    let is_upcoming = text_dump.contains("\"Upcoming\"")
                        || text_dump.contains("UPCOMING")
                        || text_dump.contains("upcomingEventData");

                    if is_live || is_upcoming {
                        let title = map
                            .get("title")
                            .and_then(|t| t.get("runs"))
                            .and_then(|r| r.as_array())
                            .and_then(|arr| arr.first())
                            .and_then(|f| f.get("text"))
                            .and_then(|t| t.as_str())
                            .unwrap_or("Untitled Stream")
                            .to_string();

                        let status = if is_live { "live" } else { "upcoming" }.to_string();
                        if !out.iter().any(|s| s.video_id == video_id) {
                            out.push(StreamInfo {
                                video_id: video_id.to_string(),
                                title,
                                status,
                                published_at: "".to_string(),
                            });
                        }
                    }
                }
            }

            for v in map.values() {
                extract_streams_from_json(v, out);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                extract_streams_from_json(v, out);
            }
        }
        _ => {}
    }
}

/// Fallback to scraping the public channel streams page to find scheduled public streams
/// that have not yet been indexed by YouTube's Search API.
async fn fetch_channel_streams_web(
    client: &reqwest::Client,
    channel_id: &str,
) -> Vec<StreamInfo> {
    let mut streams = Vec::new();
    let url = format!("https://www.youtube.com/channel/{}/streams", channel_id);

    let res = match client
        .get(&url)
        .header(
            "User-Agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        .send()
        .await
    {
        Ok(r) => r,
        Err(_) => return streams,
    };

    let html = match res.text().await {
        Ok(t) => t,
        Err(_) => return streams,
    };

    let marker = "var ytInitialData = ";
    if let Some(start) = html.find(marker) {
        let json_str = &html[start + marker.len()..];
        if let Some(end) = json_str.find(";</script>") {
            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&json_str[..end]) {
                extract_streams_from_json(&data, &mut streams);
            }
        }
    }

    streams
}

/// Keyless fallback: query YouTube's InnerTube browse API for the channel's
/// Streams tab (live + scheduled + unlisted). Uses YouTube's own public web
/// API — NO Data API key required — so it works even when the key is missing,
/// invalid, or quota-exhausted, and it pins the exact Streams tab instead of
/// relying on a scraped URL that may redirect to the channel home page.
async fn fetch_channel_streams_inner(
    client: &reqwest::Client,
    channel_id: &str,
) -> Vec<StreamInfo> {
    let mut streams = Vec::new();
    let url = "https://www.youtube.com/youtubei/v1/browse?prettyPrint=false";
    let ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

    // Channels name their live tab either "Streams" or "Live" — try both params.
    let tab_params = [
        "EgZzdHJlYW1z",            // "Streams" tab
        "EgdzdHJlYW1z8gYECgJ6AA==", // "Live" tab
    ];

    for params in &tab_params {
        let body = serde_json::json!({
            "context": {
                "client": {
                    "clientName": "WEB",
                    "clientVersion": "2.20240718.01.00",
                    "hl": "en",
                    "gl": "US",
                },
            },
            "browseId": channel_id,
            "params": params,
        });

        let res = match client
            .post(url)
            .header("User-Agent", ua)
            .header("Content-Type", "application/json")
            .header("Accept-Language", "en-US,en;q=0.9")
            .body(body.to_string())
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                error!("InnerTube browse request failed: {}", e);
                continue;
            }
        };

        match res.json::<serde_json::Value>().await {
            Ok(json) => {
                let before = streams.len();
                extract_streams_from_json(&json, &mut streams);
                let found = streams.len().saturating_sub(before);
                if found > 0 {
                    info!(
                        "InnerTube discovery for {} found {} stream(s) (params={})",
                        channel_id, found, params
                    );
                }
            }
            Err(e) => error!("InnerTube browse response parse failed: {}", e),
        }
    }

    info!(
        "InnerTube total streams-tab discovery for {} found {} stream(s)",
        channel_id,
        streams.len()
    );
    streams
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
                            if ApiKeyManager::is_quota_error(&json) {
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
                                                        published_at,
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

        // 3. Supplemental web discovery for public scheduled/live streams on the channel page
        let web_streams = fetch_channel_streams_web(client, channel_id).await;
        for ws in web_streams {
            if !all_streams.iter().any(|s: &StreamInfo| s.video_id == ws.video_id) {
                info!(
                    "Discovered stream on channel streams page: {} ({})",
                    ws.title, ws.video_id
                );
                all_streams.push(ws);
            }
        }

        // 4. Keyless InnerTube streams-tab fallback (no API key, pins the exact tab).
        let inner_streams = fetch_channel_streams_inner(client, channel_id).await;
        for ws in inner_streams {
            if !all_streams.iter().any(|s: &StreamInfo| s.video_id == ws.video_id) {
                info!(
                    "Discovered stream on channel streams tab (InnerTube): {} ({})",
                    ws.title, ws.video_id
                );
                all_streams.push(ws);
            }
        }
    }

    Ok(all_streams)
}

/// Send a Prompt to the engine (forwarded to connected UIs) and wait for the
/// operator's response (`PromptResponse.reason`). Returns None on cancel/timeout.
async fn prompt_for_input(
    write_ws: &mut WsWriteHalf,
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
    let container = Container {
        version: 1,
        auth_token: auth_token.to_string(),
        module_name: module_name.to_string(),
        module_instance_uuid7: instance_uuid.to_string(),
        payload: Some(Payload::Prompt(prompt)),
    };
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_err() {
        return None;
    }
    if write_ws.send(WsMessage::Binary(buf.into())).await.is_err() {
        return None;
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
            Ok(None) => return None,
            // The 10s poll interval elapsed with no response yet: keep waiting
            // until the real deadline (the `timeout` seconds above), rather than
            // bailing out 10 seconds in and auto-cancelling every prompt.
            Err(_) => continue,
        }
    }
    None
}

/// Prompts user to select a stream with a 30-second timeout fallback.
/// Also provides guidance and input prompt for unlisted streams.
async fn select_stream(
    streams: &[StreamInfo],
    client: &reqwest::Client,
    keys: &ApiKeyManager,
    unlisted_ids: &mut Vec<String>,
    channel_id: &str,
    write_ws: &mut WsWriteHalf,
    prompt_rx: &mut mpsc::UnboundedReceiver<PromptResponse>,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
) -> Option<StreamInfo> {
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
        println!("    streams are invisible to every public discovery method.");
        println!("  * Unlisted streams CANNOT be found via YouTube Search, but");
        println!("    they DO show on the channel's Streams tab.");
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
            "No live or scheduled stream was found for this channel.\n\nWhy? The stream must be fully scheduled (with a start time) AND Public or Unlisted in YouTube Studio. Draft and Private streams are invisible to every public discovery method (Search API, channel page, InnerTube). Unlisted streams won't appear in Search, but they DO show on the channel's Streams tab.\n\nEnter the Video ID or URL to monitor it directly, or cancel to keep scanning.",
            "Video ID or URL",
            PromptKind::String,
            120,
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
    for (i, stream) in streams.iter().enumerate() {
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
    println!("\n    > Enter a stream number [1-{}].", streams.len());
    println!("    > Or enter 'u' to monitor an Unlisted Stream URL/Video ID.");
    println!("    > If no selection is made, the newest stream will be auto-selected.\n");

    // Ask the operator (via a Prompt) for a selection.
    let mut details = format!("Select a stream to monitor.\n\n");
    for (i, stream) in streams.iter().enumerate() {
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
        120,
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
                120,
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
            if num > 0 && num <= streams.len() {
                return Some(streams[num - 1].clone());
            }
        }
    }

    // Fallback: pick newest live stream, or newest upcoming if none are live
    let mut live: Vec<&StreamInfo> = streams.iter().filter(|s| s.status == "live").collect();
    if !live.is_empty() {
        live.sort_by(|a, b| b.published_at.cmp(&a.published_at));
        println!("    > Auto-selected live stream: {}", live[0].title);
        return Some(live[0].clone());
    }

    let mut upcoming: Vec<&StreamInfo> =
        streams.iter().filter(|s| s.status == "upcoming").collect();
    if !upcoming.is_empty() {
        upcoming.sort_by(|a, b| b.published_at.cmp(&a.published_at));
        println!("    > Auto-selected upcoming stream: {}", upcoming[0].title);
        return Some(upcoming[0].clone());
    }

    Some(streams[0].clone())
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

#[allow(clippy::too_many_arguments)]
async fn monitor_stream_chat(
    write_ws: &mut WsWriteHalf,
    auth_token: &str,
    module_name: &str,
    instance_uuid: &str,
    client: &reqwest::Client,
    video_id: &str,
    keys: &ApiKeyManager,
    live_chat: &Arc<Mutex<Option<String>>>,
) {
    let mut attempt = 0u32;
    let chat_id = 'found_chat: loop {
        attempt += 1;
        if attempt > 60 {
            info!("Live chat failed to open after extended monitoring. Returning to stream discovery...");
            return;
        }

        // 1. Try the Data API first (works with a valid key).
        let mut got_live_chat_id = false;
        let mut stream_ended = false;
        let api_key = keys.current_key();
        let video_url = format!(
            "https://www.googleapis.com/youtube/v3/videos?part=liveStreamingDetails,status&id={}&key={}",
            video_id, api_key
        );

        match client.get(&video_url).send().await {
            Ok(res) => {
                if let Ok(json) = res.json::<serde_json::Value>().await {
                    if !ApiKeyManager::is_quota_error(&json) {
                        if let Some(err) = json.get("error") {
                            // API key invalid or other error — fall through to watch-page check.
                            if attempt == 1 {
                                warn!("Data API unavailable for video status: {:?} — using watch-page fallback.", err.get("message").and_then(|m|m.as_str()).unwrap_or("unknown"));
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
                                        break 'found_chat id.to_string();
                                    }
                                    if details.get("actualEndTime").is_some() {
                                        info!("Stream has concluded.");
                                        return;
                                    }
                                }
                            }
                        }
                    } else if attempt == 1 {
                        warn!("Data API quota exhausted — using watch-page fallback for stream status.");
                    }
                }
            }
            Err(_) => {}
        }

        // 2. Keyless watch-page fallback: check isLive / isUpcoming.
        if let Ok(Some(stream_info)) = fetch_video_from_watch_page(client, video_id).await {
            if stream_info.status == "live" {
                // Stream is live but we can't get the chat ID without the Data API.
                // Log once and keep trying — the chat ID might become available if
                // the user provides a real API key later.
                if attempt % 5 == 1 {
                    warn!(
                        "Stream is LIVE but Data API unavailable to get live chat ID. \
                         Add a real YouTube Data API key to enable chat monitoring."
                    );
                }
            } else {
                // Upcoming — wait and retry.
                if attempt % 6 == 1 {
                    info!(
                        "Stream '{}' is upcoming ({}). Waiting for it to go live...",
                        stream_info.title, stream_info.status
                    );
                }
            }
        }

        // Exponential-ish backoff: 10s for the first few attempts, then 30s.
        let delay = if attempt < 6 { 10 } else { 30 };
        tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;
    };

    info!(
        "Connected to live chat ID: {}. Polling messages...",
        chat_id
    );
    let mut next_page_token: Option<String> = None;

    loop {
        let api_key = keys.current_key();
        let mut chat_url = format!(
            "https://www.googleapis.com/youtube/v3/liveChat/messages?liveChatId={}&part=snippet,authorDetails&key={}",
            chat_id, api_key
        );
        if let Some(ref token) = next_page_token {
            chat_url.push_str(&format!("&pageToken={}", token));
        }

        match client.get(&chat_url).send().await {
            Ok(res) => {
                let status = res.status();

                if let Ok(json) = res.json::<serde_json::Value>().await {
                    if ApiKeyManager::is_quota_error(&json) {
                        warn!("API key quota exceeded while polling live chat messages. Rotating API key...");
                        keys.rotate_to_next();
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                        continue;
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
                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                            continue;
                        }
                    }

                    let interval = json
                        .get("pollingIntervalMillis")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(5000);

                    if let Some(new_token) = json.get("nextPageToken").and_then(|v| v.as_str()) {
                        next_page_token = Some(new_token.to_string());
                    }

                    if let Some(items) = json.get("items").and_then(|i| i.as_array()) {
                        for item in items {
                            let author = item
                                .get("authorDetails")
                                .and_then(|a| a.get("displayName"))
                                .and_then(|d| d.as_str())
                                .unwrap_or("Unknown");

                            let msg = item
                                .get("snippet")
                                .and_then(|s| {
                                    s.get("displayMessage").or_else(|| {
                                        s.get("textMessageDetails")
                                            .and_then(|t| t.get("messageText"))
                                    })
                                })
                                .and_then(|m| m.as_str())
                                .unwrap_or("");

                            if !msg.is_empty() {
                                info!("[YouTube Chat] {}: {}", author, msg);
                                let pre_process = MessagePreProcess {
                        audio: vec![],
                        audio_type: String::new(),
                                    message_uuid7: String::new(),
                                    raw_message: Some(ChatMessage {
                                        platform: "youtube".into(),
                                        raw_data: item.to_string().as_bytes().to_vec(),
                                        raw_message: msg.to_string(),
                                        user_uuid7: author.to_string(),
                                        command: None,
                                        user_data: None,
                                    }),
                                };
                                let container = Container {
                                    version: 1,
                                    auth_token: auth_token.to_string(),
                                    module_name: module_name.to_string(),
                                    module_instance_uuid7: instance_uuid.to_string(),
                                    payload: Some(Payload::MessagePreProcess(pre_process)),
                                };
                                let mut buf = Vec::new();
                                if container.encode(&mut buf).is_ok() {
                                    if let Err(e) = write_ws.send(WsMessage::Binary(buf.into())).await {
                                        error!("Failed to send to engine: {}", e);
                                    }
                                }

                                // Handle moderator commands (!ban / !timeout).
                                if let Some((qid, payload)) = parse_mod_command(msg, author) {
                                    info!("Mod command detected: {} payload={}", qid, payload);
                                    let query = Container {
                                        version: 1,
                                        auth_token: auth_token.to_string(),
                                        module_name: module_name.to_string(),
                                        module_instance_uuid7: instance_uuid.to_string(),
                                        payload: Some(Payload::DatabaseQuery(DatabaseQuery {
                                            query_id: qid,
                                            sql: payload.to_string(),
                                            params: vec![],
                                        })),
                                    };
                                    let mut qbuf = Vec::new();
                                    if query.encode(&mut qbuf).is_ok() {
                                        if let Err(e) = write_ws.send(WsMessage::Binary(qbuf.into())).await {
                                            error!("Failed to send mod command to engine: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    tokio::time::sleep(tokio::time::Duration::from_millis(interval)).await;
                } else {
                    if status == reqwest::StatusCode::FORBIDDEN
                        || status == reqwest::StatusCode::NOT_FOUND
                    {
                        info!("Live chat closed (Stream ended / Unparseable response).");
                        break;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
            Err(e) => {
                error!("Error polling chat: {}. Retrying...", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        }
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

    let cockatiel = CockatielClient::connect("config.json").await?;

    let auth_token = cockatiel.auth_token.clone();
    let instance_uuid = cockatiel.instance_uuid7.clone();
    let module_name = cockatiel.config.module_name.clone();
    let (write_ws_cockatiel, mut read_ws_cockatiel) = cockatiel.stream.split();
    let write_ws_cockatiel = Arc::new(tokio::sync::Mutex::new(write_ws_cockatiel));

    // HTTP client (HTTP/1.1: reqwest's default HTTP/2 negotiation against
    // googleapis is flaky and produced "connection closed before message
    // completed" errors).
    let client = reqwest::Client::builder()
        .http1_only()
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    // OAuth for sending: load app creds + any saved refresh token.
    let saved_cfg = load_adapter_config();
    let oauth_client_id = saved_cfg.as_ref().and_then(|c| c.google_oauth_client_id.clone()).unwrap_or_default();
    let oauth_client_secret = saved_cfg.as_ref().and_then(|c| c.google_oauth_client_secret.clone()).unwrap_or_default();
    let oauth_refresh = saved_cfg.as_ref().and_then(|c| c.refresh_token.clone()).unwrap_or_default();
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
    );

    // The live chat id currently being monitored (shared with the send task).
    let live_chat: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    // If we have OAuth app credentials but no refresh token yet, acquire one
    // now via the browser flow (bounded so it can never hang forever).
    if oauth.has_creds() && oauth_refresh.is_empty() {
        info!("No YouTube refresh token yet — opening browser to authorize sending...");
        match tokio::time::timeout(
            std::time::Duration::from_secs(120),
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

    let oauth_task = oauth.clone();
    let client_task = client.clone();
    let live_chat_task = Arc::clone(&live_chat);
    // Channel carrying PromptResponses from the engine to the stream loop,
    // so `prompt_for_input` can await the operator's typed answer.
    let (prompt_tx, mut prompt_rx) = mpsc::unbounded_channel::<PromptResponse>();
    let prompt_tx_task = prompt_tx.clone();
    let write_task = write_ws_cockatiel.clone();
    let auth_task = auth_token.clone();
    let module_task = module_name.clone();
    let instance_task = instance_uuid.clone();
    tokio::spawn(async move {
        while let Some(msg) = read_ws_cockatiel.next().await {
            match msg {
                Ok(WsMessage::Binary(data)) => {
                    if let Ok(container) = Container::decode(data.as_ref()) {
                        info!(
                            "Received from engine: {:?}",
                            container
                                .payload
                                .as_ref()
                                .map(|p| std::mem::discriminant(p))
                        );

                        // Answer the engine's liveness probe with our auth token
                        // so a quiet period never severs us.
                        if let Some(Payload::AuthVerify(_)) = container.payload {
                            let reply = Container {
                                version: 1,
                                auth_token: auth_task.clone(),
                                module_name: module_task.clone(),
                                module_instance_uuid7: instance_task.clone(),
                                payload: Some(Payload::AuthVerify(AuthVerify {
                                    cur_auth: auth_task.clone(),
                                })),
                            };
                            let mut buf = Vec::new();
                            if reply.encode(&mut buf).is_ok() {
                                let mut w = write_task.lock().await;
                                let _ = w.send(WsMessage::Binary(buf.into())).await;
                            }
                        }
                        // SendToPlatforms handling: post to the active live chat.
                        else if let Some(Payload::SendToPlatforms(send)) = container.payload {
                            match send_to_youtube(&oauth_task, &client_task, &live_chat_task, &send.msg).await {
                                Ok(()) => info!("Sent to YouTube live chat: {}", send.msg),
                                Err(e) => error!("SendToPlatforms failed: {}", e),
                            }
                        } else if let Some(Payload::PromptResponse(resp)) = container.payload {
                            // Forward operator answers to the awaiting prompt.
                            let _ = prompt_tx_task.send(resp);
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
    });

    // Re-acquire credentials whenever YouTube rejects them (bad channel/API key).
    cockatiel_client::load_env_file(".env");
    'configure: loop {
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
                        &mut *write_ws_cockatiel.lock().await,
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
                        120,
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
            &mut *write_ws_cockatiel.lock().await,
            &mut prompt_rx,
            &auth_token,
            &module_name,
            &instance_uuid,
            "YouTube Channel Configuration",
            "Enter your YouTube Channel Handle (e.g. @vulbyte), Channel ID (UC...),\n\
             or a direct Video ID / Stream URL (for unlisted streams).",
            "Channel Handle, ID, or Video URL",
            PromptKind::String,
            300,
        )
        .await;
        if let Some(val) = input {
            channel_input = val.trim().to_string();
        }
    }

    if api_keys.is_empty() {
        let input = prompt_for_input(
            &mut *write_ws_cockatiel.lock().await,
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
             config.json. Press Cancel to skip (discovery will still work\n\
             via InnerTube, but chat monitoring won't).",
            "YouTube Data API Key",
            PromptKind::Credential,
            300,
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

    // ── Validate the API key early ──────────────────────────────────────
    // If the key is invalid (e.g. the placeholder "testkey"), prompt the
    // user to enter a real key via the TUI prompt dialog so they never have
    // to manually edit a config file.
    if !is_api_key_valid(&client, &key_manager).await {
        warn!("YouTube API key is invalid or missing. Prompting user for a valid key...");
        let new_key = prompt_for_input(
            &mut *write_ws_cockatiel.lock().await,
            &mut prompt_rx,
            &auth_token,
            &module_name,
            &instance_uuid,
            "YouTube API Key Required",
            "Your YouTube Data API key is invalid or missing.\n\n\
             Stream discovery will still work without a key (via InnerTube),\n\
             but live chat monitoring requires a valid key.\n\n\
             To get a key:\n\
             1. Go to console.cloud.google.com > APIs & Services > Credentials\n\
             2. Create an API key\n\
             3. Enable the YouTube Data API v3\n\n\
             Enter your API key below, or press Cancel to continue\n\
             without chat monitoring (discovery only).",
            "YouTube Data API Key",
            PromptKind::Credential,
            300,
        )
        .await;

        if let Some(key) = new_key {
            let trimmed = key.trim().to_string();
            if !trimmed.is_empty() {
                api_keys = vec![trimmed];
                key_manager = ApiKeyManager::new(api_keys.clone());
                save_adapter_config(&channel_input, &api_keys, &unlisted_ids, "", "", "");
                info!("YouTube API key updated and saved to config.json");
                // Re-validate immediately so we catch typos before entering the
                // discovery loop.
                if !is_api_key_valid(&client, &key_manager).await {
                    warn!(
                        "The key you entered still fails validation. \
                         You can re-enter it by restarting the module."
                    );
                }
            }
        } else {
            info!("No API key provided — continuing with discovery-only mode (InnerTube).");
        }
    }

    let maybe_video_id = extract_video_id(&channel_input);

    let channel_id = if let Some(ref vid) = maybe_video_id {
        info!("Targeting direct video stream ID: {}", vid);
        if !unlisted_ids.contains(vid) {
            unlisted_ids.push(vid.clone());
        }
        "".to_string()
    } else {
        match get_channel_id(&client, &channel_input, &key_manager).await {
            Ok(cid) => {
                if cid != channel_input && cid.starts_with("UC") {
                    save_adapter_config(&cid, &api_keys, &unlisted_ids, "", "", "");
                }
                cid
            }
            Err(e) => {
                error!("Failed to resolve YouTube channel '{}': {}. Re-acquiring credentials...", channel_input, e);
                channel_input.clear();
                api_keys.clear();
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                continue 'configure;
            }
        }
    };

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
                    error!("Failed to fetch streams: {}. Retrying in 15 seconds...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
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
            &mut *write_ws_cockatiel.lock().await,
            &mut prompt_rx,
            &auth_token,
            &module_name,
            &instance_uuid,
        )
        .await
        {
            Some(s) => s,
            None => {
                info!("No streams currently found. Re-scanning in 30 seconds...");
                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                continue;
            }
        };

        info!(
            "Selected stream: {} ({})",
            chosen_stream.title, chosen_stream.video_id
        );

        // Monitor stream live chat until the stream ends
        monitor_stream_chat(
            &mut *write_ws_cockatiel.lock().await,
            &auth_token,
            &module_name,
            &instance_uuid,
            &client,
            &chosen_stream.video_id,
            &key_manager,
            &live_chat,
        )
        .await;

        info!("Stream finished. Restarting discovery loop for seamless transition...");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[tokio::test]
    async fn test_resolve_handle_web_fallback() {
        let client = reqwest::Client::new();
        let mgr = ApiKeyManager::new(vec![]);
        let resolved = get_channel_id(&client, "vulbyte", &mgr).await;
        assert!(resolved.is_ok());
        assert_eq!(resolved.unwrap(), "UCKZigHbgpJG9ldxXMqmiZUg");

        let resolved_at = get_channel_id(&client, "@vulbyte", &mgr).await;
        assert!(resolved_at.is_ok());
        assert_eq!(resolved_at.unwrap(), "UCKZigHbgpJG9ldxXMqmiZUg");
    }

    #[tokio::test]
    async fn test_fetch_channel_streams_web() {
        let client = reqwest::Client::new();
        let streams = fetch_channel_streams_web(&client, "UCKZigHbgpJG9ldxXMqmiZUg").await;
        let found = streams.iter().any(|s| s.video_id == "1a2iSWQzl7I");
        assert!(found, "Expected to find scheduled stream 1a2iSWQzl7I in web streams: {:?}", streams);
    }
}
