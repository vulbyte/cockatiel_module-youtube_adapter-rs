# YouTube Adapter

Bridges a YouTube live stream's chat into Cockatiel and sends messages back.

## Credentials

- **YouTube Data API v3 key(s)** — required for **both** stream discovery **and**
  reading live chat. Configure them in `module_specific.api_keys` (a list; the
  adapter rotates across them).
- **Google OAuth** (`google_oauth_client_id`, `google_oauth_client_secret`,
  `refresh_token`) — required for **sending** messages and moderation actions.
  An empty OAuth config disables sending only; reading still works with a key.

## Quota — why you may want several keys

YouTube grants each **Google Cloud project** roughly **10,000 quota units per
day**, and **every API key in the same project shares that single pool**. Rotating
keys that live in one project does **not** increase your daily budget — when that
project's quota drains, all of the keys fail together until the daily reset
(midnight Pacific).

To get more read quota, create **each key in its own Google Cloud project**
(`N` projects ≈ `N × 10,000`/day) and list all of them in
`module_specific.api_keys`. The adapter rotates across the ring and backs off
when every key is exhausted, then resumes automatically after the reset.

## Notes

- Reading chat uses only the Data API key; it does not use OAuth.
- **Keyless / InnerTube reading is intentionally not used.** It is not part of
  YouTube's public API and is out of scope for this adapter; a valid Data API key
  is required for chat monitoring. (If you want a keyless path, it is open source
  — build your own.)
