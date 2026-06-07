# EPG Merge Filter

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.x-blue.svg)](https://www.python.org/)

Fetches EPG (XMLTV) sources declared in an M3U playlist, merges them into a single deduplicated XML file, and publishes the result as a gzip-compressed GitHub Release asset — all automatically via GitHub Actions.

## How it works

1. **Parse M3U** — reads `url-tvg` / `tvg-id` attributes from an M3U playlist supplied via the `M3U_URL` secret (or a manual override).
2. **Fetch EPG sources concurrently** — downloads each XMLTV URL in parallel (configurable worker count and timeout) using [`curl-cffi`](https://github.com/lexiforest/curl_cffi), which replicates a real browser's TLS/HTTP2 fingerprint to bypass Cloudflare Bot Management without a headless browser.
3. **Merge & deduplicate** — combines `<channel>` and `<programme>` elements across all sources, dropping duplicates keyed on channel ID.
4. **Publish** — writes `output/merged_epg.xml.gz` and uploads it to the `latest` GitHub Release, giving a stable, permanent download URL.

## Download

The latest merged EPG is always available at:

```
https://github.com/iwinstar/epg_merge_filter/releases/latest/download/merged_epg.xml.gz
```

## Configuration

All runtime parameters are controlled through environment variables / GitHub secrets and Actions inputs — no code changes needed.

| Variable | Where to set | Description |
|---|---|---|
| `M3U_URL` | Repository secret | URL of the source M3U playlist |
| `CF_PROXY_URL` | Repository secret | *(Optional)* Cloudflare Workers proxy URL for sources that block even curl-cffi |
| `MAX_WORKERS` | Workflow env | Parallel fetch threads (default `6`) |
| `REQUEST_TIMEOUT` | Workflow env | Per-request timeout in seconds (default `60`) |
| `IMPERSONATE` | Workflow input / env | Browser fingerprint for curl-cffi: `chrome`, `safari`, `safari_ios`, `chrome136`, … (default `chrome`) |
| `OUTPUT_PATH` | Workflow env | Output file path (default `output/merged_epg.xml.gz`) |

## Triggering a run

The workflow (`Merge EPG`) can be triggered three ways:

- **Push** — automatically on any commit that modifies `scripts/epg.py` or `.github/workflows/epg.yml`.
- **Schedule** — enable the commented-out `cron` line in the workflow to run on a timer.
- **Manual** — use *Actions → Merge EPG → Run workflow*, optionally overriding `m3u_url` and `impersonate`.

## Requirements

- Python ≥ 3.10 (workflow uses 3.14)
- `curl-cffi >= 0.15`

## License

Apache 2.0
