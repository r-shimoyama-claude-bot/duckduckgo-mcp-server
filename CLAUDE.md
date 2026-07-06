# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Model Context Protocol (MCP) server providing DuckDuckGo web search and webpage content fetching. Built with Python using the FastMCP framework. Published to PyPI as `duckduckgo-mcp-server`.

## Commands

```bash
# Install dependencies
uv sync

# Run the server
uv run duckduckgo-mcp-server

# Run with MCP Inspector (for interactive testing)
mcp dev src/duckduckgo_mcp_server/server.py

# Run all tests (unit + e2e)
uv run python -m pytest src/duckduckgo_mcp_server/ -v

# Run only unit tests
uv run python -m pytest src/duckduckgo_mcp_server/test_server.py -v

# Run only e2e MCP protocol tests
uv run python -m pytest src/duckduckgo_mcp_server/test_e2e.py -v

# Run a single test
uv run python -m pytest src/duckduckgo_mcp_server/test_server.py::TestSearchThrottle::test_throttle_waits_when_request_too_recent

# Build package
uv build
```

## Architecture

Single-module server in `src/duckduckgo_mcp_server/server.py` with two main classes:

- **`DuckDuckGoSearcher`** — Scrapes DuckDuckGo Lite via POST requests using primp with randomized TLS fingerprints (`impersonate="random"`). Supports optional Tor proxy via DDG's official Onion address for CAPTCHA-free access. Parses results with BeautifulSoup. Handles SafeSearch (`kp` param) and region (`kl` param) configuration. DDG's responses (including errors, CAPTCHA pages, empty results) are passed through to the client as-is. CAPTCHA countermeasures: TLS fingerprint randomization, throttle with jitter (2s ±30%), session rotation every 10 requests, a TTL result cache for repeated queries (DDG_RESULT_CACHE_TTL), session warm-up (a GET to seed cookies before each POST, mirroring the natural browser flow), optional retry with exponential backoff on 202/429. The DDG Onion route (Tor, `DDG_BASE_URL=...onion/html/`) is CAPTCHA-free: primp is bypassed (its spoofed headers trigger HTTP 406 on the Onion endpoint) and plain httpx over socks5 is used instead, with the `/html/` endpoint parsed via `result__a` markup (vs `result-link` for lite). For high-volume batch use, `TorPortPool` (`DDG_TOR_SOCKS_PORTS`) spreads requests across multiple Tor SOCKS ports to aggregate bandwidth across independent circuits, and `search_batch` runs queries in parallel under a concurrency semaphore.
- **`WebContentFetcher`** — Fetches arbitrary URLs, strips non-content elements (script, style, nav, header, footer), and returns cleaned text truncated to 8000 chars.

Three MCP tools are exposed: `search` (single query), `search_batch` (multiple queries in parallel — faster than N sequential `search` calls; each query reuses the result cache), and `fetch_content`.

## Configuration

Environment variables read at startup (not per-request):
- `DDG_SAFE_SEARCH`: `STRICT` | `MODERATE` (default) | `OFF`
- `DDG_REGION`: Region code like `us-en`, `cn-zh`, `jp-ja`, `wt-wt`
- `DDG_THROTTLE`: Min seconds between requests (default `2.0`)
- `DDG_THROTTLE_JITTER`: Jitter fraction for throttle (default `0.3` = ±30%)
- `DDG_SESSION_ROTATION_INTERVAL`: Rotate HTTP session every N requests (default `10`)
- `DDG_WARMUP`: Warm up each new primp session with a GET to the endpoint to seed cookies before the POST search (default `1` = on). Set `0` to disable.
- `DDG_RETRY_DELAY`: Base delay in seconds for retry on 202/429 (default `3.0`)
- `DDG_MAX_RETRIES`: Max retries on CAPTCHA/rate-limit (default `3`)
- `DDG_PROXY`: SOCKS5 proxy for primp, e.g. `socks5h://127.0.0.1:9050` (Tor)
- `DDG_BASE_URL`: Override the DDG endpoint URL. An `.onion` URL forces the Onion route (Tor via socks5, CAPTCHA-free, httpx used instead of primp). A `/html/` URL switches the parser to `result__a` markup. Recommended for CAPTCHA resilience: `https://duckduckgogg42xoc72x3sjasowoarfbgcmvfimaftt6twagswzczad.onion/html/` (DDG official Onion) with `DDG_PROXY=socks5h://127.0.0.1:9050` (Tor daemon on 9050).
- `DDG_PROXIES`: Comma-separated residential proxy pool in `ip:port:user:pass` format. Enables round-robin IP rotation with automatic block detection and dynamic throttling (~5s/search overall). Blocked IPs are temporarily excluded and revived after cooldown.
- `DDG_PROXIES_TARGET_INTERVAL`: Target seconds between searches across all proxies (default `5.0`). Throttle per-proxy is auto-calculated as `target / active_count`.
- `DDG_PROXIES_COOLDOWN`: Seconds to block a proxy after CAPTCHA/403 detection (default `300.0`)
- `DDG_RESULT_CACHE_TTL`: Seconds to cache successful search results for repeated queries (default `600`). A repeated query/page returns instantly without hitting the network. Set `0` to disable.
- `DDG_TOR_SOCKS_PORTS`: Comma-separated Tor SOCKS ports for the Onion route, e.g. `127.0.0.1:9050,127.0.0.1:9051,...` (requires matching `SocksPort` entries in torrc). Each port is an independent circuit; `TorPortPool` spreads requests (least-inflight) to aggregate bandwidth. Empty/unset → single `DDG_PROXY` (legacy).
- `DDG_TOR_THROTTLE_PER_PORT`: Min seconds between requests per Tor port (default `0` — Onion is CAPTCHA-free). When `TorPortPool` is active, the global `DDG_THROTTLE` is bypassed.
- `DDG_TOR_MAX_CONNECTIONS_PER_PORT`: httpx connection pool limit per Tor port (default `4`).
- `DDG_BATCH_CONCURRENCY`: Max parallel queries inside `search_batch` (default `8`). Roughly 1.5-2× the number of Tor ports.
- `DDG_BATCH_MAX`: Max queries accepted by a single `search_batch` call (default `100`). Larger batches must be split by the caller.
- `DDG_FETCH_THROTTLE`: Min seconds between fetch_content requests per domain (default `2.0`)

## Testing

- **Unit tests** (`test_server.py`): tests using `unittest` style with `unittest.mock.patch` to mock httpx. Covers search throttle, search parsing, content fetching errors, and configuration.
- **E2E tests** (`test_e2e.py`): 6 tests using `pytest-asyncio` with MCP SDK's `create_connected_server_and_client_session` from `mcp.shared.memory` for in-memory MCP client/server testing.
- **CI**: GitHub Actions (`.github/workflows/test.yml`) runs tests on Python 3.10–3.14 using `astral-sh/setup-uv`.

## Key Dependencies

- `mcp[cli]>=1.26.0` (FastMCP framework)
- `httpx[socks]>=0.28.1` + `httpcore>=1.0.8` (async HTTP client with SOCKS5 for the Tor/Onion route; httpcore 1.0.8+ required for Python 3.14)
- `primp>=1.2.3` (TLS fingerprint randomization via browser impersonation)
- `beautifulsoup4` (HTML parsing)
- Dev: `pytest`, `pytest-asyncio`, `anyio`
- Build system: `hatchling`
- Package manager: `uv`
- Python: `>=3.10`, tested through `3.14`
