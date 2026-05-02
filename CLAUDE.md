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

- **`DuckDuckGoSearcher`** — Scrapes DuckDuckGo Lite via POST requests using primp with randomized TLS fingerprints (`impersonate="random"`). Supports optional Tor proxy via DDG's official Onion address for CAPTCHA-free access. Parses results with BeautifulSoup. Handles SafeSearch (`kp` param) and region (`kl` param) configuration. DDG's responses (including errors, CAPTCHA pages, empty results) are passed through to the client as-is. CAPTCHA countermeasures: TLS fingerprint randomization, throttle with jitter (2s ±30%), session rotation every 10 requests, optional retry with exponential backoff on 202/429.
- **`WebContentFetcher`** — Fetches arbitrary URLs, strips non-content elements (script, style, nav, header, footer), and returns cleaned text truncated to 8000 chars.

Two MCP tools are exposed: `search` and `fetch_content`.

## Configuration

Environment variables read at startup (not per-request):
- `DDG_SAFE_SEARCH`: `STRICT` | `MODERATE` (default) | `OFF`
- `DDG_REGION`: Region code like `us-en`, `cn-zh`, `jp-ja`, `wt-wt`
- `DDG_THROTTLE`: Min seconds between requests (default `2.0`)
- `DDG_THROTTLE_JITTER`: Jitter fraction for throttle (default `0.3` = ±30%)
- `DDG_SESSION_ROTATION_INTERVAL`: Rotate HTTP session every N requests (default `10`)
- `DDG_RETRY_DELAY`: Base delay in seconds for retry on 202/429 (default `3.0`)
- `DDG_MAX_RETRIES`: Max retries on CAPTCHA/rate-limit (default `3`)
- `DDG_PROXY`: SOCKS5 proxy for primp, e.g. `socks5h://127.0.0.1:9050` (Tor)
- `DDG_BASE_URL`: Override the DDG Lite endpoint URL, e.g. `https://duckduckgogg42xjoc72x3sjasowoarfbgcmvfimaftt6twagswzczad.onion/lite/` (DDG official Onion address for Tor)
- `DDG_PROXIES`: Comma-separated residential proxy pool in `ip:port:user:pass` format. Enables round-robin IP rotation with automatic block detection and dynamic throttling (~5s/search overall). Blocked IPs are temporarily excluded and revived after cooldown.
- `DDG_PROXIES_TARGET_INTERVAL`: Target seconds between searches across all proxies (default `5.0`). Throttle per-proxy is auto-calculated as `target / active_count`.
- `DDG_PROXIES_COOLDOWN`: Seconds to block a proxy after CAPTCHA/403 detection (default `300.0`)

## Testing

- **Unit tests** (`test_server.py`): tests using `unittest` style with `unittest.mock.patch` to mock httpx. Covers search throttle, search parsing, content fetching errors, and configuration.
- **E2E tests** (`test_e2e.py`): 6 tests using `pytest-asyncio` with MCP SDK's `create_connected_server_and_client_session` from `mcp.shared.memory` for in-memory MCP client/server testing.
- **CI**: GitHub Actions (`.github/workflows/test.yml`) runs tests on Python 3.10–3.14 using `astral-sh/setup-uv`.

## Key Dependencies

- `mcp[cli]>=1.26.0` (FastMCP framework)
- `httpx>=0.28.1` + `httpcore>=1.0.8` (async HTTP client; httpcore 1.0.8+ required for Python 3.14)
- `primp>=1.2.3` (TLS fingerprint randomization via browser impersonation)
- `beautifulsoup4` (HTML parsing)
- Dev: `pytest`, `pytest-asyncio`, `anyio`
- Build system: `hatchling`
- Package manager: `uv`
- Python: `>=3.10`, tested through `3.14`
