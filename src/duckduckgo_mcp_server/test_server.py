import asyncio
import sys
import threading
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import AsyncMock, patch, MagicMock
import unittest

import httpx

import duckduckgo_mcp_server.server

from duckduckgo_mcp_server.server import (
    RateLimiter,
    DuckDuckGoSearcher,
    SafeSearchMode,
    SearchResult,
    SUPPORTED_FETCH_BACKENDS,
    WebContentFetcher,
)

try:
    import curl_cffi  # noqa: F401
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False


class DummyCtx:
    async def info(self, message):
        return None

    async def error(self, message):
        return None


class TestRateLimiter(unittest.TestCase):
    def test_acquire_removes_expired_entries(self):
        limiter = RateLimiter(requests_per_minute=1)
        limiter.requests.append(datetime.now() - timedelta(minutes=2))

        asyncio.run(limiter.acquire())

        self.assertEqual(len(limiter.requests), 1)
        self.assertLess((datetime.now() - limiter.requests[0]).total_seconds(), 1.0)


class TestRateLimiterEdgeCases(unittest.TestCase):
    def test_acquire_blocks_when_at_capacity(self):
        limiter = RateLimiter(requests_per_minute=2)
        now = datetime.now()
        limiter.requests = [now - timedelta(seconds=10), now - timedelta(seconds=5)]

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            asyncio.run(limiter.acquire())
            # Called twice: once for rate limit wait, once for jitter
            self.assertEqual(mock_sleep.call_count, 2)
            # First call should wait roughly 50 seconds (60 - 10)
            wait_time = mock_sleep.call_args_list[0][0][0]
            self.assertGreater(wait_time, 40)
            self.assertLessEqual(wait_time, 60)

    def test_acquire_allows_after_window_expires(self):
        limiter = RateLimiter(requests_per_minute=2)
        limiter.requests = [
            datetime.now() - timedelta(seconds=61),
            datetime.now() - timedelta(seconds=61),
        ]

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            asyncio.run(limiter.acquire())
            # Only jitter sleep, no rate limit wait
            mock_sleep.assert_called_once()
            jitter_time = mock_sleep.call_args[0][0]
            self.assertGreaterEqual(jitter_time, 0.1)
            self.assertLessEqual(jitter_time, 0.5)


class TestDuckDuckGoSearcher(unittest.TestCase):
    def test_format_results_for_llm_populates_entries(self):
        searcher = DuckDuckGoSearcher()
        results = [
            SearchResult(
                title="First Result",
                link="https://example.com/first",
                snippet="Snippet one",
                position=1,
            ),
            SearchResult(
                title="Second Result",
                link="https://example.com/second",
                snippet="Snippet two",
                position=2,
            ),
        ]

        formatted = searcher.format_results_for_llm(results)

        self.assertIn("Found 2 search results", formatted)
        self.assertIn("1. First Result", formatted)
        self.assertIn("URL: https://example.com/first", formatted)

    def test_format_results_for_llm_handles_empty(self):
        searcher = DuckDuckGoSearcher()

        formatted = searcher.format_results_for_llm([])

        self.assertIn("No results were found", formatted)


def _make_ddg_html(results):
    """Build a minimal DDG Lite-like HTML page with the given result dicts."""
    rows = []
    for r in results:
        snippet_html = ""
        if r.get("snippet"):
            snippet_html = f'<tr><td>&nbsp;&nbsp;&nbsp;</td><td class="result-snippet">{r["snippet"]}</td></tr>'
        rows.append(
            f'<tr><td valign="top">&nbsp;</td>'
            f'<td><a href="{r["href"]}" class="result-link">{r["title"]}</a></td></tr>'
            f'{snippet_html}'
        )
    return f'<html><body><table>{"".join(rows)}</table></body></html>'


def _mock_post_response(html, status_code=200):
    """Create a mock httpx.Response for POST requests."""
    resp = MagicMock(spec=httpx.Response)
    resp.text = html
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()
    return resp


class TestDuckDuckGoSearcherParsing(unittest.TestCase):
    def _run_search(self, html, region="", page=1):
        """Helper to run a search with mocked HTTP."""
        searcher = DuckDuckGoSearcher()
        ctx = DummyCtx()

        mock_resp = _mock_post_response(html)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            results, has_next_page, blocked = asyncio.run(searcher.search("test query", ctx, region, page))
        return results, has_next_page, blocked

    def test_search_parses_results_from_html(self):
        html = _make_ddg_html([
            {"title": "Result One", "href": "https://one.com", "snippet": "Snippet 1"},
            {"title": "Result Two", "href": "https://two.com", "snippet": "Snippet 2"},
            {"title": "Result Three", "href": "https://three.com", "snippet": "Snippet 3"},
        ])
        results, _, _ = self._run_search(html)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0].title, "Result One")
        self.assertEqual(results[0].link, "https://one.com")
        self.assertEqual(results[0].snippet, "Snippet 1")
        self.assertEqual(results[1].title, "Result Two")
        self.assertEqual(results[2].title, "Result Three")

    def test_search_returns_direct_urls(self):
        html = _make_ddg_html([
            {"title": "Direct Link", "href": "https://example.com/page", "snippet": "A snippet"},
        ])
        results, _, _ = self._run_search(html)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].link, "https://example.com/page")

    def test_search_handles_missing_snippet(self):
        html = _make_ddg_html([
            {"title": "No Snippet", "href": "https://nosnip.com"},
        ])
        results, _, _ = self._run_search(html)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].snippet, "")

    def test_search_returns_empty_on_timeout(self):
        searcher = DuckDuckGoSearcher()
        ctx = DummyCtx()

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client):
            results, has_next, blocked = asyncio.run(searcher.search("test", ctx))
        self.assertEqual(results, [])
        self.assertFalse(has_next)
        self.assertFalse(blocked)

    def test_search_returns_empty_on_http_error(self):
        searcher = DuckDuckGoSearcher()
        ctx = DummyCtx()

        mock_resp = MagicMock()
        mock_resp.status_code = 503
        mock_resp.request = MagicMock()
        error = httpx.HTTPStatusError("error", request=mock_resp.request, response=mock_resp)

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_resp.raise_for_status = MagicMock(side_effect=error)
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client):
            results, has_next, blocked = asyncio.run(searcher.search("test", ctx))
        self.assertEqual(results, [])
        self.assertFalse(has_next)
        self.assertFalse(blocked)

    def test_search_returns_empty_on_no_results(self):
        html = "<html><body><p>No results</p></body></html>"
        results, _, _ = self._run_search(html)
        self.assertEqual(results, [])

    def test_search_detects_captcha_response(self):
        html = "<html><body><div class='anomaly-modal'>Are you a bot?</div></body></html>"
        searcher = DuckDuckGoSearcher()
        ctx = DummyCtx()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            results, has_next, blocked = asyncio.run(searcher.search("test", ctx))
        self.assertEqual(results, [])
        self.assertFalse(has_next)
        self.assertTrue(blocked)
        self.assertIsNotNone(searcher._cooldown_until)

    def test_search_detects_captcha_text_signal(self):
        html = "<html><body><p>Unfortunately, bots use DuckDuckGo too much</p></body></html>"
        searcher = DuckDuckGoSearcher()
        ctx = DummyCtx()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = html
        mock_resp.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            results, has_next, blocked = asyncio.run(searcher.search("test", ctx))
        self.assertEqual(results, [])
        self.assertTrue(blocked)

    def test_search_detects_http_403(self):
        searcher = DuckDuckGoSearcher()
        ctx = DummyCtx()

        mock_resp = MagicMock()
        mock_resp.status_code = 403

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            results, has_next, blocked = asyncio.run(searcher.search("test", ctx))
        self.assertEqual(results, [])
        self.assertTrue(blocked)
        self.assertIsNotNone(searcher._cooldown_until)

    def test_search_detects_http_429(self):
        searcher = DuckDuckGoSearcher()
        ctx = DummyCtx()

        mock_resp = MagicMock()
        mock_resp.status_code = 429

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            results, has_next, blocked = asyncio.run(searcher.search("test", ctx))
        self.assertEqual(results, [])
        self.assertTrue(blocked)

    def test_search_respects_cooldown(self):
        searcher = DuckDuckGoSearcher()
        searcher._cooldown_until = datetime.now() + timedelta(seconds=60)
        ctx = DummyCtx()

        with patch("asyncio.sleep", new_callable=AsyncMock):
            results, has_next, blocked = asyncio.run(searcher.search("test", ctx))
        self.assertEqual(results, [])
        self.assertTrue(blocked)
        # No HTTP request should have been made during cooldown


def _serve_html(html_content):
    """Spin up a throwaway local HTTP server serving html_content. Returns (url, stop_fn)."""

    class SimpleHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(html_content.encode("utf-8"))

        def log_message(self, format, *args):
            return

    server = HTTPServer(("127.0.0.1", 0), SimpleHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    url = f"http://127.0.0.1:{server.server_address[1]}"

    def stop():
        server.shutdown()
        thread.join()

    return url, stop


# Backends to exercise in the parameterized fetcher tests. curl is only included
# when curl_cffi is actually installed (the optional [browser] extra).
_FETCH_BACKENDS_FOR_TESTING = ["httpx"] + (["curl"] if HAS_CURL_CFFI else [])


class TestWebContentFetcher(unittest.TestCase):
    def test_fetch_and_parse_extracts_clean_text(self):
        html_content = """
        <html>
            <head>
                <title>Example</title>
                <script>console.log('ignored');</script>
                <style>body { background: #fff; }</style>
            </head>
            <body>
                <nav>Navigation</nav>
                <header>Header</header>
                <h1>Sample Heading</h1>
                <p>Some meaningful paragraph.</p>
                <footer>Footer</footer>
            </body>
        </html>
        """

        url, stop = _serve_html(html_content)
        try:
            for backend in _FETCH_BACKENDS_FOR_TESTING:
                with self.subTest(backend=backend):
                    fetcher = WebContentFetcher(backend=backend)
                    text = asyncio.run(fetcher.fetch_and_parse(url, DummyCtx()))
                    self.assertIn("Sample Heading", text)
                    self.assertIn("Some meaningful paragraph.", text)
                    self.assertNotIn("Navigation", text)
                    self.assertNotIn("console.log", text)
        finally:
            stop()

    def test_fetch_and_parse_pagination(self):
        html_content = "<html><body><p>" + "A" * 100 + "</p></body></html>"
        url, stop = _serve_html(html_content)
        try:
            for backend in _FETCH_BACKENDS_FOR_TESTING:
                with self.subTest(backend=backend):
                    fetcher = WebContentFetcher(backend=backend)
                    # Fetch first 50 chars
                    text = asyncio.run(
                        fetcher.fetch_and_parse(url, DummyCtx(), start_index=0, max_length=50)
                    )
                    self.assertIn("start_index=50 to see more", text)
                    self.assertIn("of 100 total", text)
                    # Fetch from offset 50
                    text = asyncio.run(
                        fetcher.fetch_and_parse(url, DummyCtx(), start_index=50, max_length=50)
                    )
                    self.assertNotIn("to see more", text)
                    self.assertIn("of 100 total", text)
        finally:
            stop()


def _patch_backend_client(backend, *, get_return_value=None, get_side_effect=None):
    """Return a context manager that patches the HTTP client for the given backend.

    - "httpx": patches `httpx.AsyncClient`.
    - "curl":  patches `curl_cffi.requests.AsyncSession`.
    Both are patched with an AsyncMock whose .get() uses the provided return/side-effect.
    """
    mock_client = AsyncMock()
    if get_side_effect is not None:
        mock_client.get = AsyncMock(side_effect=get_side_effect)
    else:
        mock_client.get = AsyncMock(return_value=get_return_value)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    if backend == "httpx":
        return patch("httpx.AsyncClient", return_value=mock_client)
    elif backend == "curl":
        return patch("curl_cffi.requests.AsyncSession", return_value=mock_client)
    raise ValueError(f"no patcher for backend {backend!r}")


class TestWebContentFetcherErrors(unittest.TestCase):
    def test_fetch_returns_error_on_timeout(self):
        for backend in _FETCH_BACKENDS_FOR_TESTING:
            with self.subTest(backend=backend):
                fetcher = WebContentFetcher(backend=backend)
                # Use an exception whose type-name triggers the server's curl-path
                # error handling without needing curl_cffi's exception hierarchy.
                exc = httpx.TimeoutException("timed out") if backend == "httpx" else TimeoutError("timed out")
                with _patch_backend_client(backend, get_side_effect=exc):
                    result = asyncio.run(
                        fetcher.fetch_and_parse("https://example.com", DummyCtx())
                    )
                self.assertTrue(result.startswith("Error"), f"got: {result!r}")
                self.assertIn("timed out", result.lower())

    def test_fetch_returns_error_on_http_error(self):
        for backend in _FETCH_BACKENDS_FOR_TESTING:
            with self.subTest(backend=backend):
                fetcher = WebContentFetcher(backend=backend)
                mock_resp = MagicMock()
                mock_resp.status_code = 500
                mock_resp.request = MagicMock()
                if backend == "httpx":
                    err = httpx.HTTPStatusError("server error", request=mock_resp.request, response=mock_resp)
                else:
                    err = RuntimeError("curl http 500")
                mock_resp.raise_for_status = MagicMock(side_effect=err)
                with _patch_backend_client(backend, get_return_value=mock_resp):
                    result = asyncio.run(
                        fetcher.fetch_and_parse("https://example.com", DummyCtx())
                    )
                self.assertTrue(result.startswith("Error"), f"got: {result!r}")

    def test_fetch_handles_malformed_html(self):
        for backend in _FETCH_BACKENDS_FOR_TESTING:
            with self.subTest(backend=backend):
                fetcher = WebContentFetcher(backend=backend)
                mock_resp = MagicMock()
                mock_resp.text = "<<<not valid>>>"
                mock_resp.status_code = 200
                mock_resp.raise_for_status = MagicMock()
                with _patch_backend_client(backend, get_return_value=mock_resp):
                    result = asyncio.run(
                        fetcher.fetch_and_parse("https://example.com", DummyCtx())
                    )
                # Should not crash - returns some text (possibly empty or with metadata)
                self.assertIsInstance(result, str)


class TestWebContentFetcherBackend(unittest.TestCase):
    def test_init_rejects_unknown_backend(self):
        with self.assertRaises(ValueError):
            WebContentFetcher(backend="bogus")

    def test_default_backend_is_httpx(self):
        self.assertEqual(WebContentFetcher().default_backend, "httpx")

    def test_supported_backends_tuple(self):
        self.assertEqual(SUPPORTED_FETCH_BACKENDS, ("httpx", "curl", "auto"))

    def test_per_call_backend_overrides_default(self):
        """default=httpx, pass backend='curl' per-call → curl path is exercised."""
        fetcher = WebContentFetcher(backend="httpx")
        ctx = DummyCtx()
        called = {"httpx": False, "curl": False}

        async def fake_httpx(url):
            called["httpx"] = True
            return "<html><body><p>from httpx</p></body></html>"

        async def fake_curl(url):
            called["curl"] = True
            return "<html><body><p>from curl</p></body></html>"

        with patch.object(fetcher, "_fetch_httpx", side_effect=fake_httpx), \
             patch.object(fetcher, "_fetch_curl", side_effect=fake_curl):
            text = asyncio.run(
                fetcher.fetch_and_parse("https://example.com", ctx, backend="curl")
            )

        self.assertFalse(called["httpx"])
        self.assertTrue(called["curl"])
        self.assertIn("from curl", text)

    def test_per_call_unknown_backend_returns_error(self):
        fetcher = WebContentFetcher()
        result = asyncio.run(
            fetcher.fetch_and_parse("https://example.com", DummyCtx(), backend="bogus")
        )
        self.assertIn("Unknown fetch backend", result)

    def test_curl_backend_missing_dependency_error(self):
        """If curl_cffi isn't importable, curl backend returns a helpful install hint."""
        fetcher = WebContentFetcher(backend="curl")
        # Make the lazy `from curl_cffi.requests import AsyncSession` raise ImportError.
        with patch.dict(sys.modules, {"curl_cffi": None, "curl_cffi.requests": None}):
            result = asyncio.run(
                fetcher.fetch_and_parse("https://example.com", DummyCtx())
            )
        self.assertIn("Error", result)
        self.assertIn("pip install", result)
        self.assertIn("browser", result)


class TestWebContentFetcherAutoFallback(unittest.TestCase):
    def test_auto_uses_httpx_when_successful(self):
        fetcher = WebContentFetcher(backend="auto")
        called = {"httpx": 0, "curl": 0}

        async def fake_httpx(url):
            called["httpx"] += 1
            return "<html><body><p>ok from httpx</p></body></html>"

        async def fake_curl(url):
            called["curl"] += 1
            return "<html><body><p>from curl</p></body></html>"

        with patch.object(fetcher, "_fetch_httpx", side_effect=fake_httpx), \
             patch.object(fetcher, "_fetch_curl", side_effect=fake_curl):
            text = asyncio.run(fetcher.fetch_and_parse("https://example.com", DummyCtx()))

        self.assertEqual(called["httpx"], 1)
        self.assertEqual(called["curl"], 0)
        self.assertIn("ok from httpx", text)

    def test_auto_falls_back_on_403(self):
        fetcher = WebContentFetcher(backend="auto")
        called = {"curl": 0}

        mock_resp = MagicMock()
        mock_resp.status_code = 403
        err = httpx.HTTPStatusError("forbidden", request=MagicMock(), response=mock_resp)

        async def fake_httpx(url):
            raise err

        async def fake_curl(url):
            called["curl"] += 1
            return "<html><body><p>rescued by curl</p></body></html>"

        with patch.object(fetcher, "_fetch_httpx", side_effect=fake_httpx), \
             patch.object(fetcher, "_fetch_curl", side_effect=fake_curl):
            text = asyncio.run(fetcher.fetch_and_parse("https://example.com", DummyCtx()))

        self.assertEqual(called["curl"], 1)
        self.assertIn("rescued by curl", text)

    def test_auto_falls_back_on_cloudflare_challenge(self):
        fetcher = WebContentFetcher(backend="auto")
        called = {"curl": 0}

        async def fake_httpx(url):
            return (
                "<html><head><title>Just a moment...</title></head>"
                "<body>Enable JavaScript and cookies to continue</body></html>"
            )

        async def fake_curl(url):
            called["curl"] += 1
            return "<html><body><p>real content</p></body></html>"

        with patch.object(fetcher, "_fetch_httpx", side_effect=fake_httpx), \
             patch.object(fetcher, "_fetch_curl", side_effect=fake_curl):
            text = asyncio.run(fetcher.fetch_and_parse("https://example.com", DummyCtx()))

        self.assertEqual(called["curl"], 1)
        self.assertIn("real content", text)

    def test_auto_reraises_non_403_http_error(self):
        """A 500 under auto should NOT trigger curl fallback — only 403/CF signals do."""
        fetcher = WebContentFetcher(backend="auto")
        called = {"curl": 0}

        mock_resp = MagicMock()
        mock_resp.status_code = 500
        err = httpx.HTTPStatusError("server error", request=MagicMock(), response=mock_resp)

        async def fake_httpx(url):
            raise err

        async def fake_curl(url):
            called["curl"] += 1
            return "<html></html>"

        with patch.object(fetcher, "_fetch_httpx", side_effect=fake_httpx), \
             patch.object(fetcher, "_fetch_curl", side_effect=fake_curl):
            result = asyncio.run(fetcher.fetch_and_parse("https://example.com", DummyCtx()))

        self.assertEqual(called["curl"], 0)
        self.assertTrue(result.startswith("Error"))


class TestMainCliArgs(unittest.TestCase):
    def test_main_parses_fetch_backend_flag(self):
        with patch.object(sys, "argv", ["duckduckgo-mcp-server", "--fetch-backend", "auto"]), \
             patch("duckduckgo_mcp_server.server.mcp") as mock_mcp:
            duckduckgo_mcp_server.server.main()
            mock_mcp.run.assert_called_once()
        self.assertEqual(duckduckgo_mcp_server.server.fetcher.default_backend, "auto")

    def test_main_defaults_to_httpx(self):
        with patch.object(sys, "argv", ["duckduckgo-mcp-server"]), \
             patch("duckduckgo_mcp_server.server.mcp") as mock_mcp:
            duckduckgo_mcp_server.server.main()
            mock_mcp.run.assert_called_once()
        self.assertEqual(duckduckgo_mcp_server.server.fetcher.default_backend, "httpx")

    def test_main_applies_host_and_port_to_settings(self):
        argv = [
            "duckduckgo-mcp-server",
            "--transport", "streamable-http",
            "--host", "0.0.0.0",
            "--port", "7070",
        ]
        with patch.object(sys, "argv", argv), \
             patch("duckduckgo_mcp_server.server.mcp") as mock_mcp:
            duckduckgo_mcp_server.server.main()
            self.assertEqual(mock_mcp.settings.host, "0.0.0.0")
            self.assertEqual(mock_mcp.settings.port, 7070)
            mock_mcp.run.assert_called_once_with(transport="streamable-http")

    def test_main_rejects_host_or_port_with_stdio(self):
        argv = ["duckduckgo-mcp-server", "--port", "7070"]
        with patch.object(sys, "argv", argv), \
             patch("duckduckgo_mcp_server.server.mcp"):
            with self.assertRaises(SystemExit):
                duckduckgo_mcp_server.server.main()


class TestConfiguration(unittest.TestCase):
    def test_safe_search_enum_values(self):
        self.assertEqual(SafeSearchMode.STRICT.value, "1")
        self.assertEqual(SafeSearchMode.MODERATE.value, "-1")
        self.assertEqual(SafeSearchMode.OFF.value, "-2")

    def test_searcher_passes_safe_search_to_request(self):
        searcher = DuckDuckGoSearcher(safe_search=SafeSearchMode.STRICT)
        ctx = DummyCtx()

        mock_resp = _mock_post_response("<html><body></body></html>")
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            asyncio.run(searcher.search("test", ctx))

        call_kwargs = mock_client.post.call_args
        post_data = call_kwargs.kwargs.get("data") or call_kwargs[1].get("data")
        self.assertEqual(post_data["kp"], "1")

    def test_searcher_passes_region_to_request(self):
        searcher = DuckDuckGoSearcher(default_region="us-en")
        ctx = DummyCtx()

        mock_resp = _mock_post_response("<html><body></body></html>")
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            asyncio.run(searcher.search("test", ctx))

        call_kwargs = mock_client.post.call_args
        post_data = call_kwargs.kwargs.get("data") or call_kwargs[1].get("data")
        self.assertEqual(post_data["kl"], "us-en")
