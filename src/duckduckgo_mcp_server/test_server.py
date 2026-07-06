import asyncio
import os
import sys
import threading
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import AsyncMock, patch, MagicMock
import unittest

import httpx
from bs4 import BeautifulSoup

import duckduckgo_mcp_server.server

from duckduckgo_mcp_server.server import (
    DuckDuckGoSearcher,
    ProxyRotator,
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


def _make_searcher(**kwargs):
    """Create a DuckDuckGoSearcher with primp disabled so tests use httpx mocks.

    Tests assume the clearnet lite markup (result-link); force the lite parser
    regardless of any DDG_BASE_URL set in .env (which may be the Onion html route).
    """
    s = DuckDuckGoSearcher(**kwargs)
    s._primp_available = False
    s._rotator = None
    s._is_onion = False
    s._is_html_endpoint = False
    return s


class TestDuckDuckGoSearcher(unittest.TestCase):
    def test_format_results_for_llm_populates_entries(self):
        searcher = _make_searcher()
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
        searcher = _make_searcher()

        formatted = searcher.format_results_for_llm([])

        self.assertIn("No results were found", formatted)


class TestSearchThrottle(unittest.TestCase):
    def test_throttle_waits_when_request_too_recent(self):
        """If last request was <2s ago, the next request should sleep."""
        searcher = _make_searcher()
        searcher._last_request_time = datetime.now() - timedelta(seconds=0.5)
        ctx = DummyCtx()

        mock_resp = _mock_post_response("<html><body></body></html>")
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client), \
             patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            asyncio.run(searcher.search("test", ctx))

        # Should have slept for roughly 1.5 seconds (2.0 - 0.5)
        mock_sleep.assert_called_once()
        wait_time = mock_sleep.call_args[0][0]
        self.assertGreater(wait_time, 1.0)
        self.assertLessEqual(wait_time, 2.0)

    def test_throttle_skips_wait_when_enough_time_passed(self):
        """If last request was >=2s ago, no sleep needed."""
        searcher = _make_searcher()
        searcher._last_request_time = datetime.now() - timedelta(seconds=3)
        ctx = DummyCtx()

        mock_resp = _mock_post_response("<html><body></body></html>")
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client), \
             patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            asyncio.run(searcher.search("test", ctx))

        mock_sleep.assert_not_called()

    def test_throttle_not_active_on_first_request(self):
        """First request ever should not sleep."""
        searcher = _make_searcher()
        self.assertIsNone(searcher._last_request_time)
        ctx = DummyCtx()

        mock_resp = _mock_post_response("<html><body></body></html>")
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client), \
             patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            asyncio.run(searcher.search("test", ctx))

        mock_sleep.assert_not_called()


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
        searcher = _make_searcher()
        ctx = DummyCtx()

        mock_resp = _mock_post_response(html)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            results, has_next_page, raw_response = asyncio.run(searcher.search("test query", ctx, region, page))
        return results, has_next_page, raw_response

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
        searcher = _make_searcher()
        ctx = DummyCtx()

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client):
            results, has_next, raw_response = asyncio.run(searcher.search("test", ctx))
        self.assertEqual(results, [])
        self.assertFalse(has_next)
        self.assertEqual(raw_response, "timeout")

    def test_search_returns_empty_on_http_error(self):
        searcher = _make_searcher()
        ctx = DummyCtx()

        mock_resp = MagicMock()
        mock_resp.status_code = 503
        mock_resp.request = MagicMock()
        mock_resp.text = "Service Unavailable"
        error = httpx.HTTPStatusError("error", request=mock_resp.request, response=mock_resp)

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_resp.raise_for_status = MagicMock(side_effect=error)
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client):
            results, has_next, raw_response = asyncio.run(searcher.search("test", ctx))
        self.assertEqual(results, [])
        self.assertFalse(has_next)
        self.assertEqual(raw_response, "Service Unavailable")

    def test_search_returns_empty_on_no_results(self):
        html = "<html><body><p>No results</p></body></html>"
        results, _, raw_response = self._run_search(html)
        self.assertEqual(results, [])
        self.assertIn("No results", raw_response)

    def test_search_passes_through_captcha_page(self):
        html = "<html><body><div class='anomaly-modal'>Are you a bot?</div></body></html>"
        searcher = _make_searcher()
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
            results, has_next, raw_response = asyncio.run(searcher.search("test", ctx))
        self.assertEqual(results, [])
        self.assertFalse(has_next)
        self.assertIn("Are you a bot?", raw_response)

    def test_search_passes_through_captcha_text_page(self):
        html = "<html><body><p>Unfortunately, bots use DuckDuckGo too much</p></body></html>"
        searcher = _make_searcher()
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
            results, has_next, raw_response = asyncio.run(searcher.search("test", ctx))
        self.assertEqual(results, [])
        self.assertIn("bots use DuckDuckGo", raw_response)

    def test_search_passes_through_http_403(self):
        searcher = _make_searcher()
        ctx = DummyCtx()

        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.text = "Forbidden"

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            results, has_next, raw_response = asyncio.run(searcher.search("test", ctx))
        self.assertEqual(results, [])
        self.assertEqual(raw_response, "Forbidden")

    def test_search_passes_through_http_429(self):
        searcher = _make_searcher()
        ctx = DummyCtx()

        mock_resp = MagicMock()
        mock_resp.status_code = 429
        mock_resp.text = "Too Many Requests"

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False

        with patch.object(searcher, "_get_client", return_value=mock_client), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            results, has_next, raw_response = asyncio.run(searcher.search("test", ctx))
        self.assertEqual(results, [])
        self.assertEqual(raw_response, "Too Many Requests")


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
        searcher = _make_searcher(safe_search=SafeSearchMode.STRICT)
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
        searcher = _make_searcher(default_region="us-en")
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


class TestProxyRotator(unittest.TestCase):
    def test_round_robin(self):
        proxies = ["http://a:1", "http://b:2", "http://c:3"]
        rotator = ProxyRotator(proxies, target_interval=5.0)
        self.assertEqual(rotator.next(), "http://a:1")
        self.assertEqual(rotator.next(), "http://b:2")
        self.assertEqual(rotator.next(), "http://c:3")
        self.assertEqual(rotator.next(), "http://a:1")

    def test_blocked_proxy_skipped(self):
        proxies = ["http://a:1", "http://b:2", "http://c:3"]
        rotator = ProxyRotator(proxies, target_interval=5.0, cooldown=300.0)
        rotator.next()  # a
        p = rotator.next()  # b
        rotator.mark_blocked(p)
        # Should skip b and go to c then a
        self.assertEqual(rotator.next(), "http://c:3")
        self.assertEqual(rotator.next(), "http://a:1")

    def test_all_blocked_returns_none(self):
        proxies = ["http://a:1"]
        rotator = ProxyRotator(proxies, target_interval=5.0, cooldown=300.0)
        p = rotator.next()
        rotator.mark_blocked(p)
        self.assertIsNone(rotator.next())

    def test_expired_block_revived(self):
        proxies = ["http://a:1", "http://b:2"]
        rotator = ProxyRotator(proxies, target_interval=5.0, cooldown=0.0)
        p = rotator.next()  # a
        rotator.mark_blocked(p)
        # Cooldown is 0 so a is revived; round-robin advances to b, then a
        self.assertEqual(rotator.next(), "http://b:2")
        self.assertEqual(rotator.next(), "http://a:1")

    def test_throttle_interval_scales_with_active_count(self):
        proxies = ["http://a:1", "http://b:2", "http://c:3", "http://d:4"]
        rotator = ProxyRotator(proxies, target_interval=5.0)
        # 4 active: 5/4 = 1.25
        self.assertAlmostEqual(rotator.throttle_interval, 1.25)
        # Block 2
        rotator.mark_blocked("http://a:1")
        rotator.mark_blocked("http://b:2")
        # 2 active: 5/2 = 2.5
        self.assertAlmostEqual(rotator.throttle_interval, 2.5)

    def test_active_count(self):
        proxies = ["http://a:1", "http://b:2", "http://c:3"]
        rotator = ProxyRotator(proxies, target_interval=5.0, cooldown=300.0)
        self.assertEqual(rotator.active_count, 3)
        rotator.mark_blocked("http://a:1")
        self.assertEqual(rotator.active_count, 2)

    def test_from_env_parses_ip_port_user_pass(self):
        with patch.dict(os.environ, {"DDG_PROXIES": "1.2.3.4:8080:user:pass,5.6.7.8:9090:u2:p2"}):
            rotator = ProxyRotator.from_env()
            self.assertIsNotNone(rotator)
            self.assertEqual(len(rotator._proxies), 2)
            self.assertEqual(rotator._proxies[0], "http://user:pass@1.2.3.4:8080")
            self.assertEqual(rotator._proxies[1], "http://u2:p2@5.6.7.8:9090")

    def test_from_env_returns_none_when_empty(self):
        with patch.dict(os.environ, {"DDG_PROXIES": ""}):
            self.assertIsNone(ProxyRotator.from_env())


class TestAcceptLanguage(unittest.TestCase):
    def test_region_jp_ja(self):
        self.assertEqual(
            DuckDuckGoSearcher._accept_language_for_region("jp-ja"),
            "ja-JP,ja;q=0.5",
        )

    def test_region_us_en(self):
        self.assertEqual(
            DuckDuckGoSearcher._accept_language_for_region("us-en"),
            "en-US,en;q=0.5",
        )

    def test_region_de_de(self):
        self.assertEqual(
            DuckDuckGoSearcher._accept_language_for_region("de-de"),
            "de-DE,de;q=0.5",
        )

    def test_empty_region_returns_default(self):
        self.assertEqual(
            DuckDuckGoSearcher._accept_language_for_region(""),
            "en-US,en;q=0.5",
        )

    def test_no_dash_returns_default(self):
        self.assertEqual(
            DuckDuckGoSearcher._accept_language_for_region("en"),
            "en-US,en;q=0.5",
        )

    def test_unknown_lang_falls_back(self):
        self.assertEqual(
            DuckDuckGoSearcher._accept_language_for_region("wt-wt"),
            "en-US,en;q=0.5",
        )

    def test_build_headers_includes_accept_language_for_region(self):
        searcher = _make_searcher()
        headers = searcher._build_headers("jp-ja")
        self.assertEqual(headers["Accept-Language"], "ja-JP,ja;q=0.5")

    def test_build_headers_default_accept_language(self):
        searcher = _make_searcher()
        headers = searcher._build_headers()
        self.assertEqual(headers["Accept-Language"], "en-US,en;q=0.5")


class TestPrimpSessionManagement(unittest.TestCase):
    def test_reset_primp_session_clears_client(self):
        searcher = DuckDuckGoSearcher()
        searcher._primp_available = True
        searcher._primp_client = MagicMock()
        searcher._primp_request_count = 5
        searcher._primp_proxy = "http://old:1234"
        searcher._reset_primp_session()
        self.assertIsNone(searcher._primp_client)
        self.assertIsNone(searcher._primp_proxy)
        self.assertEqual(searcher._primp_request_count, 0)

    def test_primp_client_built_when_none(self):
        """_get_primp_client should build a new client if none exists."""
        searcher = DuckDuckGoSearcher()
        searcher._primp_available = True
        searcher._warmup_enabled = False
        searcher._primp_client = None
        client = searcher._get_primp_client()
        self.assertIsNotNone(client)

    def test_primp_client_rotated_after_interval(self):
        """After session_rotation_interval requests, client should be rebuilt."""
        searcher = DuckDuckGoSearcher()
        searcher._primp_available = True
        searcher._warmup_enabled = False
        searcher._session_rotation_interval = 2
        first = searcher._get_primp_client()
        searcher._primp_request_count = 2
        second = searcher._get_primp_client()
        self.assertIsNot(first, second)

    def test_primp_client_rebuilt_on_proxy_change(self):
        """Client should be rebuilt when proxy changes, even if session is fresh."""
        searcher = DuckDuckGoSearcher()
        searcher._primp_available = True
        searcher._warmup_enabled = False
        first = searcher._get_primp_client("http://a:1")
        # Same proxy → reused
        reused = searcher._get_primp_client("http://a:1")
        self.assertIs(first, reused)
        # Different proxy → rebuilt
        second = searcher._get_primp_client("http://b:2")
        self.assertIsNot(first, second)


class TestWarmup(unittest.TestCase):
    """Session warm-up (GET before POST) behavior."""

    def _searcher_with_mock_client(self):
        s = DuckDuckGoSearcher()
        s._primp_available = True
        s._warmup_enabled = True
        mock_client = MagicMock()
        s._build_primp_client = MagicMock(return_value=mock_client)
        return s, mock_client

    def test_warmup_get_called_on_new_client(self):
        """A freshly built client triggers one warm-up GET to the endpoint."""
        searcher, mock_client = self._searcher_with_mock_client()
        searcher._get_primp_client()
        gets = [c for c in mock_client.request.call_args_list
                if c.args and c.args[0] == "GET"]
        self.assertEqual(len(gets), 1)
        self.assertEqual(gets[0].args[1], searcher._base_url)

    def test_warmup_skipped_when_disabled(self):
        """No GET is issued when warm-up is disabled."""
        searcher, mock_client = self._searcher_with_mock_client()
        searcher._warmup_enabled = False
        searcher._get_primp_client()
        mock_client.request.assert_not_called()

    def test_warmup_called_once_per_proxy(self):
        """Reusing the same proxy/client does not warm up again."""
        searcher, mock_client = self._searcher_with_mock_client()
        searcher._get_primp_client("http://a:1")
        searcher._get_primp_client("http://a:1")  # reuse, no rebuild
        gets = [c for c in mock_client.request.call_args_list
                if c.args and c.args[0] == "GET"]
        self.assertEqual(len(gets), 1)

    def test_warmup_failure_does_not_raise(self):
        """Warm-up errors are swallowed; client is still returned."""
        searcher, mock_client = self._searcher_with_mock_client()
        mock_client.request.side_effect = RuntimeError("timeout")
        client = searcher._get_primp_client()
        self.assertIsNotNone(client)

    def test_reset_primp_session_re_enables_warmup(self):
        """After resetting the session, the next build warms up again."""
        searcher, mock_client = self._searcher_with_mock_client()
        searcher._get_primp_client("http://a:1")
        self.assertEqual(mock_client.request.call_count, 1)
        searcher._reset_primp_session()
        mock_client2 = MagicMock()
        searcher._build_primp_client.return_value = mock_client2
        searcher._get_primp_client("http://a:1")
        gets = [c for c in mock_client2.request.call_args_list
                if c.args and c.args[0] == "GET"]
        self.assertEqual(len(gets), 1)

    def test_direct_ip_uses_direct_key(self):
        """No-proxy (direct IP) warm-up is tracked under the __direct__ key."""
        searcher, _ = self._searcher_with_mock_client()
        searcher._get_primp_client()
        self.assertIn("__direct__", searcher._warmed_proxies)


class TestHtmlEndpointParsing(unittest.TestCase):
    """Parser for the /html/ endpoint (Onion and clearnet html)."""

    HTML = """
    <html><body>
      <div class="results">
        <div class="result results_links results_links_deep web-result">
          <h2 class="result__title"><a class="result__a" href="https://example.com/1">Title One</a></h2>
          <a class="result__snippet">Snippet one</a>
        </div>
        <div class="result result--ad results_links">
          <h2 class="result__title"><a class="result__a" href="https://ad.example.com">Ad Title</a></h2>
          <a class="result__snippet">Ad snippet</a>
        </div>
        <div class="result results_links">
          <h2 class="result__title"><a class="result__a" href="https://example.com/2">Title Two</a></h2>
          <a class="result__snippet">Snippet two</a>
        </div>
      </div>
      <form action="/html/">
        <input name="q" value="t">
        <input name="vqd" value="4-1234567890">
        <input name="s" value="20">
      </form>
    </body></html>
    """

    def setUp(self):
        self.s = _make_searcher()
        self.s._is_html_endpoint = True
        self.soup = BeautifulSoup(self.HTML, "html.parser")

    def test_extracts_results_skipping_ads(self):
        out = self.s._extract_html_endpoint(self.soup)
        self.assertEqual(len(out), 2)  # sponsored block excluded
        self.assertEqual(out[0], ("Title One", "https://example.com/1", "Snippet one"))
        self.assertEqual(out[1][0], "Title Two")
        self.assertEqual(out[1][1], "https://example.com/2")

    def test_extract_results_builds_searchresult_with_position(self):
        results = self.s._extract_results(self.soup)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].position, 1)
        self.assertEqual(results[1].position, 2)
        self.assertEqual(results[1].title, "Title Two")

    def test_parse_pagination_returns_html_vqd(self):
        vqd, has_next = self.s._parse_pagination(self.soup)
        self.assertEqual(vqd, "4-1234567890")
        self.assertTrue(has_next)

    def test_decode_ddg_redirect(self):
        decode = DuckDuckGoSearcher._decode_ddg_redirect
        self.assertEqual(
            decode("/l/?uddg=https%3A%2F%2Fexample.com%2Fpath&rut=abc"),
            "https://example.com/path",
        )
        self.assertEqual(decode("https://example.com/direct"), "https://example.com/direct")
        self.assertEqual(decode(""), "")


class TestOnionBackendSelection(unittest.TestCase):
    """Backend selection: Onion → httpx, clearnet → primp."""

    def test_onion_route_uses_httpx_not_primp(self):
        s = DuckDuckGoSearcher()
        s._primp_available = True
        s._is_onion = True
        s._is_html_endpoint = True
        self.assertFalse(s._use_primp)

    def test_clearnet_route_uses_primp(self):
        s = DuckDuckGoSearcher()
        s._primp_available = True
        s._is_onion = False
        s._is_html_endpoint = False
        self.assertTrue(s._use_primp)

    def test_init_detects_onion_and_html_from_base_url(self):
        old = os.environ.get("DDG_BASE_URL")
        os.environ["DDG_BASE_URL"] = "https://abc.onion/html/"
        try:
            s = DuckDuckGoSearcher()
            self.assertTrue(s._is_onion)
            self.assertTrue(s._is_html_endpoint)
            self.assertFalse(s._use_primp)
        finally:
            if old is None:
                os.environ.pop("DDG_BASE_URL", None)
            else:
                os.environ["DDG_BASE_URL"] = old


class TestResultCache(unittest.TestCase):
    """TTL result cache for repeated searches."""

    def _ctx(self):
        ctx = MagicMock()
        ctx.info = AsyncMock()
        return ctx

    def test_cache_hit_skips_network(self):
        s = _make_searcher()
        calls = {"n": 0}

        async def fake_fetch(query, ctx, region, page):
            calls["n"] += 1
            return ([SearchResult(title="T", link="https://x/", snippet="s", position=1)], False, None)

        s._fetch_results = fake_fetch
        ctx = self._ctx()

        async def run():
            r1 = await s.search("q", ctx)
            r2 = await s.search("q", ctx)
            return r1, r2

        r1, r2 = asyncio.run(run())
        self.assertEqual(calls["n"], 1)  # second call served from cache
        self.assertEqual(r1, r2)

    def test_empty_results_not_cached(self):
        s = _make_searcher()
        calls = {"n": 0}

        async def fake_fetch(query, ctx, region, page):
            calls["n"] += 1
            return ([], False, "empty")

        s._fetch_results = fake_fetch
        ctx = self._ctx()

        async def run():
            await s.search("q", ctx)
            await s.search("q", ctx)

        asyncio.run(run())
        self.assertEqual(calls["n"], 2)  # empty result set not cached


class TestSearchBatch(unittest.TestCase):
    """Parallel batch search tool."""

    def test_batch_combines_per_query_results(self):
        ctx = MagicMock()
        ctx.info = AsyncMock()

        async def fake_search(query, c, region="", page=1):
            return ([SearchResult(title=f"T-{query}", link=f"https://{query}/", snippet="s", position=1)], True, None)

        with patch.object(duckduckgo_mcp_server.server.searcher, "search", side_effect=fake_search):
            async def run():
                return await duckduckgo_mcp_server.server.search_batch(["a", "b", "c"], ctx)
            result = asyncio.run(run())
        self.assertIn("T-a", result)
        self.assertIn("T-b", result)
        self.assertIn("T-c", result)
        self.assertIn("---", result)

    def test_batch_handles_exception_per_query(self):
        ctx = MagicMock()
        ctx.info = AsyncMock()

        async def fake_search(query, c, region="", page=1):
            if query == "bad":
                raise RuntimeError("boom")
            return ([SearchResult(title=f"T-{query}", link=f"https://{query}/", snippet="s", position=1)], True, None)

        with patch.object(duckduckgo_mcp_server.server.searcher, "search", side_effect=fake_search):
            async def run():
                return await duckduckgo_mcp_server.server.search_batch(["good", "bad"], ctx)
            result = asyncio.run(run())
        self.assertIn("T-good", result)
        self.assertIn("Error: boom", result)


class TestTorPortPool(unittest.TestCase):
    """Tor multi-port pool: least-inflight selection and client reuse."""

    def test_least_inflight_then_release(self):
        async def run():
            from duckduckgo_mcp_server.server import TorPortPool
            pool = TorPortPool(["127.0.0.1:9050", "127.0.0.1:9051"])
            p1, _ = await pool.acquire()
            p2, _ = await pool.acquire()  # other port (less inflight)
            self.assertNotEqual(p1, p2)
            pool.release(p1)
            pool.release(p2)
            self.assertEqual(pool._inflight[p1], 0)
            self.assertEqual(pool._inflight[p2], 0)
            await pool.close_all()
        asyncio.run(run())

    def test_client_reused_per_port(self):
        async def run():
            from duckduckgo_mcp_server.server import TorPortPool
            pool = TorPortPool(["127.0.0.1:9050"])
            _, c1 = await pool.acquire()
            pool.release("127.0.0.1:9050")
            _, c2 = await pool.acquire()
            self.assertIs(c1, c2)
            await pool.close_all()
        asyncio.run(run())

    def test_close_all_clears_clients(self):
        async def run():
            from duckduckgo_mcp_server.server import TorPortPool
            pool = TorPortPool(["127.0.0.1:9050", "127.0.0.1:9051"])
            await pool.acquire()
            await pool.close_all()
            self.assertEqual(pool._clients, {})
        asyncio.run(run())


class TestMultiProxyInit(unittest.TestCase):
    """TorPortPool is built only on the Onion route with ports configured."""

    def test_tor_pool_built_on_onion_with_ports(self):
        old_ports = os.environ.get("DDG_TOR_SOCKS_PORTS")
        old_base = os.environ.get("DDG_BASE_URL")
        os.environ["DDG_TOR_SOCKS_PORTS"] = "127.0.0.1:9050,127.0.0.1:9051"
        os.environ["DDG_BASE_URL"] = "https://abc.onion/html/"
        try:
            s = DuckDuckGoSearcher()
            self.assertIsNotNone(s._tor_pool)
            self.assertEqual(len(s._tor_pool._ports), 2)
        finally:
            if old_ports is None:
                os.environ.pop("DDG_TOR_SOCKS_PORTS", None)
            else:
                os.environ["DDG_TOR_SOCKS_PORTS"] = old_ports
            if old_base is None:
                os.environ.pop("DDG_BASE_URL", None)
            else:
                os.environ["DDG_BASE_URL"] = old_base

    def test_no_tor_pool_when_unset_or_non_onion(self):
        s = _make_searcher()  # forces _is_onion=False, lite parser
        self.assertIsNone(s._tor_pool)


class TestSearchBatchConcurrency(unittest.TestCase):
    """search_batch respects BATCH_MAX and the concurrency semaphore."""

    def test_batch_rejects_too_many_queries(self):
        ctx = MagicMock()
        ctx.info = AsyncMock()
        too_many = [f"q{i}" for i in range(duckduckgo_mcp_server.server.searcher._batch_max + 1)]

        async def run():
            return await duckduckgo_mcp_server.server.search_batch(too_many, ctx)
        result = asyncio.run(run())
        self.assertIn("Too many queries", result)

    def test_batch_caps_concurrency(self):
        ctx = MagicMock()
        ctx.info = AsyncMock()
        state = {"current": 0, "max": 0}

        async def tracking_search(q, c, region="", page=1):
            state["current"] += 1
            state["max"] = max(state["max"], state["current"])
            await asyncio.sleep(0.05)
            state["current"] -= 1
            return ([SearchResult(title=q, link="https://x/", snippet="", position=1)], True, None)

        original = duckduckgo_mcp_server.server.searcher._batch_concurrency
        duckduckgo_mcp_server.server.searcher._batch_concurrency = 3
        try:
            with patch.object(duckduckgo_mcp_server.server.searcher, "search", side_effect=tracking_search):
                async def run():
                    return await duckduckgo_mcp_server.server.search_batch(["a", "b", "c", "d", "e"], ctx)
                asyncio.run(run())
        finally:
            duckduckgo_mcp_server.server.searcher._batch_concurrency = original
        self.assertLessEqual(state["max"], 3)

