"""End-to-end MCP protocol tests using in-memory client/server sessions."""

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import pytest_asyncio

from mcp.shared.memory import create_connected_server_and_client_session

from duckduckgo_mcp_server.server import mcp as mcp_app
from duckduckgo_mcp_server import server as searcher_module


@pytest.fixture
def ddg_html_factory():
    """Build minimal DDG Lite HTML pages."""

    def _build(results):
        rows = []
        for r in results:
            snippet_html = ""
            if r.get("snippet"):
                snippet_html = f'<td class="result-snippet">{r["snippet"]}</td>'
            rows.append(
                f'<tr>'
                f'  <td><a class="result-link" href="{r["href"]}">{r["title"]}</a></td>'
                f"  {snippet_html}"
                f"</tr>"
            )
        return f"<html><body><table>{''.join(rows)}</table></body></html>"

    return _build


@pytest.fixture
def local_http_server():
    """Start a local HTTP server serving given HTML content."""

    servers = []

    def _make_server(html_content):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(html_content.encode("utf-8"))

            def log_message(self, format, *args):
                return

        server = HTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        servers.append(server)
        return f"http://127.0.0.1:{server.server_address[1]}"

    yield _make_server

    for s in servers:
        s.shutdown()


@pytest.mark.asyncio
async def test_server_lists_tools():
    async with create_connected_server_and_client_session(mcp_app) as client:
        tools_result = await client.list_tools()
        tool_names = {t.name for t in tools_result.tools}
        assert "search" in tool_names
        assert "fetch_content" in tool_names

        # Verify input schemas exist
        for tool in tools_result.tools:
            assert tool.inputSchema is not None
            assert "properties" in tool.inputSchema


@pytest.mark.asyncio
async def test_fetch_content_tool_e2e(local_http_server):
    html = "<html><body><h1>Hello E2E</h1><p>Test content here.</p></body></html>"
    url = local_http_server(html)

    async with create_connected_server_and_client_session(mcp_app) as client:
        result = await client.call_tool("fetch_content", {"url": url})
        text = result.content[0].text
        assert "Hello E2E" in text
        assert "Test content here." in text


@pytest.mark.asyncio
async def test_search_tool_e2e(ddg_html_factory):
    html = ddg_html_factory([
        {"title": "E2E Result", "href": "https://e2e.example.com", "snippet": "An e2e snippet"},
    ])

    mock_resp = MagicMock(spec=httpx.Response)
    mock_resp.text = html
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)
    mock_client.is_closed = False

    with patch.object(searcher_module.searcher, "_get_client", return_value=mock_client):
        async with create_connected_server_and_client_session(mcp_app) as client:
            result = await client.call_tool("search", {"query": "e2e test"})
            text = result.content[0].text
            assert "E2E Result" in text
            assert "https://e2e.example.com" in text


@pytest.mark.asyncio
async def test_fetch_content_tool_accepts_backend_param(local_http_server):
    """The fetch_content tool should accept a per-call `backend` argument."""
    html = "<html><body><h1>Backend Param Test</h1></body></html>"
    url = local_http_server(html)

    async with create_connected_server_and_client_session(mcp_app) as client:
        result = await client.call_tool("fetch_content", {"url": url, "backend": "httpx"})
        text = result.content[0].text
        assert "Backend Param Test" in text


@pytest.mark.asyncio
async def test_fetch_content_tool_lists_backend_in_schema():
    """The `backend` parameter should be advertised in fetch_content's inputSchema."""
    async with create_connected_server_and_client_session(mcp_app) as client:
        tools_result = await client.list_tools()
        fetch_tool = next(t for t in tools_result.tools if t.name == "fetch_content")
        props = fetch_tool.inputSchema.get("properties", {})
        assert "backend" in props, f"expected 'backend' in fetch_content inputSchema, got: {list(props)}"


@pytest.mark.asyncio
async def test_search_tool_handles_errors():
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
    mock_client.is_closed = False

    with patch.object(searcher_module.searcher, "_get_client", return_value=mock_client):
        async with create_connected_server_and_client_session(mcp_app) as client:
            result = await client.call_tool("search", {"query": "timeout test"})
            text = result.content[0].text
            # Should return a user-friendly message, not a protocol error
            assert "No results were found" in text or "error" in text.lower()
