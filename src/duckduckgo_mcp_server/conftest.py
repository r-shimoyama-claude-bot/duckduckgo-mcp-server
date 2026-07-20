"""Test configuration: isolate the suite from the local .env and Tor auto-detection.

The package's _load_dotenv() runs at server import time and would otherwise pull
the local duckduckgo-mcp-server/.env (which may enable the Onion route) into
every test. Setting these keys *before* any server import forces a deterministic
lite+primp baseline (DDG_AUTO_TOR disabled, no DDG_BASE_URL / proxy / Tor ports).
_load_dotenv() never overwrites keys that are already present in os.environ, so
pinning them here wins over .env. Individual tests (e.g. TestAutoTorDetection)
flip these flags back on within setUp/tearDown.
"""
import os

os.environ["DDG_AUTO_TOR"] = "0"
os.environ["DDG_AUTO_TOR_START"] = "0"
os.environ["DDG_BASE_URL"] = "https://lite.duckduckgo.com/lite/"
os.environ["DDG_TOR_SOCKS_PORTS"] = ""
os.environ["DDG_PROXY"] = ""
os.environ["DDG_PROXIES"] = ""
