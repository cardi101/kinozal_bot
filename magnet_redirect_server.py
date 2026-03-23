import collections
import html
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, urlsplit

HOST = os.getenv("MAGNET_REDIRECT_HOST", "0.0.0.0")
PORT = int(os.getenv("MAGNET_REDIRECT_PORT", "8081"))

_RATE_LIMIT = int(os.getenv("MAGNET_RATE_LIMIT", "30"))
_RATE_WINDOW = 60  # seconds
_rate_lock = threading.Lock()
_rate_counters: collections.defaultdict = collections.defaultdict(list)


def _is_rate_limited(ip: str) -> bool:
    if _RATE_LIMIT <= 0:
        return False
    now = time.monotonic()
    with _rate_lock:
        hits = [t for t in _rate_counters[ip] if now - t < _RATE_WINDOW]
        _rate_counters[ip] = hits
        if len(hits) >= _RATE_LIMIT:
            return True
        _rate_counters[ip].append(now)
        return False

def build_magnet(info_hash: str, dn: str) -> str:
    magnet = f"magnet:?xt=urn:btih:{info_hash}"
    if dn:
        magnet += f"&dn={quote(dn)}"
    return magnet

PAGE_TEMPLATE = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Открыть magnet</title>
  <meta name="robots" content="noindex,nofollow">
  <style>
    body {{
      font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
      margin: 0;
      background: #111827;
      color: #f9fafb;
    }}
    .wrap {{
      max-width: 760px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }}
    .card {{
      background: #1f2937;
      border: 1px solid #374151;
      border-radius: 18px;
      padding: 24px;
      box-shadow: 0 10px 30px rgba(0,0,0,.25);
    }}
    h1 {{
      margin: 0 0 12px;
      font-size: 28px;
    }}
    p {{
      line-height: 1.5;
      color: #d1d5db;
    }}
    .mono {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      background: #0b1220;
      border: 1px solid #374151;
      border-radius: 12px;
      padding: 12px;
      word-break: break-all;
      color: #e5e7eb;
      margin: 14px 0 18px;
    }}
    .btns {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-top: 18px;
    }}
    .btn {{
      display: inline-block;
      text-decoration: none;
      border-radius: 12px;
      padding: 12px 16px;
      border: 1px solid #4b5563;
      background: #111827;
      color: #f9fafb;
      cursor: pointer;
      font-size: 15px;
    }}
    .btn.primary {{
      background: #2563eb;
      border-color: #2563eb;
    }}
    .note {{
      margin-top: 16px;
      font-size: 14px;
      color: #9ca3af;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>🧲 Открыть magnet</h1>
      <p>Сейчас попробуем открыть ссылку в торрент-клиенте автоматически. Если не сработает — нажми кнопку ниже или скопируй ссылку.</p>

      <div class="mono" id="magnet">{magnet_html}</div>

      <div class="btns">
        <a class="btn primary" id="openBtn" href="{magnet_attr}">Открыть вручную</a>
        <button class="btn" id="copyBtn" type="button">Скопировать magnet</button>
      </div>

      <div class="note">Info-hash: {info_hash_html}</div>
    </div>
  </div>

  <script>
    const magnet = {magnet_js};

    document.getElementById("copyBtn").addEventListener("click", async () => {{
      try {{
        await navigator.clipboard.writeText(magnet);
        document.getElementById("copyBtn").textContent = "Скопировано";
      }} catch (e) {{
        document.getElementById("copyBtn").textContent = "Не удалось скопировать";
      }}
    }});

    setTimeout(() => {{
      window.location.href = magnet;
    }}, 150);
  </script>
</body>
</html>
"""

class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, body: bytes, content_type: str = "text/html; charset=utf-8") -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args):
        return

    def do_GET(self):
        parts = urlsplit(self.path)
        path = parts.path
        qs = parse_qs(parts.query)

        if path == "/healthz":
            self._send(200, b"ok", "text/plain; charset=utf-8")
            return

        ip = (self.headers.get("X-Forwarded-For") or self.client_address[0] or "").split(",")[0].strip()
        if _is_rate_limited(ip):
            self._send(429, b"too many requests", "text/plain; charset=utf-8")
            return

        m = re.fullmatch(r"/m/([A-Fa-f0-9]{40})", path)
        if not m:
            self._send(404, b"not found", "text/plain; charset=utf-8")
            return

        info_hash = m.group(1).upper()
        dn = qs.get("dn", [""])[0]
        magnet = build_magnet(info_hash, dn)

        page = PAGE_TEMPLATE.format(
            magnet_html=html.escape(magnet),
            magnet_attr=html.escape(magnet, quote=True),
            magnet_js=repr(magnet),
            info_hash_html=html.escape(info_hash),
        )
        self._send(200, page.encode("utf-8"))

if __name__ == "__main__":
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"magnet redirect server listening on {HOST}:{PORT}")
    httpd.serve_forever()
