"""
Hinge Tone — unified server.

- ws://localhost:8765       Lid angle broadcast
- http://localhost:8088     Static files (dj.html, index.html, ...)
- GET /api/yt?url=...       yt-dlp bestaudio stream (m4a/webm)

Sensor polling runs on a dedicated thread so asyncio never blocks on
hidapi I/O. yt-dlp runs as a subprocess and the resulting audio file
is streamed back as the response body.

Run:
    python3 -m venv venv && source venv/bin/activate
    pip install websockets pybooklid hidapi
    brew install yt-dlp
    python server.py
"""

import asyncio
import json
import os
import subprocess
import tempfile
import threading
import urllib.parse
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

import websockets
from pybooklid import LidSensor

clients = set()
latest_angle = None  # float, set by sensor thread


# ── HTTP: static + /api/yt ─────────────────────
class Handler(SimpleHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return  # quiet

    def do_GET(self):
        if self.path.startswith('/api/yt'):
            self._handle_yt()
        else:
            super().do_GET()

    def _handle_yt(self):
        qs = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(qs)
        url = params.get('url', [None])[0]
        if not url:
            self.send_error(400, 'missing url')
            return
        with tempfile.TemporaryDirectory() as tmpdir:
            outpath = os.path.join(tmpdir, 'out.%(ext)s')
            cmd = [
                'yt-dlp',
                '-f', 'bestaudio[ext=m4a]/bestaudio',
                '--no-playlist',
                '-o', outpath,
                url,
            ]
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
                if result.returncode != 0:
                    msg = (result.stderr or 'yt-dlp failed')[:400]
                    print(f"yt-dlp err: {msg}", flush=True)
                    self.send_error(500, msg)
                    return
                files = os.listdir(tmpdir)
                if not files:
                    self.send_error(500, 'no audio produced')
                    return
                fpath = os.path.join(tmpdir, files[0])
                ext = os.path.splitext(files[0])[1].lstrip('.').lower()
                ctype = {
                    'm4a': 'audio/mp4',
                    'webm': 'audio/webm',
                    'opus': 'audio/ogg',
                    'mp3': 'audio/mpeg',
                    'ogg': 'audio/ogg',
                }.get(ext, 'application/octet-stream')
                with open(fpath, 'rb') as f:
                    data = f.read()
                self.send_response(200)
                self.send_header('Content-Type', ctype)
                self.send_header('Content-Length', str(len(data)))
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('X-Audio-Ext', ext)
                self.end_headers()
                self.wfile.write(data)
                print(f"yt ok ({len(data)//1024}KB, .{ext}) {url}", flush=True)
            except subprocess.TimeoutExpired:
                self.send_error(504, 'timeout')
            except Exception as e:
                self.send_error(500, str(e)[:300])


def http_thread():
    here = os.path.dirname(os.path.abspath(__file__))
    os.chdir(here)
    httpd = ThreadingHTTPServer(('localhost', 8088), Handler)
    print(f"HTTP serving {here} on http://localhost:8088", flush=True)
    httpd.serve_forever()


# ── sensor / WebSocket ────────────────────────
def sensor_thread():
    global latest_angle
    with LidSensor() as sensor:
        for angle in sensor.monitor(interval=0.05):
            latest_angle = angle


async def broadcaster():
    while True:
        if latest_angle is not None and clients:
            msg = json.dumps({"angle": latest_angle})
            stale = []
            for ws in clients:
                try:
                    await ws.send(msg)
                except websockets.ConnectionClosed:
                    stale.append(ws)
            for ws in stale:
                clients.discard(ws)
        await asyncio.sleep(0.05)


async def handler(websocket):
    clients.add(websocket)
    print(f"WS client connected. total={len(clients)}", flush=True)
    try:
        await websocket.wait_closed()
    finally:
        clients.discard(websocket)
        print(f"WS client disconnected. total={len(clients)}", flush=True)


async def main():
    threading.Thread(target=sensor_thread, daemon=True).start()
    threading.Thread(target=http_thread, daemon=True).start()
    asyncio.create_task(broadcaster())
    async with websockets.serve(handler, "localhost", 8765):
        print("WS bridge listening on ws://localhost:8765", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.", flush=True)
