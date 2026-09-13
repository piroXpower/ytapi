import os
import json
import secrets
import asyncio
import aiohttp
from cachetools import TTLCache
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
import yt_dlp

app = FastAPI(title="High-Speed Music Stream API")

DB_FILE = "keys.json"
# Fast in-memory cache: Stores up to 10,000 extracted audio URLs for 4 hours
URL_CACHE = TTLCache(maxsize=10000, ttl=14400)


def load_keys():
    if not os.path.exists(DB_FILE):
        with open(DB_FILE, "w") as f:
            json.dump({"yuki_master": {"name": "admin"}}, f)
        return {"yuki_master": {"name": "admin"}}
    try:
        with open(DB_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_key(api_key: str, name: str):
    keys = load_keys()
    keys[api_key] = {"name": name}
    with open(DB_FILE, "w") as f:
        json.dump(keys, f)


def is_valid_key(api_key: str) -> bool:
    if not api_key:
        return False
    keys = load_keys()
    return api_key in keys


# Asynchronously extract raw audio stream using InnerTube Android/iOS clients to avoid bot blocks
def _extract(video_id: str) -> str:
    ydl_opts = {
        "format": "140/bestaudio[ext=m4a]/bestaudio",
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "extract_flat": False,
        "extractor_args": {
            "youtube": {
                "player_client": ["android", "ios"],
                "skip": ["webpage", "configs"],
            }
        },
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
        return info.get("url")


async def get_audio_url_fast(video_id: str) -> str:
    if video_id in URL_CACHE:
        return URL_CACHE[video_id]

    loop = asyncio.get_running_loop()
    url = await loop.run_in_executor(None, _extract, video_id)
    if url:
        URL_CACHE[video_id] = url
    return url


async def stream_raw_chunks(source_url: str, request_range: str = None):
    headers = {
        "User-Agent": "com.google.android.youtube/19.29.37 (Linux; U; Android 11) gzip"
    }
    if request_range:
        headers["Range"] = request_range

    timeout = aiohttp.ClientTimeout(total=None, sock_read=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(source_url, headers=headers) as resp:
            async for chunk in resp.content.iter_chunked(131072):  # 128 KB buffer
                yield chunk


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/generate-key")
async def create_key(data: dict):
    name = data.get("name", "anonymous").strip()
    new_key = f"yuki_{secrets.token_hex(16)}"
    save_key(new_key, name)
    return JSONResponse({"api_key": new_key, "status": "success"})


@app.get("/", response_class=HTMLResponse)
async def landing():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Turbo Music API</title>
        <style>
            * { box-sizing: border-box; margin:0; padding:0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
            body { background: #080b12; color: #f1f5f9; display: flex; justify-content: center; align-items: center; min-height: 100vh; padding: 20px; }
            .card { background: #111827; border: 1px solid #1f2937; border-radius: 12px; padding: 30px; width: 100%; max-width: 520px; box-shadow: 0 10px 25px rgba(0,0,0,0.5); }
            h1 { color: #38bdf8; font-size: 1.6rem; margin-bottom: 8px; }
            p { color: #94a3b8; font-size: 0.9rem; line-height: 1.5; margin-bottom: 20px; }
            input { width: 100%; padding: 12px; background: #0b0f19; border: 1px solid #374151; border-radius: 6px; color: #fff; margin-bottom: 12px; font-size: 0.95rem; }
            button { width: 100%; padding: 12px; background: #0284c7; color: #fff; border: 0; border-radius: 6px; font-weight: 600; cursor: pointer; transition: 0.2s; }
            button:hover { background: #0369a1; }
            .out { display:none; background: #042235; border: 1px solid #0284c7; padding: 12px; border-radius: 6px; margin-top: 15px; font-family: monospace; font-size: 0.85rem; word-break: break-all; }
            .code { background: #040711; padding: 12px; border-radius: 6px; font-family: monospace; font-size: 0.82rem; color: #38bdf8; border-left: 3px solid #38bdf8; margin-top: 20px; word-break: break-all; }
        </style>
    </head>
    <body>
        <div class="card">
            <h1>⚡ Turbo Music API</h1>
            <p>Generate your free streaming key. Low-latency chunk piping for bots and media players.</p>
            <input type="text" id="name" placeholder="Enter bot name or project tag...">
            <button onclick="genKey()">Generate Free API Key</button>
            <div id="result" class="out"></div>
            <div class="code">GET /stream/{VIDEO_ID}?key=YOUR_API_KEY</div>
        </div>
        <script>
            async function genKey() {
                const name = document.getElementById('name').value || 'User';
                const r = await fetch('/generate-key', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({name})
                });
                const d = await r.json();
                const res = document.getElementById('result');
                res.style.display = 'block';
                res.innerHTML = '<b>API Key:</b><br>' + d.api_key;
            }
        </script>
    </body>
    </html>
    """


@app.get("/stream/{video_id}")
async def stream(video_id: str, key: str = None, request: Request = None):
    if not is_valid_key(key):
        raise HTTPException(status_code=403, detail="Invalid or unauthorized API key.")

    try:
        audio_url = await get_audio_url_fast(video_id)
        if not audio_url:
            raise HTTPException(status_code=404, detail="Track could not be loaded.")

        client_range = request.headers.get("range") if request else None

        return StreamingResponse(
            stream_raw_chunks(audio_url, client_range),
            media_type="audio/mp4",
            headers={
                "Accept-Ranges": "bytes",
                "Cache-Control": "public, max-age=31536000, immutable",
                "X-Powered-By": "TurboMusic-Engine/3.0",
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
