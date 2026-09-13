import os
import json
import secrets
import asyncio
import aiohttp
from hashlib import sha256
from cachetools import TTLCache
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
import psycopg2
from psycopg2 import pool
import yt_dlp

app = FastAPI(title="MusicAPI Pro Stream Engine")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_yhbA5zJaf9ce@ep-nameless-queen-az3g147r-pooler.c-3.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"
)

URL_CACHE = TTLCache(maxsize=10000, ttl=14400)
SEARCH_CACHE = TTLCache(maxsize=2000, ttl=3600)

db_pool = None
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, dsn=DATABASE_URL)
except Exception as e:
    print(f"PostgreSQL pool init error: {e}")


def get_db_connection():
    if db_pool:
        return db_pool.getconn()
    return psycopg2.connect(DATABASE_URL)


def release_db_connection(conn):
    if db_pool:
        db_pool.putconn(conn)
    else:
        conn.close()


def init_db():
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute(
                """CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(255) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    api_key VARCHAR(255) UNIQUE NOT NULL,
                    audio_requests INT DEFAULT 0,
                    video_requests INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );"""
            )
            c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS audio_requests INT DEFAULT 0;")
            c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS video_requests INT DEFAULT 0;")
            conn.commit()
    finally:
        release_db_connection(conn)


init_db()


def hash_pw(password: str) -> str:
    return sha256(password.encode()).hexdigest()


def verify_and_track_request(key: str, media_type: str = "audio") -> bool:
    if not key:
        return False
    column = "video_requests" if media_type == "video" else "audio_requests"
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute(f"UPDATE users SET {column} = {column} + 1 WHERE api_key = %s RETURNING id;", (key,))
            updated = c.fetchone()
            conn.commit()
            return updated is not None
    except Exception:
        conn.rollback()
        return False
    finally:
        release_db_connection(conn)


def get_user_data(username: str):
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute("SELECT id, username, password_hash, api_key, audio_requests, video_requests FROM users WHERE username = %s;", (username,))
            return c.fetchone()
    finally:
        release_db_connection(conn)


# --- Media Stream Extraction ---

def get_yt_format_selector(media_type: str, quality: str) -> str:
    if media_type == "video":
        if quality == "720":
            return "best[height<=720][ext=mp4]/bestvideo[height<=720]+bestaudio/best[height<=720]"
        elif quality == "480":
            return "best[height<=480][ext=mp4]/best[height<=480]"
        elif quality == "360":
            return "best[height<=360][ext=mp4]/best[height<=360]"
        else:
            return "best[ext=mp4]/best"
    else:
        if quality == "low":
            return "worstaudio[ext=m4a]/worstaudio/140"
        else:
            return "140/bestaudio[ext=m4a]/bestaudio"


def _extract_media(video_id: str, media_type: str, quality: str) -> str:
    fmt = get_yt_format_selector(media_type, quality)
    ydl_opts = {
        "format": fmt,
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


async def get_stream_url_cached(video_id: str, media_type: str, quality: str) -> str:
    cache_key = f"{video_id}_{media_type}_{quality}"
    if cache_key in URL_CACHE:
        return URL_CACHE[cache_key]

    loop = asyncio.get_running_loop()
    url = await loop.run_in_executor(None, _extract_media, video_id, media_type, quality)
    if url:
        URL_CACHE[cache_key] = url
    return url


def _search_tracks(query: str, limit: int = 5):
    ydl_opts = {"format": "bestaudio", "quiet": True, "no_warnings": True, "extract_flat": "in_playlist"}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        res = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
        return [
            {"id": e.get("id"), "title": e.get("title"), "duration": e.get("duration"), "url": f"https://www.youtube.com/watch?v={e.get('id')}"}
            for e in res.get("entries", [])
        ]


async def stream_raw_chunks(source_url: str, request_range: str = None):
    headers = {"User-Agent": "com.google.android.youtube/19.29.37 (Linux; U; Android 11) gzip"}
    if request_range:
        headers["Range"] = request_range
    timeout = aiohttp.ClientTimeout(total=None, sock_read=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(source_url, headers=headers) as resp:
            async for chunk in resp.content.iter_chunked(131072):
                yield chunk


# --- Authentication & Account Endpoints ---

@app.post("/api/register")
async def register(data: dict):
    username = data.get("username", "").strip().lower()
    password = data.get("password", "")
    if len(username) < 3 or len(password) < 6:
        raise HTTPException(status_code=400, detail="Username >= 3, Password >= 6 chars required.")

    api_key = f"music_{secrets.token_hex(16)}"
    pw_hash = hash_pw(password)
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute(
                "INSERT INTO users (username, password_hash, api_key) VALUES (%s, %s, %s);",
                (username, pw_hash, api_key),
            )
            conn.commit()
        return {"status": "success", "api_key": api_key, "audio_requests": 0, "video_requests": 0}
    except psycopg2.IntegrityError:
        conn.rollback()
        raise HTTPException(status_code=400, detail="Username already registered.")
    finally:
        release_db_connection(conn)


@app.post("/api/login")
async def login(data: dict):
    username = data.get("username", "").strip().lower()
    password = data.get("password", "")
    user = get_user_data(username)
    if not user or user[2] != hash_pw(password):
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    return {
        "status": "success",
        "username": user[1],
        "api_key": user[3],
        "audio_requests": user[4],
        "video_requests": user[5]
    }


@app.post("/api/reset-key")
async def reset_key(data: dict):
    username = data.get("username", "").strip().lower()
    password = data.get("password", "")
    user = get_user_data(username)
    if not user or user[2] != hash_pw(password):
        raise HTTPException(status_code=401, detail="Authentication failed.")

    new_key = f"music_{secrets.token_hex(16)}"
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute("UPDATE users SET api_key = %s WHERE username = %s;", (new_key, username))
            conn.commit()
        return {"status": "success", "api_key": new_key}
    finally:
        release_db_connection(conn)


# --- Media Endpoints ---

@app.get("/stream/{video_id}")
async def stream(
    video_id: str,
    key: str = None,
    type: str = Query("audio", regex="^(audio|video)$"),
    quality: str = Query("high", regex="^(high|low|720|480|360)$"),
    request: Request = None,
):
    # Track metrics separately based on media_type
    if not verify_and_track_request(key, media_type=type):
        raise HTTPException(status_code=403, detail="Invalid or unauthorized API key.")

    media_url = await get_stream_url_cached(video_id, type, quality)
    if not media_url:
        raise HTTPException(status_code=404, detail="Media stream unavailable.")

    content_type = "video/mp4" if type == "video" else "audio/mpeg"
    client_range = request.headers.get("range") if request else None

    return StreamingResponse(
        stream_raw_chunks(media_url, client_range),
        media_type=content_type,
        headers={
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=31536000, immutable",
            "Access-Control-Allow-Origin": "*",
            "X-Powered-By": "MusicAPI-Engine/3.0",
        },
    )


@app.get("/search")
async def search(query: str = Query(..., min_length=1), limit: int = 5, key: str = None):
    if not verify_and_track_request(key, media_type="audio"):
        raise HTTPException(status_code=403, detail="Invalid API key.")
    if query in SEARCH_CACHE:
        return {"status": "success", "results": SEARCH_CACHE[query]}

    loop = asyncio.get_running_loop()
    results = await loop.run_in_executor(None, _search_tracks, query, limit)
    SEARCH_CACHE[query] = results
    return {"status": "success", "results": results}


# --- Frontend UI & Documentation ---

@app.get("/", response_class=HTMLResponse)
async def index():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>MusicAPI | Developer Suite</title>
        <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
        <style>
            :root {
                --bg: #090d16;
                --surface: #111726;
                --border: #1f293d;
                --primary: #38bdf8;
                --accent: #0ea5e9;
                --text: #f8fafc;
                --muted: #94a3b8;
            }
            * { margin:0; padding:0; box-sizing:border-box; font-family: 'Plus Jakarta Sans', sans-serif; }
            body { background: var(--bg); color: var(--text); min-height: 100vh; }
            nav { border-bottom: 1px solid var(--border); padding: 18px 40px; display: flex; justify-content: space-between; align-items: center; background: rgba(17, 23, 38, 0.8); backdrop-filter: blur(12px); position: sticky; top:0; z-index:50; }
            .logo { font-size: 1.3rem; font-weight: 700; color: #fff; }
            .logo span { color: var(--primary); }
            .btn { padding: 8px 16px; border-radius: 8px; font-size: 0.88rem; font-weight: 600; cursor: pointer; border: none; transition: 0.2s; }
            .btn-outline { background: transparent; border: 1px solid var(--border); color: #fff; }
            .btn-outline:hover { background: var(--surface); border-color: var(--primary); }
            .btn-primary { background: var(--accent); color: #fff; }
            .hero { text-align: center; padding: 60px 20px 30px; max-width: 800px; margin: 0 auto; }
            .hero h1 { font-size: 2.6rem; font-weight: 800; margin-bottom: 12px; }
            .hero p { color: var(--muted); font-size: 1.05rem; }
            .container { max-width: 1000px; margin: 0 auto; padding: 20px; display: grid; gap: 24px; }
            .card { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 25px; }
            .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-top: 15px; }
            .stat-box { background: #0b111e; border: 1px solid var(--border); border-radius: 8px; padding: 15px; }
            .stat-box h4 { color: var(--muted); font-size: 0.8rem; text-transform: uppercase; }
            .stat-box p { font-size: 1.4rem; font-weight: 700; color: var(--primary); margin-top: 5px; }
            .api-key-box { background: #080d1a; border: 1px dashed var(--primary); padding: 12px 16px; border-radius: 8px; display: flex; justify-content: space-between; align-items: center; margin: 15px 0; font-family: 'JetBrains Mono'; color: #38bdf8; word-break: break-all; }
            pre, code { font-family: 'JetBrains Mono', monospace; }
            .code-box { background: #070a12; border: 1px solid var(--border); border-radius: 8px; padding: 14px; margin-top: 10px; font-size: 0.85rem; color: #38bdf8; overflow-x: auto; }
            input, select { width: 100%; padding: 11px 14px; background: #0b111e; border: 1px solid var(--border); border-radius: 8px; color: #fff; margin-bottom: 12px; }
            .modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,0.7); display: none; align-items: center; justify-content: center; z-index: 100; backdrop-filter: blur(4px); }
            .modal { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 25px; width: 90%; max-width: 380px; }
            video, audio { width: 100%; margin-top: 12px; border-radius: 8px; }
        </style>
    </head>
    <body>
        <nav>
            <div class="logo">⚡ Music<span>API</span></div>
            <div id="navBtns">
                <button class="btn btn-outline" onclick="openModal('login')">Sign In</button>
                <button class="btn btn-primary" onclick="openModal('reg')">Get API Key</button>
            </div>
            <div id="userMenu" style="display:none;">
                <span id="welcomeUser" style="margin-right:12px; color:var(--muted); font-size:0.9rem;"></span>
                <button class="btn btn-outline" onclick="logout()">Logout</button>
            </div>
        </nav>

        <div class="hero">
            <h1>Unified Audio & Video Streaming Engine</h1>
            <p>Direct low-latency media piping with independent audio/video telemetry and multiple quality profiles.</p>
        </div>

        <div class="container">
            <div class="card" id="dashView" style="display:none;">
                <h2>Developer Dashboard</h2>
                <div class="api-key-box">
                    <span id="keyDisplay">music_...</span>
                    <button class="btn btn-primary" style="padding:6px 12px;" onclick="copyKey()">Copy</button>
                </div>
                <div class="stats-grid">
                    <div class="stat-box">
                        <h4>Audio Streams Served</h4>
                        <p id="audioReqCount">0</p>
                    </div>
                    <div class="stat-box">
                        <h4>Video Streams Served</h4>
                        <p id="videoReqCount">0</p>
                    </div>
                    <div class="stat-box">
                        <h4>Tier</h4>
                        <p>Unlimited</p>
                    </div>
                </div>
                <div style="margin-top:15px; display:flex; gap:10px;">
                    <button class="btn btn-outline" onclick="resetKey()">Reset Key</button>
                </div>

                <!-- In-Browser Stream Player (Supports Audio & Video) -->
                <div style="margin-top: 25px; border-top: 1px solid var(--border); padding-top: 20px;">
                    <h3>Live Stream Inspector</h3>
                    <p style="font-size:0.85rem; color:var(--muted); margin-bottom:10px;">Test playback and observe direct counter updates.</p>
                    <div style="display:grid; grid-template-columns: 2fr 1fr 1fr auto; gap:10px;">
                        <input type="text" id="testVidId" placeholder="Video ID (e.g. dQw4w9WgXcQ)" style="margin:0;">
                        <select id="testType" style="margin:0;" onchange="updateQualityDropdown()">
                            <option value="audio">Audio</option>
                            <option value="video">Video</option>
                        </select>
                        <select id="testQuality" style="margin:0;">
                            <option value="high">High (Default)</option>
                            <option value="low">Low</option>
                        </select>
                        <button class="btn btn-primary" onclick="testMedia()">Load Stream</button>
                    </div>
                    <div id="mediaContainer" style="margin-top: 15px;"></div>
                </div>
            </div>

            <!-- Documentation Area -->
            <div class="card">
                <h2>API Documentation</h2>

                <h3 style="margin-top:20px; font-size:1rem; color:var(--primary);">1. Audio Streaming (Increments Audio Counter)</h3>
                <div class="code-box">GET /stream/{VIDEO_ID}?type=audio&quality=high&key=YOUR_API_KEY</div>
                <p style="font-size:0.85rem; color:var(--muted); margin-top:6px;">Qualities: <code>high</code> (140 M4A 128kbps), <code>low</code> (64kbps Opus)</p>

                <h3 style="margin-top:20px; font-size:1rem; color:var(--primary);">2. Video Streaming (Increments Video Counter)</h3>
                <div class="code-box">GET /stream/{VIDEO_ID}?type=video&quality=720&key=YOUR_API_KEY</div>
                <p style="font-size:0.85rem; color:var(--muted); margin-top:6px;">Qualities: <code>720</code> (720p HD Progressive), <code>480</code> (480p), <code>360</code> (360p Fast)</p>

                <h3 style="margin-top:20px; font-size:1rem; color:var(--primary);">3. Track Search</h3>
                <div class="code-box">GET /search?query=unstoppable&limit=5&key=YOUR_API_KEY</div>
            </div>
        </div>

        <div class="modal-overlay" id="modal">
            <div class="modal">
                <h3 id="modalTitle" style="margin-bottom:15px;">Sign In</h3>
                <div id="modalErr" style="color:#ef4444; font-size:0.82rem; margin-bottom:10px; display:none;"></div>
                <input type="text" id="mUsername" placeholder="Username">
                <input type="password" id="mPassword" placeholder="Password">
                <button class="btn btn-primary" style="width:100%;" id="mSubmit" onclick="submitAuth()">Continue</button>
                <button class="btn btn-outline" style="width:100%; margin-top:8px;" onclick="closeModal()">Cancel</button>
            </div>
        </div>

        <script>
            let curMode = 'login';
            let u = localStorage.getItem('m_user');
            let k = localStorage.getItem('m_key');
            let audioCount = localStorage.getItem('m_audio_count') || 0;
            let videoCount = localStorage.getItem('m_video_count') || 0;

            function syncUI() {
                if(u && k) {
                    document.getElementById('navBtns').style.display = 'none';
                    document.getElementById('userMenu').style.display = 'flex';
                    document.getElementById('welcomeUser').innerText = 'User: ' + u;
                    document.getElementById('dashView').style.display = 'block';
                    document.getElementById('keyDisplay').innerText = k;
                    document.getElementById('audioReqCount').innerText = audioCount;
                    document.getElementById('videoReqCount').innerText = videoCount;
                } else {
                    document.getElementById('navBtns').style.display = 'flex';
                    document.getElementById('userMenu').style.display = 'none';
                    document.getElementById('dashView').style.display = 'none';
                }
            }
            syncUI();

            function openModal(mode) {
                curMode = mode;
                document.getElementById('modalTitle').innerText = mode === 'login' ? 'Sign In' : 'Create Account';
                document.getElementById('modal').style.display = 'flex';
            }
            function closeModal() { document.getElementById('modal').style.display = 'none'; }

            function updateQualityDropdown() {
                const t = document.getElementById('testType').value;
                const q = document.getElementById('testQuality');
                q.innerHTML = '';
                if(t === 'video') {
                    q.innerHTML = '<option value="720">720p HD</option><option value="480">480p SD</option><option value="360">360p Fast</option>';
                } else {
                    q.innerHTML = '<option value="high">High (128kbps)</option><option value="low">Low (64kbps)</option>';
                }
            }

            async function submitAuth() {
                const username = document.getElementById('mUsername').value.trim();
                const password = document.getElementById('mPassword').value;
                const err = document.getElementById('modalErr');
                err.style.display = 'none';

                const endpoint = curMode === 'login' ? '/api/login' : '/api/register';
                try {
                    const res = await fetch(endpoint, {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({username, password})
                    });
                    const d = await res.json();
                    if(!res.ok) {
                        err.innerText = d.detail || 'Failed';
                        err.style.display = 'block';
                        return;
                    }
                    u = username; k = d.api_key;
                    audioCount = d.audio_requests || 0;
                    videoCount = d.video_requests || 0;
                    localStorage.setItem('m_user', u);
                    localStorage.setItem('m_key', k);
                    localStorage.setItem('m_audio_count', audioCount);
                    localStorage.setItem('m_video_count', videoCount);
                    closeModal();
                    syncUI();
                } catch(e) {
                    err.innerText = 'Network connection failed.';
                    err.style.display = 'block';
                }
            }

            async function resetKey() {
                const password = prompt('Enter your password to generate a new key:');
                if(!password) return;
                const res = await fetch('/api/reset-key', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({username: u, password})
                });
                const d = await res.json();
                if(res.ok) {
                    k = d.api_key;
                    localStorage.setItem('m_key', k);
                    syncUI();
                    alert('Key reset successful.');
                } else {
                    alert(d.detail || 'Reset failed');
                }
            }

            function testMedia() {
                const vid = document.getElementById('testVidId').value.trim();
                const type = document.getElementById('testType').value;
                const qual = document.getElementById('testQuality').value;
                if(!vid) return alert('Enter a Video ID');

                const container = document.getElementById('mediaContainer');
                const src = `/stream/${vid}?type=${type}&quality=${qual}&key=${k}`;

                if(type === 'video') {
                    container.innerHTML = `<video controls autoplay src="${src}"></video>`;
                    videoCount++;
                    document.getElementById('videoReqCount').innerText = videoCount;
                    localStorage.setItem('m_video_count', videoCount);
                } else {
                    container.innerHTML = `<audio controls autoplay src="${src}" style="filter: invert(0.85) hue-rotate(180deg);"></audio>`;
                    audioCount++;
                    document.getElementById('audioReqCount').innerText = audioCount;
                    localStorage.setItem('m_audio_count', audioCount);
                }
            }

            function copyKey() {
                navigator.clipboard.writeText(k);
                alert('Copied to clipboard');
            }

            function logout() {
                localStorage.clear();
                u = null; k = null; audioCount = 0; videoCount = 0;
                syncUI();
            }
        </script>
    </body>
    </html>
