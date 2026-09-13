import os
import json
import sqlite3
import secrets
import asyncio
import aiohttp
from hashlib import sha256
from cachetools import TTLCache
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
import yt_dlp

app = FastAPI(title="MusicAPI Pro Stream Engine")

DB_FILE = "users.db"
URL_CACHE = TTLCache(maxsize=10000, ttl=14400)


def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            password_hash TEXT,
            api_key TEXT UNIQUE
        )"""
    )
    conn.commit()
    conn.close()


init_db()


def hash_pw(password: str) -> str:
    return sha256(password.encode()).hexdigest()


def get_user(username: str):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, username, password_hash, api_key FROM users WHERE username = ?", (username,))
    row = c.fetchone()
    conn.close()
    return row


def verify_api_key(key: str) -> bool:
    if not key:
        return False
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id FROM users WHERE api_key = ?", (key,))
    row = c.fetchone()
    conn.close()
    return row is not None


# Direct stream extraction without transcoding to minimize CPU/latency
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


async def get_audio_url(video_id: str) -> str:
    if video_id in URL_CACHE:
        return URL_CACHE[video_id]
    loop = asyncio.get_running_loop()
    url = await loop.run_in_executor(None, _extract, video_id)
    if url:
        URL_CACHE[video_id] = url
    return url


async def stream_raw_chunks(source_url: str, request_range: str = None):
    headers = {"User-Agent": "com.google.android.youtube/19.29.37 (Linux; U; Android 11) gzip"}
    if request_range:
        headers["Range"] = request_range

    timeout = aiohttp.ClientTimeout(total=None, sock_read=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(source_url, headers=headers) as resp:
            async for chunk in resp.content.iter_chunked(131072):
                yield chunk


# --- Authentication Endpoints ---

@app.post("/api/register")
async def register(data: dict):
    username = data.get("username", "").strip().lower()
    password = data.get("password", "")
    if len(username) < 3 or len(password) < 6:
        raise HTTPException(status_code=400, detail="Username >= 3 chars, Password >= 6 chars required.")

    api_key = f"music_{secrets.token_hex(16)}"
    pw_hash = hash_pw(password)
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("INSERT INTO users (username, password_hash, api_key) VALUES (?, ?, ?)", (username, pw_hash, api_key))
        conn.commit()
        conn.close()
        return {"status": "success", "api_key": api_key}
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Username already registered.")


@app.post("/api/login")
async def login(data: dict):
    username = data.get("username", "").strip().lower()
    password = data.get("password", "")
    user = get_user(username)
    if not user or user[2] != hash_pw(password):
        raise HTTPException(status_code=401, detail="Invalid username or password.")
    return {"status": "success", "username": user[1], "api_key": user[3]}


@app.post("/api/reset-key")
async def reset_key(data: dict):
    username = data.get("username", "").strip().lower()
    password = data.get("password", "")
    user = get_user(username)
    if not user or user[2] != hash_pw(password):
        raise HTTPException(status_code=401, detail="Authentication failed.")

    new_key = f"music_{secrets.token_hex(16)}"
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE users SET api_key = ? WHERE username = ?", (new_key, username))
    conn.commit()
    conn.close()
    return {"status": "success", "api_key": new_key}


# --- Stream Endpoint ---

@app.get("/stream/{video_id}")
async def stream(video_id: str, key: str = None, request: Request = None):
    if not verify_api_key(key):
        raise HTTPException(status_code=403, detail="Invalid or unauthorized API key.")

    try:
        audio_url = await get_audio_url(video_id)
        if not audio_url:
            raise HTTPException(status_code=404, detail="Track unavailable.")

        client_range = request.headers.get("range") if request else None

        return StreamingResponse(
            stream_raw_chunks(audio_url, client_range),
            media_type="audio/mpeg",
            headers={
                "Accept-Ranges": "bytes",
                "Cache-Control": "public, max-age=31536000, immutable",
                "Access-Control-Allow-Origin": "*",
                "X-Powered-By": "MusicAPI-CDN/2.0",
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- Complete Professional UI & Documentation ---

@app.get("/", response_class=HTMLResponse)
async def index():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>MusicAPI | Developer Portal & Streaming Engine</title>
        <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
        <style>
            :root {
                --bg: #090d16;
                --surface: #111726;
                --border: #1f293d;
                --primary: #38bdf8;
                --primary-glow: rgba(56, 189, 248, 0.15);
                --accent: #0ea5e9;
                --text: #f8fafc;
                --muted: #94a3b8;
            }
            * { margin:0; padding:0; box-sizing:border-box; font-family: 'Plus Jakarta Sans', sans-serif; }
            body { background: var(--bg); color: var(--text); min-height: 100vh; display: flex; flex-direction: column; }
            nav { border-bottom: 1px solid var(--border); padding: 18px 40px; display: flex; justify-content: space-between; align-items: center; background: rgba(17, 23, 38, 0.7); backdrop-filter: blur(12px); position: sticky; top: 0; z-index: 50; }
            .logo { font-size: 1.3rem; font-weight: 700; color: #fff; display: flex; align-items: center; gap: 8px; }
            .logo span { color: var(--primary); }
            .nav-actions { display: flex; gap: 12px; }
            .btn { padding: 8px 18px; border-radius: 8px; font-size: 0.9rem; font-weight: 600; cursor: pointer; transition: 0.2s; border: none; }
            .btn-outline { background: transparent; border: 1px solid var(--border); color: #fff; }
            .btn-outline:hover { background: var(--surface); border-color: var(--primary); }
            .btn-primary { background: var(--accent); color: #fff; }
            .btn-primary:hover { opacity: 0.9; }

            .hero { text-align: center; padding: 70px 20px 40px; max-width: 800px; margin: 0 auto; }
            .badge { display: inline-block; padding: 6px 14px; background: var(--primary-glow); color: var(--primary); border-radius: 50px; font-size: 0.8rem; font-weight: 600; margin-bottom: 16px; border: 1px solid rgba(56, 189, 248, 0.3); }
            .hero h1 { font-size: 2.8rem; font-weight: 800; line-height: 1.2; margin-bottom: 16px; }
            .hero p { color: var(--muted); font-size: 1.1rem; line-height: 1.6; }

            .container { max-width: 1080px; margin: 0 auto; padding: 20px; width: 100%; display: grid; grid-template-columns: 1fr; gap: 30px; }

            /* Dashboard & Cards */
            .card { background: var(--surface); border: 1px solid var(--border); border-radius: 14px; padding: 30px; box-shadow: 0 10px 30px rgba(0,0,0,0.3); }
            .input-group { margin-bottom: 16px; }
            .input-group label { display: block; font-size: 0.85rem; color: var(--muted); margin-bottom: 6px; font-weight: 500; }
            input { width: 100%; padding: 12px 14px; background: #0b111e; border: 1px solid var(--border); border-radius: 8px; color: #fff; font-size: 0.95rem; }
            input:focus { outline: none; border-color: var(--primary); }

            /* Code Documentation */
            pre, code { font-family: 'JetBrains Mono', monospace; }
            .code-box { background: #070a12; border: 1px solid var(--border); border-radius: 8px; padding: 18px; margin-top: 14px; position: relative; overflow-x: auto; font-size: 0.88rem; color: #38bdf8; line-height: 1.5; }
            .doc-section { margin-top: 40px; }
            .doc-section h2 { font-size: 1.4rem; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }
            .endpoint-tag { font-family: 'JetBrains Mono'; background: #0284c7; color: white; padding: 3px 8px; border-radius: 4px; font-size: 0.8rem; }
            
            /* Modal Auth */
            .modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,0.7); display: none; align-items: center; justify-content: center; z-index: 100; backdrop-filter: blur(4px); }
            .modal { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 30px; width: 90%; max-width: 400px; }
            .modal h3 { font-size: 1.2rem; margin-bottom: 16px; }
            .tabs { display: flex; gap: 10px; margin-bottom: 20px; border-bottom: 1px solid var(--border); padding-bottom: 10px; }
            .tab-btn { background: none; border: none; color: var(--muted); font-weight: 600; cursor: pointer; }
            .tab-btn.active { color: var(--primary); border-bottom: 2px solid var(--primary); }

            /* Dashboard View */
            #dashView { display: none; }
            .api-key-display { background: #080d1a; border: 1px dashed var(--primary); padding: 14px; border-radius: 8px; display: flex; justify-content: space-between; align-items: center; margin: 15px 0; font-family: 'JetBrains Mono'; color: #38bdf8; font-size: 0.95rem; word-break: break-all; }
        </style>
    </head>
    <body>
        <nav>
            <div class="logo">⚡ Music<span>API</span></div>
            <div class="nav-actions" id="navBtns">
                <button class="btn btn-outline" onclick="openAuth('login')">Sign In</button>
                <button class="btn btn-primary" onclick="openAuth('register')">Get API Key</button>
            </div>
            <div class="nav-actions" id="userMenu" style="display:none;">
                <span id="welcomeUser" style="margin-right:10px; font-size:0.9rem; color:var(--muted); align-self:center;"></span>
                <button class="btn btn-outline" onclick="logout()">Logout</button>
            </div>
        </nav>

        <div class="hero">
            <div class="badge">TURBO AUDIO CDN 2.0</div>
            <h1>High-Speed YouTube Audio Streaming API</h1>
            <p>Direct low-latency chunk piping designed for Telegram bots, web apps, and media players. Bypass bot detection natively with edge caching.</p>
        </div>

        <div class="container">
            <!-- User Dashboard Panel (Shows when Logged In) -->
            <div class="card" id="dashView">
                <h2>Developer Dashboard</h2>
                <p style="color:var(--muted); font-size:0.95rem; margin-top:4px;">Manage your access credentials and live usage endpoint.</p>
                <div class="api-key-display">
                    <span id="userKeyText">music_xxxxxxxxxxxx</span>
                    <button class="btn btn-primary" style="padding:6px 12px;" onclick="copyKey()">Copy Key</button>
                </div>
                <div style="display:flex; gap:10px;">
                    <button class="btn btn-outline" onclick="resetKeyAction()">Generate New Key</button>
                </div>
            </div>

            <!-- Documentation Area -->
            <div class="card">
                <h2>API Documentation</h2>
                <p style="color:var(--muted); font-size:0.95rem;">Authenticate by passing your key as a query parameter.</p>

                <div class="doc-section">
                    <h3><span class="endpoint-tag">GET</span> /stream/{video_id}</h3>
                    <p style="color:var(--muted); font-size:0.9rem; margin-top: 6px;">Streams raw audio chunks directly without saving files to disk.</p>
                    
                    <div class="code-box">
                        <b>Direct URL:</b><br>
                        https://YOUR-SITE-URL/stream/<span style="color:#f59e0b;">VIDEO_ID</span>?key=<span style="color:#10b981;">YOUR_API_KEY</span>
                    </div>

                    <h4 style="margin-top:20px; font-size:0.95rem;">cURL Verification:</h4>
                    <div class="code-box">
curl -I "https://YOUR-SITE-URL/stream/dQw4w9WgXcQ?key=YOUR_API_KEY"
                    </div>

                    <h4 style="margin-top:20px; font-size:0.95rem;">Pyrogram / Py-TgCalls Integration:</h4>
                    <div class="code-box">
stream_url = f"https://YOUR-SITE-URL/stream/{video_id}?key={API_KEY}"<br>
await app.play(chat_id, MediaStream(stream_url))
                    </div>
                </div>
            </div>
        </div>

        <!-- Auth Modal (Login / Register) -->
        <div class="modal-overlay" id="authModal">
            <div class="modal">
                <div class="tabs">
                    <button class="tab-btn active" id="tabLogin" onclick="switchTab('login')">Sign In</button>
                    <button class="tab-btn" id="tabReg" onclick="switchTab('reg')">Create Account</button>
                </div>
                <div id="modalErr" style="color:#ef4444; font-size:0.85rem; margin-bottom:10px; display:none;"></div>
                <div class="input-group">
                    <label>Username</label>
                    <input type="text" id="authUsername" placeholder="Enter username">
                </div>
                <div class="input-group">
                    <label>Password</label>
                    <input type="password" id="authPassword" placeholder="Enter password (min 6 chars)">
                </div>
                <button class="btn btn-primary" style="width:100%; margin-top:10px;" id="authSubmit" onclick="handleAuth()">Sign In</button>
                <button class="btn btn-outline" style="width:100%; margin-top:8px;" onclick="closeAuth()">Cancel</button>
            </div>
        </div>

        <script>
            let currentTab = 'login';
            let activeUser = localStorage.getItem('music_user');
            let activeKey = localStorage.getItem('music_key');

            function updateUI() {
                if (activeUser && activeKey) {
                    document.getElementById('navBtns').style.display = 'none';
                    document.getElementById('userMenu').style.display = 'flex';
                    document.getElementById('welcomeUser').innerText = 'Account: ' + activeUser;
                    document.getElementById('dashView').style.display = 'block';
                    document.getElementById('userKeyText').innerText = activeKey;
                } else {
                    document.getElementById('navBtns').style.display = 'flex';
                    document.getElementById('userMenu').style.display = 'none';
                    document.getElementById('dashView').style.display = 'none';
                }
            }
            updateUI();

            function openAuth(tab) {
                switchTab(tab);
                document.getElementById('authModal').style.display = 'flex';
            }
            function closeAuth() {
                document.getElementById('authModal').style.display = 'none';
            }
            function switchTab(tab) {
                currentTab = tab;
                document.getElementById('modalErr').style.display = 'none';
                if(tab === 'login') {
                    document.getElementById('tabLogin').classList.add('active');
                    document.getElementById('tabReg').classList.remove('active');
                    document.getElementById('authSubmit').innerText = 'Sign In';
                } else {
                    document.getElementById('tabReg').classList.add('active');
                    document.getElementById('tabLogin').classList.remove('active');
                    document.getElementById('authSubmit').innerText = 'Register & Get Key';
                }
            }

            async function handleAuth() {
                const u = document.getElementById('authUsername').value.trim();
                const p = document.getElementById('authPassword').value;
                const err = document.getElementById('modalErr');
                err.style.display = 'none';

                const endpoint = currentTab === 'login' ? '/api/login' : '/api/register';
                try {
                    const res = await fetch(endpoint, {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({username: u, password: p})
                    });
                    const data = await res.json();
                    if(!res.ok) {
                        err.innerText = data.detail || 'Authentication failed';
                        err.style.display = 'block';
                        return;
                    }
                    activeUser = u;
                    activeKey = data.api_key;
                    localStorage.setItem('music_user', activeUser);
                    localStorage.setItem('music_key', activeKey);
                    closeAuth();
                    updateUI();
                } catch(e) {
                    err.innerText = 'Network connection failed.';
                    err.style.display = 'block';
                }
            }

            async function resetKeyAction() {
                const p = prompt("Re-enter your password to generate a new key:");
                if(!p) return;
                const res = await fetch('/api/reset-key', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({username: activeUser, password: p})
                });
                const data = await res.json();
                if(res.ok) {
                    activeKey = data.api_key;
                    localStorage.setItem('music_key', activeKey);
                    updateUI();
                    alert("Your API key has been regenerated successfully!");
                } else {
                    alert(data.detail || "Failed to reset key");
                }
            }

            function copyKey() {
                navigator.clipboard.writeText(activeKey);
                alert("Copied API key to clipboard!");
            }

            function logout() {
                localStorage.removeItem('music_user');
                localStorage.removeItem('music_key');
                activeUser = null;
                activeKey = null;
                updateUI();
            }
        </script>
    </body>
    </html>
    """
                                
