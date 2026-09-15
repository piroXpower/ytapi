import os
import gc
import asyncio
from aiohttp import web
from pyrogram import Client
from pytgcalls import PyTgCalls
from pytgcalls.types import MediaStream

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID", "24091097"))
API_HASH = os.environ.get("API_HASH", "4478b9c13824b7ece40de4586f4c796a")
PORT = int(os.environ.get("PORT", 8080))
BRIDGE_SECRET = os.environ.get("BRIDGE_SECRET", "Adnet@123")

# Sessions for 5 assistants (Session 1 configured, 2-5 optional via environment)
SESSIONS = [
    os.environ.get(
        "STRING_SESSION1",
        "BQFvmdkAKPsfcTKsXjhIq7GtKjuBzbCX98byCnCfOM4kbH2AMsuPe9J65Sc4Pt0gBofWzFM5JYDPdlzmfsU8fJcBzYrpqxZpWpV0va_MREITGIfL2xjO6Kh2wCNfTyAJu-osCbom1kemL5I-Ais_hE-2wlFxyJvNYiXNJ2Xmy1iiz0_Bu5KJpTWHCIyYvMeQypE0c_BR8wpaRICJJ1mx71pzTNOLAI2GKtUPqKaev_ml4AcurNAIf9QT3hpwX_VNqx3qKq98z50QQkOUPj3S1CftP9RXjdoVk2H4LhrDMe6-ZVppybNMNJlh3E8tVHUS1MTRvtwKNDQ1-ZJnjvBR5rGeqShbIwAAAAH6uplCAA"
    ),
    os.environ.get("STRING_SESSION2", ""),
    os.environ.get("STRING_SESSION3", ""),
    os.environ.get("STRING_SESSION4", ""),
    os.environ.get("STRING_SESSION5", ""),
]

ACTIVE_SESSIONS = [s.strip() for s in SESSIONS if s.strip()]

assistants = []
call_apps = []
chat_worker_map = {}

routes = web.RouteTableDef()


def check_auth(request: web.Request) -> bool:
    return request.headers.get("X-Bridge-Token") == BRIDGE_SECRET


def get_least_busy_client_idx() -> int:
    """Find the assistant with the fewest active voice calls."""
    active_counts = [len(c.active_calls) for c in call_apps]
    min_val = min(active_counts)
    return active_counts.index(min_val)


@routes.get("/status")
async def handle_status(request: web.Request):
    report = []
    total = 0
    for idx, c in enumerate(call_apps, 1):
        count = len(c.active_calls)
        total += count
        report.append({"assistant": idx, "active_vcs": count})
    return web.json_response({
        "status": "online",
        "total_vcs": total,
        "assistants": report
    })


@routes.post("/play")
async def handle_play(request: web.Request):
    if not check_auth(request):
        return web.json_response({"error": "Unauthorized"}, status=401)

    try:
        data = await request.json()
        chat_id = int(data["chat_id"])
        link = data["link"]
    except Exception as e:
        return web.json_response({"error": f"Invalid payload: {str(e)}"}, status=400)

    # Route to existing assistant if already active, else select least busy
    if chat_id in chat_worker_map:
        idx = chat_worker_map[chat_id]
    else:
        idx = get_least_busy_client_idx()
        chat_worker_map[chat_id] = idx

    call = call_apps[idx]
    try:
        await call.play(
            chat_id,
            MediaStream(
                link,
                audio_parameters=MediaStream.AudioQuality.LOW
            )
        )
        return web.json_response({"status": "playing", "assistant": idx + 1, "chat_id": chat_id})
    except Exception as e:
        chat_worker_map.pop(chat_id, None)
        return web.json_response({"status": "error", "message": str(e)}, status=500)


@routes.post("/pause")
async def handle_pause(request: web.Request):
    if not check_auth(request):
        return web.json_response({"error": "Unauthorized"}, status=401)

    data = await request.json()
    chat_id = int(data["chat_id"])
    idx = chat_worker_map.get(chat_id)
    if idx is None:
        return web.json_response({"status": "error", "message": "Chat not active"}, status=404)

    try:
        await call_apps[idx].pause_stream(chat_id)
        return web.json_response({"status": "paused", "chat_id": chat_id})
    except Exception as e:
        return web.json_response({"status": "error", "message": str(e)}, status=500)


@routes.post("/resume")
async def handle_resume(request: web.Request):
    if not check_auth(request):
        return web.json_response({"error": "Unauthorized"}, status=401)

    data = await request.json()
    chat_id = int(data["chat_id"])
    idx = chat_worker_map.get(chat_id)
    if idx is None:
        return web.json_response({"status": "error", "message": "Chat not active"}, status=404)

    try:
        await call_apps[idx].resume_stream(chat_id)
        return web.json_response({"status": "resumed", "chat_id": chat_id})
    except Exception as e:
        return web.json_response({"status": "error", "message": str(e)}, status=500)


@routes.post("/stop")
async def handle_stop(request: web.Request):
    if not check_auth(request):
        return web.json_response({"error": "Unauthorized"}, status=401)

    data = await request.json()
    chat_id = int(data["chat_id"])
    idx = chat_worker_map.pop(chat_id, None)
    if idx is not None:
        try:
            await call_apps[idx].leave_call(chat_id)
        except Exception:
            pass
        finally:
            gc.collect()

    return web.json_response({"status": "stopped", "chat_id": chat_id})


async def main():
    if not ACTIVE_SESSIONS:
        raise RuntimeError("No active STRING_SESSION found.")

    print(f"Starting {len(ACTIVE_SESSIONS)} assistant client(s)...")
    for i, session in enumerate(ACTIVE_SESSIONS):
        cli = Client(
            f"koyeb_assistant_{i+1}",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=session,
            in_memory=True
        )
        call = PyTgCalls(cli)
        await cli.start()
        await call.start()
        assistants.append(cli)
        call_apps.append(call)
        print(f"Assistant {i+1} successfully booted and connected.")

    app = web.Application()
    app.add_routes(routes)
    return app


if __name__ == "__main__":
    web.run_app(main(), host="0.0.0.0", port=PORT)
        
