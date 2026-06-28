"""
flask web panel — browser-based gui for the channel cloner.
runs telethon in a background asyncio loop.
"""

import asyncio
import json
import logging
import signal
import threading
from queue import Queue, Empty

from flask import Flask, render_template, request, jsonify, Response

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel

from config import (
    API_ID, API_HASH, PHONE, SESSION_FILE, SESSION_STRING, WEB_HOST, WEB_PORT,
    NOTIFY_ON_ERROR, NOTIFY_ON_COMPLETE,
)
from tracker import create_tracker
from cloner import clone_channel, LiveForwarder, start_live_forward, stop_live_forward

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("telethon").setLevel(logging.WARNING)
log = logging.getLogger("web")

app = Flask(__name__)

# -- telethon client lifecycle --
_loop: asyncio.AbstractEventLoop = None
_client: TelegramClient = None
_thread: threading.Thread = None

# -- clone job state --
_job_lock = threading.Lock()
_stop_event: asyncio.Event = None
_progress_queues: list[Queue] = []
_current_job = {"running": False, "stats": None, "last_stats": None}

# -- live forwarding state --
_forwarder = LiveForwarder()
_forward_lock = threading.Lock()
_forward_progress_queues: list[Queue] = []


def _run_async(coro):
    """schedule a coroutine on the telethon loop and wait for result."""
    return asyncio.run_coroutine_threadsafe(coro, _loop).result()


def _start_telethon():
    """boot the asyncio loop + telethon client in a daemon thread."""
    global _loop, _client, _thread

    _loop = asyncio.new_event_loop()

    def _worker():
        asyncio.set_event_loop(_loop)
        _loop.run_forever()

    _thread = threading.Thread(target=_worker, daemon=True)
    _thread.start()

    # use StringSession if SESSION_STRING is set (server/Render),
    # otherwise fall back to file-based session (local dev)
    if SESSION_STRING:
        log.info("using StringSession from SESSION_STRING env var")
        session = StringSession(SESSION_STRING)
    else:
        log.info("using file-based session: %s", SESSION_FILE)
        session = SESSION_FILE

    _client = TelegramClient(session, API_ID, API_HASH, loop=_loop)

    async def _do_connect():
        await _client.connect()
        if not await _client.is_user_authorized():
            raise RuntimeError(
                "Telegram session is not authorized. "
                "SESSION_STRING is missing, invalid, or expired — "
                "generate a new one and update the SESSION_STRING env var."
            )

    _run_async(_do_connect())

    me = _run_async(_client.get_me())
    if me:
        log.info("telethon connected as %s (@%s)", me.first_name, me.username)
    else:
        log.info("telethon connected, but get_me() returned no user")


async def _notify_self(text: str):
    """send a message to Saved Messages so the user knows what happened."""
    try:
        await _client.send_message("me", f"**[rogue-helix]** {text}")
    except Exception as e:
        log.error(f"failed to send self-notification: {e}")


def _broadcast_progress(stats: dict):
    """push progress update to all connected SSE clients."""
    _current_job["last_stats"] = stats
    for q in _progress_queues[:]:
        try:
            q.put_nowait(stats)
        except Exception:
            pass


def _broadcast_forward_progress(stats: dict):
    """push live-forwarding progress to all connected SSE clients."""
    for q in _forward_progress_queues[:]:
        try:
            q.put_nowait(stats)
        except Exception:
            pass


# -- routes --

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    try:
        me = _run_async(_client.get_me())
        logged_in = True
        user_info = {"name": me.first_name, "username": me.username}
    except Exception:
        logged_in = False
        user_info = None

    tracker = create_tracker()
    return jsonify({
        "logged_in": logged_in,
        "user": user_info,
        "job": _current_job,
        "tracker": tracker.get_stats(),
    })


@app.route("/api/channels")
def api_channels():
    async def _fetch():
        channels = []
        async for dialog in _client.iter_dialogs():
            if isinstance(dialog.entity, Channel):
                channels.append({
                    "id": dialog.entity.id,
                    "title": dialog.name,
                    "members": dialog.entity.participants_count,
                    "username": getattr(dialog.entity, "username", None),
                })
        return channels

    try:
        channels = _run_async(_fetch())
        return jsonify({"channels": channels})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/clone/start", methods=["POST"])
def api_clone_start():
    global _stop_event

    with _job_lock:
        if _current_job["running"]:
            return jsonify({"error": "a clone job is already running"}), 409

        data = request.get_json(force=True)
        source = data.get("source")
        dest = data.get("dest")
        since_hours_raw = data.get("since_hours")

        if not source or not dest:
            return jsonify({"error": "source and dest are required"}), 400

        if str(source) == str(dest):
            return jsonify({"error": "source and dest can't be the same"}), 400

        since_hours = None
        if since_hours_raw not in (None, "", 0, "0"):
            try:
                since_hours = float(since_hours_raw)
                if since_hours <= 0:
                    since_hours = None
            except (ValueError, TypeError):
                return jsonify({"error": "since_hours must be a number"}), 400

        _current_job["running"] = True
        _current_job["stats"] = None
        _stop_event = asyncio.Event()

    def _run_clone():
        try:
            tracker = create_tracker()

            try:
                source_id = int(source)
            except (ValueError, TypeError):
                source_id = source
            try:
                dest_id = int(dest)
            except (ValueError, TypeError):
                dest_id = dest

            stats = _run_async(clone_channel(
                _client,
                source_id,
                dest_id,
                tracker,
                progress_callback=_broadcast_progress,
                stop_event=_stop_event,
                since_hours=since_hours,
            ))

            _current_job["stats"] = stats
            _broadcast_progress(stats)
            log.info(f"clone finished: {stats}")

            if NOTIFY_ON_COMPLETE:
                summary = (
                    f"clone finished\n"
                    f"cloned: {stats.get('cloned', 0)} | "
                    f"skipped: {stats.get('skipped', 0)} | "
                    f"failed: {stats.get('failed', 0)} / {stats.get('total', 0)}"
                )
                _run_async(_notify_self(summary))
        except Exception as e:
            error_stats = {"status": "error", "error": str(e)}
            _current_job["stats"] = error_stats
            _broadcast_progress(error_stats)
            log.error(f"clone error: {e}")

            if NOTIFY_ON_ERROR:
                _run_async(_notify_self(f"crashed: {e}"))
        finally:
            _current_job["running"] = False

    threading.Thread(target=_run_clone, daemon=True).start()
    return jsonify({"message": "clone started"})


@app.route("/api/clone/stop", methods=["POST"])
def api_clone_stop():
    if not _current_job["running"]:
        return jsonify({"error": "no job running"}), 404

    if _stop_event:
        _loop.call_soon_threadsafe(_stop_event.set)

    return jsonify({"message": "stop signal sent"})


@app.route("/api/clone/progress")
def api_clone_progress():
    """SSE endpoint — streams progress events to the browser."""
    q = Queue()
    _progress_queues.append(q)

    def stream():
        try:
            while True:
                try:
                    stats = q.get(timeout=30)
                    yield f"data: {json.dumps(stats)}\n\n"
                except Empty:
                    yield f"data: {json.dumps({'heartbeat': True})}\n\n"
        except GeneratorExit:
            pass
        finally:
            if q in _progress_queues:
                _progress_queues.remove(q)

    return Response(stream(), mimetype="text/event-stream")


@app.route("/api/forward/start", methods=["POST"])
def api_forward_start():
    with _forward_lock:
        if _forwarder.running:
            return jsonify({"error": "live forwarding is already running"}), 409

        data = request.get_json(force=True)
        sources = data.get("sources")
        dest = data.get("dest")

        if not sources or not isinstance(sources, list):
            return jsonify({"error": "sources must be a non-empty list"}), 400
        if not dest:
            return jsonify({"error": "dest is required"}), 400
        if len(sources) > 30:
            return jsonify({"error": "max 30 source channels supported"}), 400

        try:
            source_ids = [int(s) for s in sources]
        except (ValueError, TypeError):
            return jsonify({"error": "all source ids must be numeric"}), 400

        try:
            dest_id = int(dest)
        except (ValueError, TypeError):
            dest_id = dest

        if dest_id in source_ids:
            return jsonify({"error": "destination can't also be a source"}), 400

        try:
            tracker = create_tracker()
            _run_async(start_live_forward(
                _client,
                source_ids,
                dest_id,
                tracker,
                _forwarder,
                progress_callback=_broadcast_forward_progress,
            ))
        except Exception as e:
            return jsonify({"error": str(e)}), 500

        # save config so it auto-restarts on next boot
        try:
            from cloner import ForwardStateStore
            state_store = ForwardStateStore()
            state_store.save_config(source_ids, dest_id if isinstance(dest_id, int) else 0)
        except Exception as e:
            log.warning("could not save forward config: %s", e)

    return jsonify({"message": "live forwarding started", "sources": source_ids})


@app.route("/api/forward/stop", methods=["POST"])
def api_forward_stop():
    with _forward_lock:
        if not _forwarder.running:
            return jsonify({"error": "live forwarding isn't running"}), 404
        _run_async(asyncio.sleep(0))  # ensure loop context
        stop_live_forward(_client, _forwarder)

    return jsonify({"message": "live forwarding stopped"})


@app.route("/api/forward/status")
def api_forward_status():
    return jsonify({
        "running": _forwarder.running,
        "stats": _forwarder.stats,
    })


@app.route("/api/forward/progress")
def api_forward_progress():
    """SSE endpoint — streams live-forwarding progress to the browser."""
    q = Queue()
    _forward_progress_queues.append(q)

    def stream():
        try:
            while True:
                try:
                    stats = q.get(timeout=30)
                    yield f"data: {json.dumps(stats)}\n\n"
                except Empty:
                    yield f"data: {json.dumps({'heartbeat': True})}\n\n"
        except GeneratorExit:
            pass
        finally:
            if q in _forward_progress_queues:
                _forward_progress_queues.remove(q)

    return Response(stream(), mimetype="text/event-stream")


# -- graceful shutdown --

def _shutdown_handler(signum, frame):
    sig_name = signal.Signals(signum).name
    log.info(f"got {sig_name}, shutting down gracefully...")

    if _current_job["running"] and _stop_event:
        _loop.call_soon_threadsafe(_stop_event.set)
        if NOTIFY_ON_ERROR:
            try:
                _run_async(_notify_self(f"shutdown triggered ({sig_name}), stopping current clone"))
            except Exception:
                pass


for _sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(_sig, _shutdown_handler)


# -- boot --

with app.app_context():
    if API_ID and API_HASH:
        _start_telethon()
        # auto-start live forwarding if a config was saved from the previous run
        try:
            from cloner import ForwardStateStore
            state_store = ForwardStateStore()
            saved = state_store.load_config()
            if saved:
                log.info(
                    "auto-starting live forward from saved config: %d sources -> dest %s",
                    len(saved["source_ids"]), saved["dest_id"],
                )
                tracker = create_tracker()
                _run_async(start_live_forward(
                    _client,
                    saved["source_ids"],
                    saved["dest_id"],
                    tracker,
                    _forwarder,
                    progress_callback=_broadcast_forward_progress,
                ))
            else:
                log.info("no saved forward config found — waiting for manual start")
        except Exception as e:
            log.warning("auto-start failed: %s — manual start required", e)
    else:
        log.warning("API_ID/API_HASH not set — telethon won't connect")


if __name__ == "__main__":
    app.run(host=WEB_HOST, port=WEB_PORT, debug=False, threaded=True)
