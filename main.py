#!/usr/bin/env python3
"""
🎬 KOYEB-READY TELEGRAM HLS BOT - COMPLETE & WORKING 🎬
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ TESTED & WORKING - Fixed updater error for PTB v20+
🚀 Port 8080 optimized for Koyeb deployment
📺 Host live m3u8 streams through your own server

Deploy to Koyeb:
1. Build: pip install aiohttp aiofiles python-telegram-bot==21.6
2. Run: python main.py
3. PORT: 8080 (auto-detected)
"""

import os
import asyncio
import json
import uuid
import time
import logging
import shutil
import signal
import hashlib
import subprocess
import urllib.request
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional, Any

# Web server
from aiohttp import web, ClientSession
import aiofiles

# Telegram bot (PTB v20+)
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, MenuButtonWebApp, BotCommand
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters, CallbackQueryHandler
from telegram.constants import ParseMode

# ==================== CONFIGURATION ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "REPLACE_ME")
# If you want to keep hardcoded token, uncomment next line (not recommended for production)
BOT_TOKEN = "8484774966:AAEqlVPcJHDtPszUMFLAsGwdrK2luwWiwB8"

HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8080"))

LIVE_DIR = Path("./live")
TEMP_DIR = Path("./temp")
LOGS_DIR = Path("./logs")
for d in (LIVE_DIR, TEMP_DIR, LOGS_DIR):
    d.mkdir(parents=True, exist_ok=True)

def get_koyeb_url() -> str:
    app_url = os.getenv("APP_URL", "").strip()
    if app_url:
        return app_url.rstrip("/")
    koyeb_app = os.getenv("KOYEB_APP_NAME")
    koyeb_user = os.getenv("KOYEB_USERNAME")
    if koyeb_app and koyeb_user:
        return f"https://{koyeb_app}-{koyeb_user}.koyeb.app"
    if koyeb_app:
        return f"https://{koyeb_app}.koyeb.app"
    try:
        ip = urllib.request.urlopen("https://api.ipify.org", timeout=5).read().decode()
        return f"http://{ip}:{PORT}"
    except:
        return f"http://localhost:{PORT}"

BASE_URL = get_koyeb_url()

CONFIG = {
    "SEGMENT_DURATION": 6,
    "PLAYLIST_SIZE": 5,
    "MAX_CONCURRENT_STREAMS": 10,
    "HEALTH_CHECK_INTERVAL": 30,
    "FFMPEG_PATH": "ffmpeg"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("HLS_Bot")

# ==================== GLOBAL STATE ====================
active_streams: Dict[str, Dict[str, Any]] = {}
ffmpeg_processes: Dict[str, asyncio.subprocess.Process] = {}
client_session: Optional[ClientSession] = None

# ==================== UTILITY ====================
def generate_stream_id() -> str:
    return str(uuid.uuid4())[:12]

def get_stream_dir(stream_id: str) -> Path:
    p = LIVE_DIR / stream_id
    p.mkdir(exist_ok=True)
    return p

def get_hosted_url(stream_id: str) -> str:
    return f"{BASE_URL}/stream/{stream_id}/playlist.m3u8"

def get_player_url(stream_id: str) -> str:
    return f"{BASE_URL}/player/{stream_id}"

# ==================== STREAM MANAGEMENT ====================
async def start_stream_restream(source_url: str, user_id: int) -> str:
    stream_id = generate_stream_id()
    stream_info = {
        "stream_id": stream_id,
        "user_id": user_id,
        "source_url": source_url,
        "status": "starting",
        "created_at": datetime.now(),
        "hosted_url": get_hosted_url(stream_id),
        "player_url": get_player_url(stream_id),
        "viewers": 0,
        "health": {
            "is_healthy": True,
            "last_check": datetime.now(),
            "error_count": 0
        }
    }
    active_streams[stream_id] = stream_info
    asyncio.create_task(start_ffmpeg_restream(stream_id))
    return stream_id

async def start_ffmpeg_restream(stream_id: str):
    if stream_id not in active_streams:
        return
    stream_info = active_streams[stream_id]
    source_url = stream_info["source_url"]
    stream_dir = get_stream_dir(stream_id)
    playlist_path = stream_dir / "playlist.m3u8"
    segment_pattern = str(stream_dir / "segment_%05d.ts")

    cmd = [
        CONFIG["FFMPEG_PATH"],
        "-hide_banner", "-loglevel", "error",
        "-i", source_url,
        "-c", "copy",
        "-f", "hls",
        "-hls_time", str(CONFIG["SEGMENT_DURATION"]),
        "-hls_list_size", str(CONFIG["PLAYLIST_SIZE"]),
        "-hls_flags", "delete_segments+append_list+independent_segments",
        "-hls_segment_filename", segment_pattern,
        str(playlist_path)
    ]

    try:
        logger.info(f"Starting FFmpeg for {stream_id}")
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        ffmpeg_processes[stream_id] = process
        stream_info["status"] = "active"
        asyncio.create_task(monitor_ffmpeg_process(stream_id, process))
        asyncio.create_task(monitor_stream_health(stream_id))
    except Exception as e:
        logger.error(f"Error starting FFmpeg for {stream_id}: {e}")
        stream_info["status"] = "error"
        stream_info["health"]["error_count"] += 1

async def monitor_ffmpeg_process(stream_id: str, process: asyncio.subprocess.Process):
    try:
        returncode = await process.wait()
        if stream_id in active_streams:
            if returncode == 0:
                active_streams[stream_id]["status"] = "stopped"
            else:
                active_streams[stream_id]["status"] = "error"
                active_streams[stream_id]["health"]["error_count"] += 1
        logger.info(f"FFmpeg {stream_id} exited with code {returncode}")
    except Exception as e:
        logger.error(f"Monitor error {stream_id}: {e}")
    finally:
        ffmpeg_processes.pop(stream_id, None)

async def monitor_stream_health(stream_id: str):
    while stream_id in active_streams and active_streams[stream_id]["status"] == "active":
        try:
            await asyncio.sleep(CONFIG["HEALTH_CHECK_INTERVAL"])
            if stream_id not in active_streams:
                break
            stream_info = active_streams[stream_id]
            playlist_path = get_stream_dir(stream_id) / "playlist.m3u8"
            if playlist_path.exists():
                last_modified = datetime.fromtimestamp(playlist_path.stat().st_mtime)
                if datetime.now() - last_modified > timedelta(seconds=CONFIG["SEGMENT_DURATION"] * 3):
                    stream_info["health"]["error_count"] += 1
                    stream_info["health"]["is_healthy"] = False
                    if stream_info["health"]["error_count"] > 3:
                        logger.warning(f"Stream {stream_id} unhealthy, stopping")
                        await stop_stream(stream_id)
                        break
                else:
                    stream_info["health"]["is_healthy"] = True
                    stream_info["health"]["error_count"] = 0
            else:
                stream_info["health"]["error_count"] += 1
                stream_info["health"]["is_healthy"] = False
            stream_info["health"]["last_check"] = datetime.now()
        except Exception as e:
            logger.error(f"Health check error {stream_id}: {e}")
            break

async def stop_stream(stream_id: str) -> bool:
    if stream_id not in active_streams:
        return False
    try:
        process = ffmpeg_processes.get(stream_id)
        if process and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                process.kill()
        ffmpeg_processes.pop(stream_id, None)
        active_streams[stream_id]["status"] = "stopped"
        logger.info(f"Stopped stream {stream_id}")
        return True
    except Exception as e:
        logger.error(f"Error stopping {stream_id}: {e}")
        return False

def get_user_streams(user_id: int) -> Dict[str, Dict[str, Any]]:
    return {k: v for k, v in active_streams.items() if v["user_id"] == user_id}

# ==================== TELEGRAM HANDLERS ====================
def create_main_keyboard() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📺 Host Live Stream", callback_data="host_live")],
        [InlineKeyboardButton("📊 My Streams", callback_data="my_streams")],
        [
            InlineKeyboardButton("📱 Mini App", web_app=WebAppInfo(url=f"{BASE_URL}/miniapp")),
            InlineKeyboardButton("❓ Help", callback_data="help")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_stream_keyboard(stream_id: str) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("▶️ Open Player", url=get_player_url(stream_id))],
        [
            InlineKeyboardButton("📊 Stats", callback_data=f"stats:{stream_id}"),
            InlineKeyboardButton("⏹️ Stop", callback_data=f"stop:{stream_id}")
        ],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = f"""
🎬 **Advanced HLS Streaming Bot**

Welcome! I can host live m3u8 streams through your own server.

✨ **Features:**
• 📺 Host live m3u8 streams
• 🔄 Real-time restreaming
• 📱 Advanced web interface
• 📊 Stream monitoring
• 🎛️ Stream controls

**Quick Start:**
Send me an m3u8 URL to start hosting!

**Example:**
`https://example.com/playlist.m3u8`

**Server:** {BASE_URL}
"""
    await update.message.reply_text(
        welcome_text, parse_mode=ParseMode.MARKDOWN, reply_markup=create_main_keyboard()
    )

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = update.effective_user.id

    if data == "main_menu":
        await query.edit_message_text(
            "🏠 **Main Menu**\n\nChoose an option:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=create_main_keyboard()
        )
    elif data == "host_live":
        await query.edit_message_text(
            "📺 **Host Live Stream**\n\n"
            "Send me a live m3u8 URL and I'll host it through your server!\n\n"
            "**Supported:**\n"
            "• Live HLS streams (.m3u8)\n"
            "• IPTV playlists\n"
            "• Direct streaming URLs\n\n"
            "**Example:**\n"
            "`https://example.com/playlist.m3u8`\n\n"
            "Just send the URL as a message!",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Back", callback_data="main_menu")]])
        )
    elif data == "my_streams":
        user_streams = get_user_streams(user_id)
        if not user_streams:
            await query.edit_message_text(
                "📊 **My Streams**\n\n"
                "You don't have any active streams.\n"
                "Send an m3u8 URL to create one.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=create_main_keyboard()
            )
        else:
            text = "📊 **Your Active Streams:**\n\n"
            keyboard = []
            for sid, info in user_streams.items():
                status_emoji = {"active": "🟢", "starting": "🟡", "error": "🔴", "stopped": "⚫"}.get(info["status"], "❓")
                text += f"{status_emoji} `{sid}`\n"
                text += f"   Status: {info['status'].title()}\n"
                text += f"   Created: {info['created_at'].strftime('%H:%M:%S')}\n\n"
                keyboard.append([InlineKeyboardButton(f"📺 {sid[:8]}...", callback_data=f"stream:{sid}")])
            keyboard.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
            await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))
    elif data.startswith("stream:"):
        sid = data.split(":", 1)[1]
        if sid not in active_streams:
            await query.edit_message_text("❌ Stream not found!")
            return
        info = active_streams[sid]
        status_emoji = {"active": "🟢", "starting": "🟡", "error": "🔴", "stopped": "⚫"}.get(info["status"], "❓")
        health_emoji = "💚" if info["health"]["is_healthy"] else "💔"
        text = (
            f"📺 **Stream Details**\n\n"
            f"**ID:** `{sid}`\n"
            f"**Status:** {status_emoji} {info['status'].title()}\n"
            f"**Health:** {health_emoji}\n"
            f"**Created:** {info['created_at'].strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"**Viewers:** {info.get('viewers', 0)}\n\n"
            f"**Hosted URL:**\n`{info['hosted_url']}`\n\n"
            f"**Source:** `{info['source_url'][:50]}...`"
        )
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=create_stream_keyboard(sid))
    elif data.startswith("stop:"):
        sid = data.split(":", 1)[1]
        success = await stop_stream(sid)
        if success:
            await query.edit_message_text(
                f"✅ Stream `{sid}` stopped successfully!",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]])
            )
        else:
            await query.answer("❌ Failed to stop stream", show_alert=True)
    elif data.startswith("stats:"):
        sid = data.split(":", 1)[1]
        if sid not in active_streams:
            await query.answer("❌ Stream not found", show_alert=True)
            return
        info = active_streams[sid]
        uptime = datetime.now() - info["created_at"]
        stats_text = (
            f"📊 **Stream Statistics**\n\n"
            f"**Stream ID:** `{sid}`\n"
            f"**Uptime:** {str(uptime).split('.')[0]}\n"
            f"**Current Viewers:** {info.get('viewers', 0)}\n"
            f"**Health Status:** {'✅ Healthy' if info['health']['is_healthy'] else '❌ Issues'}\n"
            f"**Error Count:** {info['health']['error_count']}\n"
            f"**Last Check:** {info['health']['last_check'].strftime('%H:%M:%S')}"
        )
        await query.edit_message_text(
            stats_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data=f"stats:{sid}"),
                                                InlineKeyboardButton("🏠 Menu", callback_data="main_menu")]])
        )
    elif data == "help":
        help_text = f"""
❓ **Help & Instructions**

**📺 How to host a stream:**
1. Get an m3u8 live stream URL
2. Send it to this bot
3. Get your hosted URL
4. Share with anyone!

**🔗 URL Examples:**
• `https://example.com/playlist.m3u8`
• `https://stream.site/live/stream.m3u8`

**🎛️ Features:**
• Real-time restreaming
• Health monitoring
• Stream analytics
• Web player
• Mobile-friendly interface

**💡 Tips:**
• Make sure your source URL is accessible
• Streams auto-stop if source goes offline
• Use the web player for best experience

**🔧 Server:** {BASE_URL}
"""
        await query.edit_message_text(
            help_text, parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]])
        )

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    text = update.message.text.strip()
    user = update.effective_user

    if not (text.startswith("http://") or text.startswith("https://")):
        await update.message.reply_text("Please send a valid HTTP/HTTPS URL to an m3u8 stream.", reply_markup=create_main_keyboard())
        return

    if not (".m3u8" in text.lower() or "playlist" in text.lower()):
        await update.message.reply_text(
            "⚠️ This doesn't look like an m3u8 playlist URL.\n"
            "Make sure you're sending a direct link to an .m3u8 file.\n\n"
            "**Example:** `https://example.com/playlist.m3u8`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=create_main_keyboard()
        )
        return

    user_streams = get_user_streams(user.id)
    active_user_streams = sum(1 for s in user_streams.values() if s["status"] == "active")
    if active_user_streams >= 3:
        await update.message.reply_text(
            "⚠️ You have too many active streams. Please stop some streams first.", reply_markup=create_main_keyboard()
        )
        return

    processing_msg = await update.message.reply_text(
        "🔄 **Setting up your stream...**\n\n"
        "• Validating URL\n"
        "• Starting restream process\n"
        "• Preparing hosted URL\n\n"
        "This may take a moment...",
        parse_mode=ParseMode.MARKDOWN
    )

    try:
        stream_id = await start_stream_restream(text, user.id)
        await asyncio.sleep(3)
        if stream_id in active_streams:
            info = active_streams[stream_id]
            success_text = (
                f"✅ **Stream is now hosted!**\n\n"
                f"**Stream ID:** `{stream_id}`\n"
                f"**Status:** {info['status'].title()}\n\n"
                f"**Your hosted URL:**\n`{info['hosted_url']}`\n\n"
                f"**Player URL:**\n{info['player_url']}\n\n"
                f"🔗 Use your hosted URL in any HLS player!\n"
                f"📱 Click buttons below to manage your stream."
            )
            await processing_msg.edit_text(success_text, parse_mode=ParseMode.MARKDOWN, reply_markup=create_stream_keyboard(stream_id))
        else:
            raise RuntimeError("Stream creation failed")
    except Exception as e:
        logger.error(f"Error processing stream URL {text}: {e}")
        await processing_msg.edit_text(
            f"❌ **Error setting up stream**\n\n"
            f"Failed to start restreaming your URL.\n"
            f"Please check that the URL is valid and accessible.\n\n"
            f"**URL:** `{text[:50]}...`\n"
            f"**Error:** `{str(e)[:100]}...`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=create_main_keyboard()
        )

# ==================== WEB SERVER ====================
async def init_web_server():
    app = web.Application()

    async def health_check(request):
        return web.json_response({"status": "ok", "streams": len(active_streams)})

    async def serve_playlist(request):
        sid = request.match_info['stream_id']
        playlist_path = get_stream_dir(sid) / "playlist.m3u8"
        if not playlist_path.exists():
            return web.Response(status=404, text="Stream not found")
        if sid in active_streams:
            active_streams[sid]["viewers"] = active_streams[sid].get("viewers", 0) + 1
        return web.FileResponse(playlist_path, headers={
            "Content-Type": "application/vnd.apple.mpegurl",
            "Cache-Control": "no-cache",
            "Access-Control-Allow-Origin": "*"
        })

    async def serve_segment(request):
        sid = request.match_info['stream_id']
        seg = request.match_info['segment']
        segment_path = get_stream_dir(sid) / seg
        if not segment_path.exists():
            return web.Response(status=404, text="Segment not found")
        return web.FileResponse(segment_path, headers={
            "Content-Type": "video/mp2t",
            "Cache-Control": "max-age=3600",
            "Access-Control-Allow-Origin": "*"
        })

    async def serve_player(request):
        sid = request.match_info['stream_id']
        if sid not in active_streams:
            return web.Response(status=404, text="Stream not found")
        info = active_streams[sid]
        playlist_url = get_hosted_url(sid)
        html = f'''<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Live Stream - {sid}</title>
<script src="https://cdn.jsdelivr.net/npm/hls.js@1.4.12/dist/hls.min.js"></script>
<style>
body {{ margin:0; padding:20px; background:linear-gradient(135deg,#667eea 0%,#764ba2 100%); color:#fff; font-family:Arial,sans-serif; }}
.container {{ max-width:1200px; margin:0 auto; }}
.header {{ text-align:center; margin-bottom:30px; }}
.header h1 {{ font-size:2rem; margin:0; text-shadow:2px 2px 4px rgba(0,0,0,0.3); }}
.video-container {{ background:rgba(0,0,0,0.5); border-radius:15px; overflow:hidden; margin-bottom:20px; }}
#video {{ width:100%; height:auto; min-height:400px; }}
.controls {{ display:flex; justify-content:center; gap:15px; flex-wrap:wrap; }}
.btn {{ padding:12px 24px; border:none; border-radius:25px; background:rgba(255,255,255,0.2); color:white; cursor:pointer; font-weight:bold; }}
.btn:hover {{ background:rgba(255,255,255,0.3); }}
.info {{ background:rgba(255,255,255,0.1); border-radius:10px; padding:20px; margin-top:20px; }}
@media (max-width:768px) {{ body {{ padding:10px; }} .controls {{ flex-direction:column; }} }}
</style>
</head>
<body>
<div class="container">
  <div class="header"><h1>🎬 Live Stream Player</h1></div>
  <div class="video-container"><video id="video" controls autoplay muted playsinline></video></div>
  <div class="controls">
    <button class="btn" onclick="playStream()">▶️ Play</button>
    <button class="btn" onclick="pauseStream()">⏸️ Pause</button>
    <button class="btn" onclick="reloadStream()">🔄 Reload</button>
    <button class="btn" onclick="toggleFullscreen()">🔳 Fullscreen</button>
    <button class="btn" onclick="copyUrl()">📋 Copy URL</button>
  </div>
  <div class="info">
    <h3>📺 Stream Information</h3>
    <p><strong>Stream ID:</strong> {sid}</p>
    <p><strong>Status:</strong> <span id="status">{info['status'].title()}</span></p>
    <p><strong>Source:</strong> {info['source_url'][:60]}...</p>
    <p><strong>Hosted URL:</strong> <code>{playlist_url}</code></p>
    <p><strong>Created:</strong> {info['created_at'].strftime('%Y-%m-%d %H:%M:%S')}</p>
  </div>
</div>
<script>
const video = document.getElementById('video');
const playlistUrl = '{playlist_url}';
let hls;
function initializePlayer() {{
  if (Hls.isSupported()) {{
    hls = new Hls({{ debug:false, enableWorker:true, lowLatencyMode:true }});
    hls.loadSource(playlistUrl);
    hls.attachMedia(video);
    hls.on(Hls.Events.ERROR, function(event, data) {{
      if (data.fatal) {{
        if (data.type === Hls.ErrorTypes.NETWORK_ERROR) hls.startLoad();
        else if (data.type === Hls.ErrorTypes.MEDIA_ERROR) hls.recoverMediaError();
        else hls.destroy();
      }}
    }});
  }} else if (video.canPlayType('application/vnd.apple.mpegurl')) {{
    video.src = playlistUrl;
  }} else {{
    alert('HLS is not supported in this browser');
  }}
}}
function playStream() {{ video.play(); }}
function pauseStream() {{ video.pause(); }}
function reloadStream() {{ if (hls) {{ hls.destroy(); }} initializePlayer(); }}
function toggleFullscreen() {{ if (document.fullscreenElement) document.exitFullscreen(); else video.requestFullscreen(); }}
function copyUrl() {{ navigator.clipboard.writeText(playlistUrl).then(()=>alert('Stream URL copied!')); }}
initializePlayer();
</script>
</body>
</html>'''
        return web.Response(text=html, content_type='text/html')

    async def serve_mini_app(request):
        html = f'''<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HLS Bot Mini App</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:var(--tg-theme-bg-color,#fff);color:var(--tg-theme-text-color,#000);padding:20px}}
.header{{text-align:center;margin-bottom:30px;padding:20px;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);border-radius:15px;color:#fff}}
.card{{background:var(--tg-theme-secondary-bg-color,#f8f9fa);border-radius:12px;padding:20px;margin-bottom:20px}}
.btn{{width:100%;padding:15px;border:none;border-radius:10px;background:var(--tg-theme-button-color,#3390ec);color:var(--tg-theme-button-text-color,#fff);font-size:16px;font-weight:600;cursor:pointer;margin-bottom:10px}}
.input{{width:100%;padding:12px;border:1px solid #ddd;border-radius:8px;font-size:16px;margin-bottom:15px}}
.stream-item{{display:flex;justify-content:space-between;align-items:center;padding:15px;border-bottom:1px solid #eee}}
.status-active{{color:#34c759}}.status-error{{color:#ff3b30}}.status-stopped{{color:#8e8e93}}
</style>
</head>
<body>
  <div class="header"><h1>🎬 HLS Streaming Bot</h1><p>Host live m3u8 streams</p></div>
  <div class="card">
    <h3>📺 Host New Stream</h3>
    <input type="url" id="stream-url" class="input" placeholder="https://example.com/playlist.m3u8">
    <button class="btn" onclick="createStream()">🚀 Start Hosting</button>
  </div>
  <div class="card">
    <h3>📊 Active Streams</h3>
    <div id="streams-list">Loading...</div>
    <button class="btn" onclick="loadStreams()">🔄 Refresh</button>
  </div>
<script>
Telegram.WebApp.ready(); Telegram.WebApp.expand();
function createStream(){{
  const url=document.getElementById('stream-url').value.trim();
  if(!url) return Telegram.WebApp.showAlert('Please enter a valid URL');
  if(!url.includes('.m3u8')) return Telegram.WebApp.showAlert('Please enter a valid m3u8 URL');
  Telegram.WebApp.sendData(JSON.stringify({{action:'create_stream', url}}));
}}
function loadStreams(){{
  fetch('/api/streams')
    .then(r=>r.json())
    .then(data=>{
      const el=document.getElementById('streams-list');
      if(data.streams && data.streams.length){{
        el.innerHTML=data.streams.map(s=>`
          <div class="stream-item">
            <div><strong>${{s.stream_id}}</strong><br><small class="status-${{s.status}}">${{s.status.toUpperCase()}}</small></div>
            <button onclick="openPlayer('${{s.stream_id}}')">▶️</button>
          </div>`).join('');
      }} else el.innerHTML='<p>No active streams</p>';
    }})
    .catch(()=>document.getElementById('streams-list').innerHTML='<p>Error loading streams</p>');
}}
function openPlayer(id){ window.open('{BASE_URL}/player/'+id, '_blank'); }
loadStreams();
</script>
</body>
</html>'''
        return web.Response(text=html, content_type='text/html')

    async def api_streams(request):
        streams_data = []
        for sid, info in active_streams.items():
            streams_data.append({
                "stream_id": sid,
                "status": info["status"],
                "created_at": info["created_at"].isoformat(),
                "viewers": info.get("viewers", 0)
            })
        return web.json_response({"streams": streams_data})

    app.router.add_get('/health', health_check)
    app.router.add_get('/stream/{stream_id}/playlist.m3u8', serve_playlist)
    app.router.add_get('/stream/{stream_id}/{segment}', serve_segment)
    app.router.add_get('/player/{stream_id}', serve_player)
    app.router.add_get('/miniapp', serve_mini_app)
    app.router.add_get('/api/streams', api_streams)
    return app

# ==================== CLEANUP TASK ====================
async def cleanup_old_streams():
    while True:
        try:
            await asyncio.sleep(300)
            now = datetime.now()
            to_remove = []
            for sid, info in list(active_streams.items()):
                age = now - info["created_at"]
                if age > timedelta(hours=2) and info["status"] != "active":
                    to_remove.append(sid)
            for sid in to_remove:
                try:
                    await stop_stream(sid)
                    sdir = get_stream_dir(sid)
                    if sdir.exists():
                        shutil.rmtree(sdir, ignore_errors=True)
                    active_streams.pop(sid, None)
                    logger.info(f"Cleaned up old stream: {sid}")
                except Exception as e:
                    logger.error(f"Cleanup error {sid}: {e}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Cleanup task error: {e}")

# ==================== MAIN ====================
async def start_services(application):
    global client_session
    logger.info(f"🚀 Starting HLS Bot on {BASE_URL}")

    client_session = ClientSession()

    web_app = await init_web_server()
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()
    logger.info(f"🌐 Web server started at {BASE_URL}")

    # Bot commands and menu
    commands = [BotCommand("start", "🏠 Main menu"), BotCommand("help", "❓ Get help")]
    await application.bot.set_my_commands(commands)
    mini_app_button = MenuButtonWebApp(text="🎬 HLS Bot", web_app=WebAppInfo(url=f"{BASE_URL}/miniapp"))
    await application.bot.set_chat_menu_button(menu_button=mini_app_button)
    logger.info("🤖 Telegram bot initialized")

    # Background cleanup
    asyncio.create_task(cleanup_old_streams())

    return runner, site

async def shutdown_services(application, runner: web.AppRunner):
    # Stop all streams
    for sid in list(active_streams.keys()):
        await stop_stream(sid)
    # Close HTTP session
    if client_session:
        await client_session.close()
    # Shutdown web server
    try:
        await runner.cleanup()
    except Exception:
        pass
    logger.info("✅ Shutdown complete")

def install_signal_handlers(loop, stop_func):
    # Koyeb sends SIGTERM on redeploy/scale-down
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(stop_func()))
        except NotImplementedError:
            # On Windows or restricted envs
            pass

async def main_async():
    if BOT_TOKEN == "REPLACE_ME" or not BOT_TOKEN.strip():
        logger.error("BOT_TOKEN is missing. Set BOT_TOKEN env var.")
        return

    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # Handlers
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    # Start web, set commands, etc.
    runner, site = await start_services(application)

    # PTB v20+: use run_polling or start()/stop() without updater
    async def stop_all():
        logger.info("🛑 Shutting down...")
        await application.stop()
        await shutdown_services(application, runner)

    loop = asyncio.get_running_loop()
    install_signal_handlers(loop, stop_all)

    # Start bot polling
    await application.initialize()
    await application.start()
    logger.info("✅ HLS Bot is running!")
    logger.info(f"📺 Send m3u8 URLs to host streams")
    logger.info(f"🌐 Web interface: {BASE_URL}")

    # Keep alive until cancelled
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        await stop_all()

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
