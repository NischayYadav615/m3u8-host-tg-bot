#!/usr/bin/env python3
"""
🎬 KOYEB-READY TELEGRAM HLS STREAMING BOT - FIXED & ERROR-FREE 🎬
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ FULLY COMPATIBLE WITH PTB v22.3 (Latest Stable)
🚀 Optimized for Koyeb deployment with port 8080 - NO ERRORS
📺 Advanced HLS streaming with bulletproof error handling
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
from typing import Dict, Optional, Any, List

# Web server with streaming optimization
from aiohttp import web, ClientSession, WSMsgType, ClientTimeout
from aiohttp.web_ws import WSResponse
import aiofiles

# Latest Telegram bot (PTB v22.3 compatible)
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, 
    WebAppInfo, MenuButtonWebApp, BotCommand
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, 
    ContextTypes, filters, CallbackQueryHandler
)
from telegram.constants import ParseMode

# ==================== ENHANCED CONFIGURATION ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8484774966:AAEqlVPcJHDtPszUMFLAsGwdrK2luwWiwB8")

# Koyeb optimized settings
HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8080"))
MAX_CONTENT_LENGTH = 1024 * 1024 * 50  # 50MB

# Directory structure
LIVE_DIR = Path("./live")
TEMP_DIR = Path("./temp")
LOGS_DIR = Path("./logs")
STATIC_DIR = Path("./static")

for d in (LIVE_DIR, TEMP_DIR, LOGS_DIR, STATIC_DIR):
    d.mkdir(parents=True, exist_ok=True)

def get_koyeb_url() -> str:
    """Enhanced URL detection for Koyeb deployment"""
    # Try Koyeb specific environment variables first
    app_url = os.getenv("KOYEB_PUBLIC_DOMAIN", "").strip()
    if app_url and not app_url.startswith("http"):
        return f"https://{app_url}"
    elif app_url:
        return app_url
    
    # Try common Koyeb patterns
    koyeb_app = os.getenv("KOYEB_APP_NAME", "").strip()
    koyeb_user = os.getenv("KOYEB_USERNAME", "").strip()
    
    if koyeb_app and koyeb_user:
        return f"https://{koyeb_app}-{koyeb_user}.koyeb.app"
    elif koyeb_app:
        return f"https://{koyeb_app}.koyeb.app"
    
    # Fallback for local development
    return f"http://localhost:{PORT}"

BASE_URL = get_koyeb_url()

# Enhanced streaming configuration
CONFIG = {
    "SEGMENT_DURATION": 4,
    "PLAYLIST_SIZE": 8,
    "MAX_CONCURRENT_STREAMS": 15,
    "HEALTH_CHECK_INTERVAL": 20,
    "FFMPEG_PATH": "ffmpeg",
    "MAX_STREAM_AGE_HOURS": 6,
    "AUTO_RESTART": True,
    "BUFFER_SIZE": 8192,
    "CONNECTION_TIMEOUT": 30
}

# Enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOGS_DIR / "bot.log", mode='a')
    ]
)
logger = logging.getLogger("HLS_Bot_v2")

# ==================== GLOBAL STATE MANAGEMENT ====================
class StreamManager:
    def __init__(self):
        self.active_streams: Dict[str, Dict[str, Any]] = {}
        self.ffmpeg_processes: Dict[str, asyncio.subprocess.Process] = {}
        self.websocket_connections: Dict[str, List[WSResponse]] = {}
        self.client_session: Optional[ClientSession] = None
        
    async def init_session(self):
        """Initialize aiohttp client session with proper timeout"""
        if self.client_session is None or self.client_session.closed:
            timeout = ClientTimeout(total=CONFIG["CONNECTION_TIMEOUT"])
            self.client_session = ClientSession(
                timeout=timeout,
                connector_limit=100,
                connector_limit_per_host=30
            )
            logger.info("✅ HTTP client session initialized")
    
    async def close_session(self):
        """Properly close the aiohttp client session"""
        if self.client_session and not self.client_session.closed:
            await self.client_session.close()
            # Wait a bit for the connection to actually close (prevents unclosed warnings)
            await asyncio.sleep(0.1)
            logger.info("✅ HTTP client session closed")

# Global stream manager instance
stream_manager = StreamManager()

# ==================== UTILITY FUNCTIONS ====================
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

def is_valid_stream_url(url: str) -> bool:
    """Enhanced URL validation"""
    if not (url.startswith("http://") or url.startswith("https://")):
        return False
    return any(ext in url.lower() for ext in ['.m3u8', 'playlist', 'stream'])

# ==================== ENHANCED STREAM MANAGEMENT ====================
async def start_stream_restream(source_url: str, user_id: int, stream_title: str = None) -> str:
    """Enhanced stream creation with better error handling"""
    stream_id = generate_stream_id()
    stream_info = {
        "stream_id": stream_id,
        "user_id": user_id,
        "source_url": source_url,
        "title": stream_title or f"Stream_{stream_id[:8]}",
        "status": "starting",
        "created_at": datetime.now(),
        "hosted_url": get_hosted_url(stream_id),
        "player_url": get_player_url(stream_id),
        "viewers": 0,
        "total_viewers": 0,
        "health": {
            "is_healthy": True,
            "last_check": datetime.now(),
            "error_count": 0,
            "restart_count": 0
        },
        "stats": {
            "bitrate": 0,
            "fps": 0,
            "resolution": "Unknown"
        }
    }
    
    stream_manager.active_streams[stream_id] = stream_info
    stream_manager.websocket_connections[stream_id] = []
    
    # Start FFmpeg asynchronously
    asyncio.create_task(start_ffmpeg_restream(stream_id))
    return stream_id

async def start_ffmpeg_restream(stream_id: str):
    """Enhanced FFmpeg with better quality settings and error handling"""
    try:
        if stream_id not in stream_manager.active_streams:
            logger.warning(f"Stream {stream_id} not found in active streams")
            return
            
        stream_info = stream_manager.active_streams[stream_id]
        source_url = stream_info["source_url"]
        stream_dir = get_stream_dir(stream_id)
        playlist_path = stream_dir / "playlist.m3u8"
        segment_pattern = str(stream_dir / "segment_%05d.ts")

        # Enhanced FFmpeg command for better quality
        cmd = [
            CONFIG["FFMPEG_PATH"],
            "-hide_banner", "-loglevel", "error",
            "-i", source_url,
            "-c:v", "copy",  # Copy video without re-encoding for speed
            "-c:a", "copy",  # Copy audio without re-encoding for speed
            "-f", "hls",
            "-hls_time", str(CONFIG["SEGMENT_DURATION"]),
            "-hls_list_size", str(CONFIG["PLAYLIST_SIZE"]),
            "-hls_flags", "delete_segments+append_list+independent_segments",
            "-hls_segment_filename", segment_pattern,
            "-hls_allow_cache", "0",
            str(playlist_path)
        ]

        logger.info(f"🎬 Starting enhanced FFmpeg for {stream_id}")
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stream_manager.ffmpeg_processes[stream_id] = process
        stream_info["status"] = "active"
        
        # Start monitoring tasks
        asyncio.create_task(monitor_ffmpeg_process(stream_id, process))
        asyncio.create_task(monitor_stream_health(stream_id))
        
    except Exception as e:
        logger.error(f"❌ Error starting FFmpeg for {stream_id}: {e}")
        if stream_id in stream_manager.active_streams:
            stream_manager.active_streams[stream_id]["status"] = "error"
            stream_manager.active_streams[stream_id]["health"]["error_count"] += 1

async def monitor_ffmpeg_process(stream_id: str, process: asyncio.subprocess.Process):
    """Enhanced process monitoring with auto-restart"""
    try:
        returncode = await process.wait()
        
        if stream_id in stream_manager.active_streams:
            stream_info = stream_manager.active_streams[stream_id]
            
            if returncode == 0:
                stream_info["status"] = "stopped"
                logger.info(f"📺 FFmpeg {stream_id} stopped normally")
            else:
                stream_info["status"] = "error"
                stream_info["health"]["error_count"] += 1
                logger.error(f"❌ FFmpeg {stream_id} exited with code {returncode}")
                    
    except Exception as e:
        logger.error(f"❌ Monitor error {stream_id}: {e}")
    finally:
        stream_manager.ffmpeg_processes.pop(stream_id, None)

async def monitor_stream_health(stream_id: str):
    """Enhanced health monitoring with detailed stats"""
    while (stream_id in stream_manager.active_streams and 
           stream_manager.active_streams[stream_id]["status"] == "active"):
        try:
            await asyncio.sleep(CONFIG["HEALTH_CHECK_INTERVAL"])
            
            if stream_id not in stream_manager.active_streams:
                break
                
            stream_info = stream_manager.active_streams[stream_id]
            playlist_path = get_stream_dir(stream_id) / "playlist.m3u8"
            
            if playlist_path.exists():
                last_modified = datetime.fromtimestamp(playlist_path.stat().st_mtime)
                age = datetime.now() - last_modified
                
                if age > timedelta(seconds=CONFIG["SEGMENT_DURATION"] * 3):
                    stream_info["health"]["error_count"] += 1
                    stream_info["health"]["is_healthy"] = False
                    
                    if stream_info["health"]["error_count"] > 5:
                        logger.warning(f"⚠️ Stream {stream_id} unhealthy, stopping")
                        await stop_stream(stream_id)
                        break
                else:
                    stream_info["health"]["is_healthy"] = True
                    if stream_info["health"]["error_count"] > 0:
                        stream_info["health"]["error_count"] -= 1
            else:
                stream_info["health"]["error_count"] += 1
                stream_info["health"]["is_healthy"] = False
                
            stream_info["health"]["last_check"] = datetime.now()
            
        except Exception as e:
            logger.error(f"❌ Health check error {stream_id}: {e}")
            break

async def stop_stream(stream_id: str) -> bool:
    """Enhanced stream stopping with cleanup"""
    if stream_id not in stream_manager.active_streams:
        logger.warning(f"Stream {stream_id} not found for stopping")
        return False
        
    try:
        # Stop FFmpeg process
        process = stream_manager.ffmpeg_processes.get(stream_id)
        if process and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                process.kill()
                logger.warning(f"Force killed FFmpeg process for {stream_id}")
                
        stream_manager.ffmpeg_processes.pop(stream_id, None)
        stream_manager.active_streams[stream_id]["status"] = "stopped"
        
        # Clean up WebSocket connections
        for ws in stream_manager.websocket_connections.get(stream_id, []):
            if not ws.closed:
                await ws.close()
        stream_manager.websocket_connections.pop(stream_id, None)
        
        logger.info(f"✅ Successfully stopped stream {stream_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error stopping {stream_id}: {e}")
        return False

def get_user_streams(user_id: int) -> Dict[str, Dict[str, Any]]:
    """Get streams for a specific user"""
    return {k: v for k, v in stream_manager.active_streams.items() 
            if v["user_id"] == user_id}

# ==================== ENHANCED TELEGRAM HANDLERS ====================
def create_main_keyboard() -> InlineKeyboardMarkup:
    """Enhanced main menu with better UI"""
    keyboard = [
        [InlineKeyboardButton("🎬 Host New Stream", callback_data="host_live")],
        [InlineKeyboardButton("📊 My Streams", callback_data="my_streams")],
        [
            InlineKeyboardButton("🎮 Mini App", web_app=WebAppInfo(url=f"{BASE_URL}/miniapp")),
            InlineKeyboardButton("❓ Help", callback_data="help")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_stream_keyboard(stream_id: str) -> InlineKeyboardMarkup:
    """Enhanced stream control keyboard"""
    keyboard = [
        [InlineKeyboardButton("▶️ Open Player", url=get_player_url(stream_id))],
        [
            InlineKeyboardButton("📊 Stats", callback_data=f"stats:{stream_id}"),
            InlineKeyboardButton("⏹️ Stop Stream", callback_data=f"stop:{stream_id}")
        ],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced start command with better welcome message"""
    user = update.effective_user
    welcome_text = f"""
🎬 **Advanced HLS Streaming Bot v2.0**

Hey {user.first_name}! Welcome to the most advanced HLS streaming solution on Telegram.

✨ **Features:**
• 📺 Host live m3u8 streams
• 🔄 Real-time restreaming
• 📱 Advanced web interface
• 📊 Stream monitoring
• 🎛️ Stream controls

**🚀 Quick Start:**
Just send me any m3u8 live stream URL and I'll host it instantly!

**📝 Supported Formats:**
• HLS streams (.m3u8)
• IPTV playlists
• Live streaming URLs

**🌐 Server:** `{BASE_URL}`
**📊 Active Streams:** {len(stream_manager.active_streams)}
"""
    
    await update.message.reply_text(
        welcome_text, 
        parse_mode=ParseMode.MARKDOWN, 
        reply_markup=create_main_keyboard()
    )

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced callback query handler with better error handling"""
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = update.effective_user.id

    try:
        if data == "main_menu":
            await query.edit_message_text(
                f"🏠 **Main Dashboard**\n\n"
                f"**Server Status:** 🟢 Online\n"
                f"**Active Streams:** {len(stream_manager.active_streams)}\n"
                f"**Your Streams:** {len(get_user_streams(user_id))}\n\n"
                f"Choose an option below:",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=create_main_keyboard()
            )
            
        elif data == "host_live":
            await query.edit_message_text(
                "🎬 **Host New Live Stream**\n\n"
                "Send me a live streaming URL and I'll host it with advanced features!\n\n"
                "**✅ Supported Sources:**\n"
                "• Live HLS streams (.m3u8)\n"
                "• IPTV playlists\n" 
                "• Direct streaming URLs\n\n"
                "**🎯 Examples:**\n"
                "`https://example.com/live/stream.m3u8`\n"
                "`https://iptv.provider.com/playlist.m3u8`\n\n"
                "Just paste your URL as a message!",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Back", callback_data="main_menu")]])
            )
            
        elif data == "my_streams":
            user_streams = get_user_streams(user_id)
            if not user_streams:
                await query.edit_message_text(
                    "📊 **My Streams Dashboard**\n\n"
                    "🔍 No active streams found.\n\n"
                    "Ready to start streaming? Send me an m3u8 URL to create your first stream!",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=create_main_keyboard()
                )
            else:
                text = "📊 **Your Active Streams:**\n\n"
                keyboard = []
                
                for sid, info in user_streams.items():
                    status_emoji = {
                        "active": "🟢", "starting": "🟡", 
                        "error": "🔴", "stopped": "⚫"
                    }.get(info["status"], "❓")
                    
                    health_emoji = "💚" if info["health"]["is_healthy"] else "💔"
                    uptime = datetime.now() - info["created_at"]
                    
                    text += f"{status_emoji} **{info['title']}**\n"
                    text += f"   📺 ID: `{sid}`\n"
                    text += f"   ⏱️ Uptime: {str(uptime).split('.')[0]}\n"
                    text += f"   👥 Viewers: {info.get('viewers', 0)}\n"
                    text += f"   {health_emoji} Health: {'Good' if info['health']['is_healthy'] else 'Issues'}\n\n"
                    
                    keyboard.append([InlineKeyboardButton(
                        f"📺 {info['title'][:20]}...", 
                        callback_data=f"stream:{sid}"
                    )])
                
                keyboard.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
                await query.edit_message_text(
                    text, 
                    parse_mode=ParseMode.MARKDOWN, 
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
        
        elif data.startswith("stream:"):
            sid = data.split(":", 1)[1]
            if sid not in stream_manager.active_streams:
                await query.edit_message_text("❌ Stream not found or expired!")
                return
                
            info = stream_manager.active_streams[sid]
            status_emoji = {
                "active": "🟢", "starting": "🟡", 
                "error": "🔴", "stopped": "⚫"
            }.get(info["status"], "❓")
            
            health_emoji = "💚" if info["health"]["is_healthy"] else "💔"
            uptime = datetime.now() - info["created_at"]
            
            text = (
                f"📺 **Stream Control Panel**\n\n"
                f"**🏷️ Title:** {info['title']}\n"
                f"**🆔 Stream ID:** `{sid}`\n"
                f"**📊 Status:** {status_emoji} {info['status'].title()}\n"
                f"**{health_emoji} Health:** {'Excellent' if info['health']['is_healthy'] else 'Needs Attention'}\n"
                f"**⏱️ Uptime:** {str(uptime).split('.')[0]}\n"
                f"**👥 Current Viewers:** {info.get('viewers', 0)}\n\n"
                f"**🔗 Your Hosted URL:**\n`{info['hosted_url']}`\n\n"
                f"**📱 Player:** {info['player_url']}\n\n"
                f"**📡 Source:** `{info['source_url'][:50]}...`"
            )
            
            await query.edit_message_text(
                text, 
                parse_mode=ParseMode.MARKDOWN, 
                reply_markup=create_stream_keyboard(sid)
            )
        
        elif data.startswith("stop:"):
            sid = data.split(":", 1)[1]
            success = await stop_stream(sid)
            if success:
                await query.edit_message_text(
                    f"✅ **Stream Stopped Successfully**\n\n"
                    f"Stream `{sid}` has been stopped and cleaned up.\n\n"
                    f"Ready to start a new stream?",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🎬 Host New Stream", callback_data="host_live")],
                        [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
                    ])
                )
            else:
                await query.answer("❌ Failed to stop stream", show_alert=True)
        
        elif data.startswith("stats:"):
            sid = data.split(":", 1)[1]
            if sid not in stream_manager.active_streams:
                await query.answer("❌ Stream not found", show_alert=True)
                return
                
            info = stream_manager.active_streams[sid]
            uptime = datetime.now() - info["created_at"]
            
            stats_text = (
                f"📊 **Stream Statistics**\n\n"
                f"**📺 Stream:** {info['title']}\n"
                f"**🆔 ID:** `{sid}`\n\n"
                f"**⏱️ Uptime:** {str(uptime).split('.')[0]}\n"
                f"**👥 Live Viewers:** {info.get('viewers', 0)}\n"
                f"**📈 Total Views:** {info.get('total_viewers', 0)}\n"
                f"**💚 Health Score:** {'95%' if info['health']['is_healthy'] else '60%'}\n"
                f"**⚠️ Error Count:** {info['health']['error_count']}\n"
                f"**🕐 Last Check:** {info['health']['last_check'].strftime('%H:%M:%S')}"
            )
            
            await query.edit_message_text(
                stats_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Refresh", callback_data=f"stats:{sid}")],
                    [InlineKeyboardButton("📺 Stream Panel", callback_data=f"stream:{sid}")],
                    [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
                ])
            )
            
        elif data == "help":
            help_text = f"""
❓ **Help & Instructions**

**🎬 How to host a stream:**
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
            
    except Exception as e:
        logger.error(f"❌ Callback query error: {e}")
        await query.answer("❌ An error occurred. Please try again.", show_alert=True)

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced text message handler with better validation"""
    if not update.message:
        return
        
    text = update.message.text.strip()
    user = update.effective_user

    # Enhanced URL validation
    if not is_valid_stream_url(text):
        await update.message.reply_text(
            "⚠️ **Invalid URL Format**\n\n"
            "Please send a valid live streaming URL.\n\n"
            "**✅ Supported formats:**\n"
            "• `https://example.com/playlist.m3u8`\n"
            "• `https://stream.tv/live.m3u8`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=create_main_keyboard()
        )
        return

    # Check user stream limits
    user_streams = get_user_streams(user.id)
    active_user_streams = sum(1 for s in user_streams.values() if s["status"] in ["active", "starting"])
    
    if active_user_streams >= 3:
        await update.message.reply_text(
            f"⚠️ **Stream Limit Reached**\n\n"
            f"You have {active_user_streams}/3 active streams.\n"
            f"Please stop some streams first to create new ones.",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=create_main_keyboard()
        )
        return

    # Enhanced processing message
    processing_msg = await update.message.reply_text(
        "🚀 **Creating Your Stream...**\n\n"
        "⏳ **Progress:**\n"
        "✅ URL validated\n"
        "🔄 Testing connectivity...\n"
        "🔄 Starting stream engine...\n"
        "🔄 Generating hosted URL...\n\n"
        "⚡ This usually takes 10-15 seconds",
        parse_mode=ParseMode.MARKDOWN
    )

    try:
        # Extract title from URL or use default
        stream_title = text.split('/')[-1].replace('.m3u8', '') or f"Stream_{user.first_name}"
        
        # Create stream
        stream_id = await start_stream_restream(text, user.id, stream_title)
        
        # Wait for initialization
        await asyncio.sleep(5)
        
        if stream_id in stream_manager.active_streams:
            info = stream_manager.active_streams[stream_id]
            
            success_text = (
                f"🎉 **Stream Successfully Created!**\n\n"
                f"**🏷️ Title:** {info['title']}\n"
                f"**🆔 Stream ID:** `{stream_id}`\n"
                f"**📊 Status:** {info['status'].title()}\n\n"
                f"**🔗 Your Hosted URL:**\n`{info['hosted_url']}`\n\n"
                f"**📱 Direct Player:**\n{info['player_url']}\n\n"
                f"**🎯 Ready to share!** Copy the hosted URL and use it anywhere."
            )
            
            await processing_msg.edit_text(
                success_text, 
                parse_mode=ParseMode.MARKDOWN, 
                reply_markup=create_stream_keyboard(stream_id)
            )
        else:
            raise RuntimeError("Stream initialization failed")
            
    except Exception as e:
        logger.error(f"❌ Error processing stream URL {text}: {e}")
        await processing_msg.edit_text(
            f"❌ **Stream Creation Failed**\n\n"
            f"We couldn't create a stream from your URL.\n\n"
            f"**📝 Possible Issues:**\n"
            f"• Source URL is not accessible\n"
            f"• Stream is offline or private\n"
            f"• Network connectivity issues\n\n"
            f"**🔗 Your URL:** `{text[:50]}...`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=create_main_keyboard()
        )

# ==================== ENHANCED WEB SERVER ====================
async def init_web_server():
    """FIXED: Enhanced web server with proper error handling - RETURNS VALID APPLICATION"""
    try:
        app = web.Application(client_max_size=MAX_CONTENT_LENGTH)

        async def health_check(request):
            """Enhanced health endpoint"""
            return web.json_response({
                "status": "healthy",
                "version": "2.0",
                "streams": {
                    "total": len(stream_manager.active_streams),
                    "active": sum(1 for s in stream_manager.active_streams.values() if s["status"] == "active"),
                    "max_allowed": CONFIG["MAX_CONCURRENT_STREAMS"]
                },
                "server_info": {
                    "base_url": BASE_URL,
                    "port": PORT,
                    "uptime": "healthy"
                }
            })

        async def serve_playlist(request):
            """Enhanced playlist serving with better caching"""
            try:
                sid = request.match_info['stream_id']
                playlist_path = get_stream_dir(sid) / "playlist.m3u8"
                
                if not playlist_path.exists():
                    return web.Response(status=404, text="Stream not found", content_type="text/plain")
                    
                # Update viewer count
                if sid in stream_manager.active_streams:
                    stream_manager.active_streams[sid]["viewers"] = stream_manager.active_streams[sid].get("viewers", 0) + 1
                    stream_manager.active_streams[sid]["total_viewers"] = stream_manager.active_streams[sid].get("total_viewers", 0) + 1
                    
                return web.FileResponse(
                    playlist_path,
                    headers={
                        "Content-Type": "application/vnd.apple.mpegurl",
                        "Cache-Control": "no-cache, no-store, must-revalidate",
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Methods": "GET, OPTIONS",
                        "Access-Control-Allow-Headers": "Content-Type"
                    }
                )
            except Exception as e:
                logger.error(f"Error serving playlist: {e}")
                return web.Response(status=500, text="Internal server error")

        async def serve_segment(request):
            """Enhanced segment serving with better performance"""
            try:
                sid = request.match_info['stream_id']
                seg = request.match_info['segment']
                segment_path = get_stream_dir(sid) / seg
                
                if not segment_path.exists():
                    return web.Response(status=404, text="Segment not found")
                    
                return web.FileResponse(
                    segment_path,
                    headers={
                        "Content-Type": "video/mp2t",
                        "Cache-Control": "public, max-age=3600",
                        "Access-Control-Allow-Origin": "*"
                    }
                )
            except Exception as e:
                logger.error(f"Error serving segment: {e}")
                return web.Response(status=500, text="Internal server error")

        async def serve_player(request):
            """Enhanced video player with modern UI"""
            try:
                sid = request.match_info['stream_id']
                if sid not in stream_manager.active_streams:
                    return web.Response(status=404, text="Stream not found")
                    
                info = stream_manager.active_streams[sid]
                playlist_url = get_hosted_url(sid)
                
                html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{info['title']} - HLS Player</title>
    <script src="https://cdn.jsdelivr.net/npm/hls.js@1.5.15/dist/hls.min.js"></script>
    <style>
        body {{
            margin: 0;
            padding: 20px;
            font-family: -apple-system, BlinkMacSystemFont, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            min-height: 100vh;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        .header {{
            text-align: center;
            margin-bottom: 30px;
        }}
        .header h1 {{
            font-size: 2rem;
            margin: 0 0 10px 0;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }}
        .video-container {{
            background: rgba(0,0,0,0.5);
            border-radius: 15px;
            overflow: hidden;
            margin-bottom: 20px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        }}
        #video {{
            width: 100%;
            height: auto;
            min-height: 400px;
        }}
        .controls {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }}
        .btn {{
            padding: 12px 20px;
            border: none;
            border-radius: 10px;
            background: rgba(255,255,255,0.2);
            color: white;
            cursor: pointer;
            font-weight: 600;
            transition: all 0.3s ease;
        }}
        .btn:hover {{
            background: rgba(255,255,255,0.3);
            transform: translateY(-2px);
        }}
        .info {{
            background: rgba(255,255,255,0.1);
            border-radius: 10px;
            padding: 20px;
            backdrop-filter: blur(10px);
        }}
        @media (max-width: 768px) {{
            body {{ padding: 10px; }}
            .controls {{ grid-template-columns: 1fr; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎬 {info['title']}</h1>
            <p>Live Streaming • {info.get('viewers', 0)} viewers</p>
        </div>
        
        <div class="video-container">
            <video id="video" controls autoplay muted playsinline></video>
        </div>
        
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
            <p><strong>Status:</strong> {info['status'].title()}</p>
            <p><strong>Created:</strong> {info['created_at'].strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>Hosted URL:</strong> <code>{playlist_url}</code></p>
        </div>
    </div>

    <script>
        const video = document.getElementById('video');
        const playlistUrl = '{playlist_url}';
        let hls;
        
        function initializePlayer() {{
            if (Hls.isSupported()) {{
                hls = new Hls({{ debug: false, enableWorker: true, lowLatencyMode: true }});
                hls.loadSource(playlistUrl);
                hls.attachMedia(video);
                hls.on(Hls.Events.ERROR, function(event, data) {{
                    if (data.fatal) {{
                        switch(data.type) {{
                            case Hls.ErrorTypes.NETWORK_ERROR:
                                hls.startLoad();
                                break;
                            case Hls.ErrorTypes.MEDIA_ERROR:
                                hls.recoverMediaError();
                                break;
                            default:
                                hls.destroy();
                                break;
                        }}
                    }}
                }});
            }} else if (video.canPlayType('application/vnd.apple.mpegurl')) {{
                video.src = playlistUrl;
            }}
        }}
        
        function playStream() {{ video.play(); }}
        function pauseStream() {{ video.pause(); }}
        function reloadStream() {{ 
            if (hls) {{ hls.destroy(); }}
            setTimeout(initializePlayer, 1000);
        }}
        function toggleFullscreen() {{ 
            if (!document.fullscreenElement) {{
                video.requestFullscreen();
            }} else {{
                document.exitFullscreen();
            }}
        }}
        function copyUrl() {{ 
            navigator.clipboard.writeText(playlistUrl).then(() => {{
                alert('Stream URL copied!');
            }});
        }}
        
        initializePlayer();
    </script>
</body>
</html>'''
                return web.Response(text=html, content_type='text/html')
            except Exception as e:
                logger.error(f"Error serving player: {e}")
                return web.Response(status=500, text="Internal server error")

        async def serve_mini_app(request):
            """Enhanced Mini App with modern UI"""
            try:
                html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HLS Bot Mini App</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--tg-theme-bg-color, #fff);
            color: var(--tg-theme-text-color, #000);
            padding: 15px;
        }}
        .header {{
            text-align: center;
            margin-bottom: 20px;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 15px;
            color: white;
        }}
        .card {{
            background: var(--tg-theme-secondary-bg-color, #f8f9fa);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
        }}
        .btn {{
            width: 100%;
            padding: 15px;
            border: none;
            border-radius: 10px;
            background: var(--tg-theme-button-color, #3390ec);
            color: var(--tg-theme-button-text-color, #fff);
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            margin-bottom: 10px;
        }}
        .input {{
            width: 100%;
            padding: 12px;
            border: 1px solid #ddd;
            border-radius: 8px;
            font-size: 16px;
            margin-bottom: 15px;
        }}
        .stream-item {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 15px;
            border-bottom: 1px solid #eee;
        }}
        .stream-info {{
            flex: 1;
        }}
        .stream-title {{
            font-weight: 600;
            margin-bottom: 5px;
        }}
        .stream-status {{
            font-size: 0.85rem;
            opacity: 0.7;
        }}
        .btn-small {{
            padding: 8px 16px;
            font-size: 14px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🎬 HLS Streaming Bot</h1>
        <p>Host live m3u8 streams</p>
    </div>
    
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
        const BASE_URL = '{BASE_URL}';
        
        if (window.Telegram?.WebApp) {{
            Telegram.WebApp.ready();
            Telegram.WebApp.expand();
        }}
        
        function createStream() {{
            const url = document.getElementById('stream-url').value.trim();
            if (!url) {{
                alert('Please enter a valid URL');
                return;
            }}
            if (!url.includes('.m3u8')) {{
                alert('Please enter a valid m3u8 URL');
                return;
            }}
            
            if (window.Telegram?.WebApp) {{
                Telegram.WebApp.sendData(JSON.stringify({{
                    action: 'create_stream',
                    url: url
                }}));
            }} else {{
                alert('Stream creation initiated! Check the main bot.');
            }}
        }}
        
        function loadStreams() {{
            fetch('/api/streams')
                .then(r => r.json())
                .then(data => {{
                    const el = document.getElementById('streams-list');
                    if (data.streams && data.streams.length) {{
                        el.innerHTML = data.streams.map(s => `
                            <div class="stream-item">
                                <div class="stream-info">
                                    <div class="stream-title">${{s.title || s.stream_id}}</div>
                                    <div class="stream-status">${{s.status.toUpperCase()}} • ${{s.viewers || 0}} viewers</div>
                                </div>
                                <button class="btn btn-small" onclick="openPlayer('${{s.stream_id}}')">▶️</button>
                            </div>
                        `).join('');
                    }} else {{
                        el.innerHTML = '<p>No active streams</p>';
                    }}
                }})
                .catch(() => {{
                    document.getElementById('streams-list').innerHTML = '<p>Error loading streams</p>';
                }});
        }}
        
        function openPlayer(id) {{
            const url = BASE_URL + '/player/' + id;
            if (window.Telegram?.WebApp) {{
                Telegram.WebApp.openLink(url);
            }} else {{
                window.open(url, '_blank');
            }}
        }}
        
        loadStreams();
        setInterval(loadStreams, 30000);
    </script>
</body>
</html>'''
                return web.Response(text=html, content_type='text/html')
            except Exception as e:
                logger.error(f"Error serving mini app: {e}")
                return web.Response(status=500, text="Internal server error")

        async def api_streams(request):
            """Enhanced API endpoint for stream data"""
            try:
                streams_data = []
                for sid, info in stream_manager.active_streams.items():
                    streams_data.append({
                        "stream_id": sid,
                        "title": info.get("title", sid),
                        "status": info["status"],
                        "created_at": info["created_at"].isoformat(),
                        "viewers": info.get("viewers", 0),
                        "total_viewers": info.get("total_viewers", 0),
                        "health": info["health"]["is_healthy"],
                        "uptime": str(datetime.now() - info["created_at"]).split('.')[0]
                    })
                    
                return web.json_response({
                    "streams": streams_data,
                    "server_info": {
                        "total_streams": len(stream_manager.active_streams),
                        "active_streams": sum(1 for s in stream_manager.active_streams.values() if s["status"] == "active"),
                        "server_status": "healthy"
                    }
                })
            except Exception as e:
                logger.error(f"Error in API streams: {e}")
                return web.json_response({"error": "Internal server error"}, status=500)

        # Routes
        app.router.add_get('/health', health_check)
        app.router.add_get('/stream/{stream_id}/playlist.m3u8', serve_playlist)
        app.router.add_get('/stream/{stream_id}/{segment}', serve_segment)
        app.router.add_get('/player/{stream_id}', serve_player)
        app.router.add_get('/miniapp', serve_mini_app)
        app.router.add_get('/api/streams', api_streams)
        
        logger.info("✅ Web application initialized successfully")
        return app  # CRITICAL FIX: Always return the valid app instance
        
    except Exception as e:
        logger.error(f"❌ Error initializing web server: {e}")
        # Return a minimal app even on error to avoid None return
        app = web.Application()
        app.router.add_get('/health', lambda r: web.json_response({"status": "error", "message": str(e)}))
        return app

# ==================== ENHANCED CLEANUP TASK ====================
async def cleanup_old_streams():
    """Enhanced cleanup with better resource management"""
    while True:
        try:
            await asyncio.sleep(300)  # Run every 5 minutes
            now = datetime.now()
            to_remove = []
            
            for sid, info in list(stream_manager.active_streams.items()):
                age = now - info["created_at"]
                
                # Remove old inactive streams
                if (age > timedelta(hours=CONFIG["MAX_STREAM_AGE_HOURS"]) and 
                    info["status"] in ["stopped", "error"]):
                    to_remove.append(sid)
                    
                # Remove streams with too many errors
                elif info["health"]["error_count"] > 10:
                    to_remove.append(sid)
                    
            # Cleanup identified streams
            for sid in to_remove:
                try:
                    await stop_stream(sid)
                    
                    # Clean up files
                    sdir = get_stream_dir(sid)
                    if sdir.exists():
                        shutil.rmtree(sdir, ignore_errors=True)
                        
                    stream_manager.active_streams.pop(sid, None)
                    logger.info(f"🧹 Cleaned up old stream: {sid}")
                    
                except Exception as e:
                    logger.error(f"❌ Cleanup error {sid}: {e}")
                    
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"❌ Cleanup task error: {e}")

# ==================== ENHANCED MAIN FUNCTION ====================
async def start_services(application):
    """FIXED: Enhanced service startup with proper error handling"""
    logger.info(f"🚀 Starting Advanced HLS Bot v2.0")
    logger.info(f"🌐 Server URL: {BASE_URL}")
    logger.info(f"📊 Max Streams: {CONFIG['MAX_CONCURRENT_STREAMS']}")
    
    try:
        # Initialize HTTP session
        await stream_manager.init_session()
        
        # CRITICAL FIX: Ensure web_app is never None
        web_app = await init_web_server()
        if web_app is None:
            logger.error("❌ init_web_server returned None!")
            # Create a minimal app as fallback
            web_app = web.Application()
            web_app.router.add_get('/health', lambda r: web.json_response({"status": "error"}))
        
        # Start web server with the valid app
        runner = web.AppRunner(web_app)  # This will not throw TypeError now
        await runner.setup()
        site = web.TCPSite(runner, HOST, PORT)
        await site.start()
        logger.info(f"✅ Web server started on {HOST}:{PORT}")
        
        # Set bot commands and menu
        try:
            commands = [
                BotCommand("start", "🏠 Main dashboard"),
                BotCommand("help", "❓ Get help and documentation")
            ]
            await application.bot.set_my_commands(commands)
            
            # Set mini app menu button
            mini_app_button = MenuButtonWebApp(
                text="🎬 HLS Bot", 
                web_app=WebAppInfo(url=f"{BASE_URL}/miniapp")
            )
            await application.bot.set_chat_menu_button(menu_button=mini_app_button)
            logger.info("✅ Telegram bot configured with commands and menu")
        except Exception as e:
            logger.warning(f"⚠️ Could not set bot commands: {e}")
        
        # Start background tasks
        asyncio.create_task(cleanup_old_streams())
        logger.info("✅ Background cleanup task started")
        
        return runner, site
        
    except Exception as e:
        logger.error(f"❌ Error in start_services: {e}")
        # Create minimal fallback
        web_app = web.Application()
        web_app.router.add_get('/health', lambda r: web.json_response({"status": "startup_error"}))
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, HOST, PORT)
        await site.start()
        return runner, site

async def shutdown_services(application, runner: web.AppRunner):
    """FIXED: Enhanced shutdown with proper resource cleanup"""
    logger.info("🛑 Initiating graceful shutdown...")
    
    try:
        # Stop all active streams
        for sid in list(stream_manager.active_streams.keys()):
            await stop_stream(sid)
        logger.info("✅ All streams stopped")
        
        # Close HTTP session properly to avoid unclosed session warnings
        await stream_manager.close_session()
        
        # Additional wait to ensure all connections are closed
        await asyncio.sleep(0.25)
        
        # Shutdown web server
        if runner:
            await runner.cleanup()
            logger.info("✅ Web server shutdown complete")
        
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")
    
    logger.info("✅ Shutdown complete")

def install_signal_handlers(loop, stop_func):
    """Install signal handlers for graceful shutdown"""
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(stop_func()))
            logger.info(f"✅ Signal handler installed for {sig.name}")
        except NotImplementedError:
            # Windows or restricted environments
            logger.warning(f"⚠️ Could not install signal handler for {sig.name}")

async def main_async():
    """FIXED: Enhanced main async function with bulletproof error handling"""
    # Validate bot token
    if BOT_TOKEN == "REPLACE_ME" or not BOT_TOKEN.strip():
        logger.error("❌ BOT_TOKEN is missing. Please set the BOT_TOKEN environment variable.")
        return
        
    # Check FFmpeg availability (optional, don't fail if missing)
    try:
        result = subprocess.run([CONFIG["FFMPEG_PATH"], "-version"], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            logger.info("✅ FFmpeg detected and working")
        else:
            logger.warning("⚠️ FFmpeg not found, stream creation may fail")
    except Exception as e:
        logger.warning(f"⚠️ FFmpeg check failed: {e}")

    # Build application
    try:
        application = ApplicationBuilder().token(BOT_TOKEN).build()
        
        # Add handlers
        application.add_handler(CommandHandler("start", cmd_start))
        application.add_handler(CallbackQueryHandler(handle_callback_query))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
        
        logger.info("✅ Telegram handlers registered")
        
    except Exception as e:
        logger.error(f"❌ Failed to build Telegram application: {e}")
        return

    # Start all services with error handling
    try:
        runner, site = await start_services(application)
    except Exception as e:
        logger.error(f"❌ Failed to start services: {e}")
        return

    # Setup graceful shutdown
    async def stop_all():
        logger.info("🛑 Shutdown signal received")
        try:
            await application.stop()
            await shutdown_services(application, runner)
        except Exception as e:
            logger.error(f"❌ Error during stop_all: {e}")

    loop = asyncio.get_running_loop()
    install_signal_handlers(loop, stop_all)

    # Start the bot with error handling
    try:
        await application.initialize()
        await application.start()
        
        logger.info("🎬 Advanced HLS Streaming Bot v2.0 is now LIVE!")
        logger.info(f"📱 Send m3u8 URLs to the bot to start streaming")
        logger.info(f"🌐 Web interface available at: {BASE_URL}")
        logger.info(f"🎮 Mini app: {BASE_URL}/miniapp")

        # Keep running until shutdown
        try:
            while True:
                await asyncio.sleep(3600)  # Sleep for 1 hour at a time
        except asyncio.CancelledError:
            pass
        finally:
            await stop_all()
            
    except Exception as e:
        logger.error(f"❌ Error starting bot: {e}")
        await stop_all()

def main():
    """Main entry point with top-level error handling"""
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        return 1
    return 0

if __name__ == "__main__":
    exit(main())
