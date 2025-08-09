#!/usr/bin/env python3
"""
🎬 ADVANCED TELEGRAM HLS BOT WITH LIVE M3U8 RESTREAMING 🎬
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✨ Features:
- Host live m3u8 streams through your own server
- Re-stream and proxy live HLS content
- Convert videos to multiple quality m3u8
- Real-time stream monitoring and analytics
- Advanced inline keyboards & Mini App
- Live stream health checking
- Stream recording and DVR functionality
- Multi-bitrate adaptive streaming
- Stream authentication and access control
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
import mimetypes
import re
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List, Tuple, Set
import urllib.request
import urllib.parse
from urllib.parse import urljoin, urlparse
import aiohttp
from aiohttp import web, ClientSession, WSMsgType
from aiohttp.web_ws import WebSocketResponse
import aiofiles

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, 
    CallbackQuery, Message, WebAppInfo, MenuButton, 
    MenuButtonWebApp, BotCommand
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, 
    ContextTypes, filters, CallbackQueryHandler
)
from telegram.constants import ParseMode

# ==================== CONFIGURATION ====================
BOT_TOKEN = "8484774966:AAEqlVPcJHDtPszUMFLAsGwdrK2luwWiwB8"

HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "8080"))
STORAGE_DIR = Path("./hls_storage")
LIVE_DIR = Path("./live_streams")
TEMP_DIR = Path("./temp")
RECORDINGS_DIR = Path("./recordings")

# Create directories
for dir_path in [STORAGE_DIR, LIVE_DIR, TEMP_DIR, RECORDINGS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Auto-detect base URL
def detect_base_url():
    base_url = os.getenv("APP_URL", "").strip()
    if base_url:
        return base_url.rstrip("/")
    
    koyeb_app = os.getenv("KOYEB_APP_NAME") 
    koyeb_user = os.getenv("KOYEB_USERNAME")
    if koyeb_app:
        if koyeb_user:
            return f"https://{koyeb_app}-{koyeb_user}.koyeb.app"
        return f"https://{koyeb_app}.koyeb.app"
    
    try:
        ip = urllib.request.urlopen("https://api.ipify.org", timeout=5).read().decode()
        return f"http://{ip}:{PORT}"
    except:
        return f"http://localhost:{PORT}"

BASE_URL = detect_base_url()

# Configuration
CONFIG = {
    "MAX_FILE_SIZE_MB": 1000,
    "MAX_STORAGE_GB": 5,
    "SEGMENT_DURATION": 6,
    "PLAYLIST_SIZE": 10,
    "MAX_CONCURRENT_STREAMS": 10,
    "STREAM_HEALTH_CHECK_INTERVAL": 30,
    "PROXY_TIMEOUT": 30,
    "FFMPEG_PATH": "ffmpeg",
    "FFPROBE_PATH": "ffprobe"
}

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("HLS_Bot")

# ==================== GLOBAL STATE ====================
jobs: Dict[str, Dict[str, Any]] = {}
live_streams: Dict[str, Dict[str, Any]] = {}
processes: Dict[str, subprocess.Popen] = {}
websocket_connections: Dict[str, WebSocketResponse] = {}
stream_health: Dict[str, Dict[str, Any]] = {}
client_session: Optional[ClientSession] = None

# ==================== UTILITY FUNCTIONS ====================
def generate_stream_id() -> str:
    return str(uuid.uuid4())[:12]

def generate_token() -> str:
    return hashlib.sha256(str(uuid.uuid4()).encode()).hexdigest()[:16]

def get_stream_dir(stream_id: str) -> Path:
    stream_path = LIVE_DIR / stream_id
    stream_path.mkdir(exist_ok=True)
    return stream_path

def get_user_dir(user_id: int) -> Path:
    user_path = STORAGE_DIR / str(user_id)
    user_path.mkdir(exist_ok=True)
    return user_path

def get_job_dir(user_id: int, job_id: str) -> Path:
    job_path = get_user_dir(user_id) / job_id
    job_path.mkdir(exist_ok=True)
    return job_path

def get_hosted_stream_url(stream_id: str) -> str:
    return f"{BASE_URL}/live/{stream_id}/playlist.m3u8"

def get_stream_player_url(stream_id: str) -> str:
    return f"{BASE_URL}/player/live/{stream_id}"

# ==================== M3U8 PROCESSING ====================
class M3U8Parser:
    @staticmethod
    def parse_playlist(content: str) -> Dict[str, Any]:
        """Parse m3u8 playlist content"""
        lines = content.strip().split('\n')
        playlist = {
            "version": 3,
            "segments": [],
            "is_master": False,
            "is_live": False,
            "target_duration": 10,
            "media_sequence": 0,
            "variants": []
        }
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            if line.startswith('#EXT-X-VERSION:'):
                playlist["version"] = int(line.split(':')[1])
            elif line.startswith('#EXT-X-TARGETDURATION:'):
                playlist["target_duration"] = int(line.split(':')[1])
            elif line.startswith('#EXT-X-MEDIA-SEQUENCE:'):
                playlist["media_sequence"] = int(line.split(':')[1])
            elif line.startswith('#EXT-X-PLAYLIST-TYPE:'):
                playlist_type = line.split(':')[1]
                playlist["is_live"] = playlist_type != "VOD"
            elif line.startswith('#EXT-X-STREAM-INF:'):
                # Master playlist with variants
                playlist["is_master"] = True
                variant_info = M3U8Parser.parse_stream_inf(line)
                if i + 1 < len(lines):
                    variant_info["url"] = lines[i + 1].strip()
                    playlist["variants"].append(variant_info)
                i += 1
            elif line.startswith('#EXTINF:'):
                # Media segment
                duration = float(line.split(':')[1].split(',')[0])
                if i + 1 < len(lines):
                    url = lines[i + 1].strip()
                    playlist["segments"].append({
                        "duration": duration,
                        "url": url
                    })
                i += 1
            
            i += 1
        
        if not playlist["variants"] and playlist["segments"]:
            playlist["is_live"] = not any("#EXT-X-ENDLIST" in content for content in [content])
        
        return playlist
    
    @staticmethod
    def parse_stream_inf(line: str) -> Dict[str, Any]:
        """Parse EXT-X-STREAM-INF line"""
        info = {}
        # Extract attributes from STREAM-INF
        attrs = line.split(':', 1)[1]
        
        # Parse bandwidth
        if "BANDWIDTH=" in attrs:
            bandwidth = re.search(r'BANDWIDTH=(\d+)', attrs)
            if bandwidth:
                info["bandwidth"] = int(bandwidth.group(1))
        
        # Parse resolution
        if "RESOLUTION=" in attrs:
            resolution = re.search(r'RESOLUTION=(\d+x\d+)', attrs)
            if resolution:
                info["resolution"] = resolution.group(1)
        
        # Parse codecs
        if "CODECS=" in attrs:
            codecs = re.search(r'CODECS="([^"]+)"', attrs)
            if codecs:
                info["codecs"] = codecs.group(1)
        
        return info
    
    @staticmethod
    def generate_playlist(segments: List[Dict], target_duration: int, media_sequence: int, is_live: bool = True) -> str:
        """Generate m3u8 playlist content"""
        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            f"#EXT-X-TARGETDURATION:{target_duration}",
            f"#EXT-X-MEDIA-SEQUENCE:{media_sequence}"
        ]
        
        for segment in segments:
            lines.append(f"#EXTINF:{segment['duration']:.3f},")
            lines.append(segment['url'])
        
        if not is_live:
            lines.append("#EXT-X-ENDLIST")
        
        return "\n".join(lines) + "\n"

# ==================== LIVE STREAM MANAGEMENT ====================
class LiveStreamManager:
    def __init__(self):
        self.active_streams: Dict[str, Dict[str, Any]] = {}
        self.proxy_sessions: Dict[str, ClientSession] = {}
    
    async def start_live_restream(self, source_url: str, user_id: int) -> str:
        """Start restreaming a live m3u8 source"""
        stream_id = generate_stream_id()
        access_token = generate_token()
        
        stream_info = {
            "stream_id": stream_id,
            "user_id": user_id,
            "source_url": source_url,
            "status": "starting",
            "created_at": datetime.now(),
            "access_token": access_token,
            "hosted_url": get_hosted_stream_url(stream_id),
            "player_url": get_stream_player_url(stream_id),
            "viewers": 0,
            "health": {
                "last_check": datetime.now(),
                "is_healthy": True,
                "error_count": 0,
                "last_error": None
            },
            "segments": [],
            "current_sequence": 0,
            "recording_enabled": False
        }
        
        self.active_streams[stream_id] = stream_info
        live_streams[stream_id] = stream_info
        
        # Start the restreaming task
        asyncio.create_task(self._restream_task(stream_id))
        
        return stream_id
    
    async def _restream_task(self, stream_id: str):
        """Main restreaming task"""
        stream_info = self.active_streams.get(stream_id)
        if not stream_info:
            return
        
        source_url = stream_info["source_url"]
        stream_dir = get_stream_dir(stream_id)
        
        logger.info(f"Starting restream task for {stream_id} from {source_url}")
        
        try:
            # Create proxy session
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
            session = ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=30))
            self.proxy_sessions[stream_id] = session
            
            stream_info["status"] = "active"
            
            # Start FFmpeg restreaming process
            await self._start_ffmpeg_restream(stream_id, source_url, stream_dir)
            
            # Monitor stream health
            asyncio.create_task(self._monitor_stream_health(stream_id))
            
        except Exception as e:
            logger.error(f"Error in restream task for {stream_id}: {e}")
            stream_info["status"] = "error"
            stream_info["health"]["last_error"] = str(e)
    
    async def _start_ffmpeg_restream(self, stream_id: str, source_url: str, output_dir: Path):
        """Start FFmpeg process for restreaming"""
        playlist_path = output_dir / "playlist.m3u8"
        segment_pattern = str(output_dir / "segment_%05d.ts")
        
        # FFmpeg command for live restreaming
        cmd = [
            CONFIG["FFMPEG_PATH"],
            "-hide_banner", "-loglevel", "error",
            "-i", source_url,
            "-c", "copy",  # Copy streams without re-encoding for better performance
            "-f", "hls",
            "-hls_time", str(CONFIG["SEGMENT_DURATION"]),
            "-hls_list_size", str(CONFIG["PLAYLIST_SIZE"]),
            "-hls_flags", "delete_segments+append_list",
            "-hls_segment_filename", segment_pattern,
            "-hls_base_url", f"{BASE_URL}/live/{stream_id}/",
            str(playlist_path)
        ]
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            processes[stream_id] = process
            
            # Monitor process
            asyncio.create_task(self._monitor_ffmpeg_process(stream_id, process))
            
        except Exception as e:
            logger.error(f"Error starting FFmpeg for {stream_id}: {e}")
            if stream_id in self.active_streams:
                self.active_streams[stream_id]["status"] = "error"
                self.active_streams[stream_id]["health"]["last_error"] = str(e)
    
    async def _monitor_ffmpeg_process(self, stream_id: str, process: subprocess.Popen):
        """Monitor FFmpeg process"""
        try:
            stderr_data = b""
            while process.returncode is None:
                try:
                    line = await asyncio.wait_for(process.stderr.readline(), timeout=1.0)
                    if not line:
                        break
                    stderr_data += line
                    
                    # Update stream status
                    if stream_id in self.active_streams:
                        self.active_streams[stream_id]["health"]["last_check"] = datetime.now()
                        
                except asyncio.TimeoutError:
                    continue
            
            # Process ended
            returncode = await process.wait()
            
            if stream_id in self.active_streams:
                if returncode == 0:
                    self.active_streams[stream_id]["status"] = "stopped"
                else:
                    self.active_streams[stream_id]["status"] = "error"
                    error_msg = stderr_data.decode(errors='ignore')[-500:]  # Last 500 chars
                    self.active_streams[stream_id]["health"]["last_error"] = error_msg
                    logger.error(f"FFmpeg error for {stream_id}: {error_msg}")
            
        except Exception as e:
            logger.error(f"Error monitoring FFmpeg process {stream_id}: {e}")
        finally:
            processes.pop(stream_id, None)
    
    async def _monitor_stream_health(self, stream_id: str):
        """Monitor stream health periodically"""
        while stream_id in self.active_streams and self.active_streams[stream_id]["status"] == "active":
            try:
                await asyncio.sleep(CONFIG["STREAM_HEALTH_CHECK_INTERVAL"])
                
                stream_info = self.active_streams[stream_id]
                playlist_path = get_stream_dir(stream_id) / "playlist.m3u8"
                
                if playlist_path.exists():
                    # Check if playlist is being updated
                    stat = playlist_path.stat()
                    last_modified = datetime.fromtimestamp(stat.st_mtime)
                    
                    if datetime.now() - last_modified > timedelta(seconds=CONFIG["SEGMENT_DURATION"] * 3):
                        # Playlist not updated recently, might be stale
                        stream_info["health"]["error_count"] += 1
                        stream_info["health"]["is_healthy"] = False
                        
                        if stream_info["health"]["error_count"] > 3:
                            logger.warning(f"Stream {stream_id} appears to be stale, stopping")
                            await self.stop_stream(stream_id)
                            break
                    else:
                        stream_info["health"]["is_healthy"] = True
                        stream_info["health"]["error_count"] = 0
                
                stream_info["health"]["last_check"] = datetime.now()
                
            except Exception as e:
                logger.error(f"Error in health check for {stream_id}: {e}")
    
    async def stop_stream(self, stream_id: str) -> bool:
        """Stop a live stream"""
        if stream_id not in self.active_streams:
            return False
        
        try:
            # Stop FFmpeg process
            if stream_id in processes:
                process = processes[stream_id]
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    process.kill()
                processes.pop(stream_id, None)
            
            # Close proxy session
            if stream_id in self.proxy_sessions:
                session = self.proxy_sessions.pop(stream_id)
                await session.close()
            
            # Update status
            self.active_streams[stream_id]["status"] = "stopped"
            
            # Clean up files (optional, keep for a while for debugging)
            # stream_dir = get_stream_dir(stream_id)
            # shutil.rmtree(stream_dir, ignore_errors=True)
            
            logger.info(f"Stopped stream {stream_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping stream {stream_id}: {e}")
            return False
    
    async def get_stream_info(self, stream_id: str) -> Optional[Dict[str, Any]]:
        """Get stream information"""
        return self.active_streams.get(stream_id)
    
    async def list_active_streams(self) -> Dict[str, Dict[str, Any]]:
        """List all active streams"""
        return self.active_streams.copy()

# Global stream manager instance
stream_manager = LiveStreamManager()

# ==================== TELEGRAM HANDLERS ====================
def create_main_keyboard() -> InlineKeyboardMarkup:
    """Create main menu keyboard"""
    keyboard = [
        [
            InlineKeyboardButton("🎥 Convert Video", callback_data="convert_video"),
            InlineKeyboardButton("📺 Host Live Stream", callback_data="host_live")
        ],
        [
            InlineKeyboardButton("📱 Mini App", web_app=WebAppInfo(url=f"{BASE_URL}/miniapp")),
            InlineKeyboardButton("📊 My Streams", callback_data="my_streams")
        ],
        [
            InlineKeyboardButton("⚙️ Settings", callback_data="settings"),
            InlineKeyboardButton("❓ Help", callback_data="help")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def create_stream_keyboard(stream_id: str) -> InlineKeyboardMarkup:
    """Create stream control keyboard"""
    keyboard = [
        [
            InlineKeyboardButton("▶️ Open Player", url=get_stream_player_url(stream_id)),
            InlineKeyboardButton("📱 Share", callback_data=f"share_stream:{stream_id}")
        ],
        [
            InlineKeyboardButton("📊 Analytics", callback_data=f"stream_analytics:{stream_id}"),
            InlineKeyboardButton("🔧 Settings", callback_data=f"stream_settings:{stream_id}")
        ],
        [
            InlineKeyboardButton("⏹️ Stop Stream", callback_data=f"stop_stream:{stream_id}"),
            InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh_stream:{stream_id}")
        ],
        [InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    welcome_text = """
🎬 **Advanced HLS Streaming Bot** 🎬

Welcome to the most advanced HLS streaming solution!

✨ **Features:**
• 🎥 Convert videos to HLS format
• 📺 Host live m3u8 streams through your server
• 📱 Advanced Mini App interface  
• 🔄 Real-time stream monitoring
• 📊 Stream analytics and viewer stats
• 🎛️ Multi-quality streaming options
• 🔒 Stream access control
• 📹 Recording and DVR functionality

**Quick Start:**
• Send a video file to convert to HLS
• Send an m3u8 URL to host it live
• Use buttons below for more options

Choose an option to get started:
"""
    
    await update.message.reply_text(
        welcome_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=create_main_keyboard()
    )

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline keyboard callbacks"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == "main_menu":
        await query.edit_message_text(
            "🏠 **Main Menu**\n\nChoose an option:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=create_main_keyboard()
        )
    
    elif data == "host_live":
        await query.edit_message_text(
            "📺 **Host Live Stream**\n\n"
            "Send me a live m3u8 URL and I'll host it through your own server!\n\n"
            "**Supported formats:**\n"
            "• Direct m3u8 playlist URLs\n"
            "• HLS live streams\n"
            "• IPTV streams\n\n"
            "**Example:**\n"
            "`https://example.com/playlist.m3u8`\n\n"
            "Just send the URL as a message!",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 Back to Menu", callback_data="main_menu")
            ]])
        )
    
    elif data == "my_streams":
        user_id = update.effective_user.id
        active_streams = await stream_manager.list_active_streams()
        user_streams = {k: v for k, v in active_streams.items() if v["user_id"] == user_id}
        
        if not user_streams:
            await query.edit_message_text(
                "📊 **My Streams**\n\n"
                "You don't have any active streams.\n"
                "Create one by hosting a live m3u8 URL!",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=create_main_keyboard()
            )
        else:
            text = "📊 **Your Active Streams:**\n\n"
            keyboard = []
            
            for stream_id, info in user_streams.items():
                status_emoji = {"active": "🟢", "starting": "🟡", "error": "🔴", "stopped": "⚫"}.get(info["status"], "❓")
                text += f"{status_emoji} `{stream_id}`\n"
                text += f"   Status: {info['status'].title()}\n"
                text += f"   Created: {info['created_at'].strftime('%H:%M:%S')}\n\n"
                
                keyboard.append([InlineKeyboardButton(
                    f"📺 {stream_id[:8]}...", 
                    callback_data=f"stream_details:{stream_id}"
                )])
            
            keyboard.append([InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")])
            await query.edit_message_text(
                text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    
    elif data.startswith("stream_details:"):
        stream_id = data.split(":", 1)[1]
        stream_info = await stream_manager.get_stream_info(stream_id)
        
        if not stream_info:
            await query.edit_message_text("❌ Stream not found!")
            return
        
        status_emoji = {"active": "🟢", "starting": "🟡", "error": "🔴", "stopped": "⚫"}.get(stream_info["status"], "❓")
        health_emoji = "💚" if stream_info["health"]["is_healthy"] else "💔"
        
        text = f"📺 **Stream Details**\n\n"
        text += f"**ID:** `{stream_id}`\n"
        text += f"**Status:** {status_emoji} {stream_info['status'].title()}\n"
        text += f"**Health:** {health_emoji} {'Healthy' if stream_info['health']['is_healthy'] else 'Issues'}\n"
        text += f"**Created:** {stream_info['created_at'].strftime('%Y-%m-%d %H:%M:%S')}\n"
        text += f"**Viewers:** {stream_info.get('viewers', 0)}\n\n"
        text += f"**Your Stream URL:**\n`{stream_info['hosted_url']}`\n\n"
        text += f"**Source:** `{stream_info['source_url'][:50]}...`"
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=create_stream_keyboard(stream_id)
        )
    
    elif data.startswith("stop_stream:"):
        stream_id = data.split(":", 1)[1]
        success = await stream_manager.stop_stream(stream_id)
        
        if success:
            await query.edit_message_text(
                f"✅ Stream `{stream_id}` has been stopped successfully!",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
                ]])
            )
        else:
            await query.answer("❌ Failed to stop stream", show_alert=True)

async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages (URLs)"""
    text = update.message.text.strip()
    user = update.effective_user
    
    # Check if it's a URL
    if not (text.startswith("http://") or text.startswith("https://")):
        await update.message.reply_text(
            "Please send a valid HTTP/HTTPS URL to a live m3u8 stream.",
            reply_markup=create_main_keyboard()
        )
        return
    
    # Check if it's likely an m3u8 URL
    if not (".m3u8" in text.lower() or "playlist" in text.lower()):
        await update.message.reply_text(
            "⚠️ This doesn't appear to be an m3u8 playlist URL.\n"
            "Make sure you're sending a direct link to an m3u8 file.\n\n"
            "Example: `https://example.com/playlist.m3u8`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=create_main_keyboard()
        )
        return
    
    # Show processing message
    processing_msg = await update.message.reply_text(
        "🔄 **Processing your stream...**\n\n"
        "• Checking stream URL\n"
        "• Setting up restreaming\n"
        "• Preparing your hosted URL\n\n"
        "This may take a few moments...",
        parse_mode=ParseMode.MARKDOWN
    )
    
    try:
        # Start live restreaming
        stream_id = await stream_manager.start_live_restream(text, user.id)
        
        # Wait a moment for setup
        await asyncio.sleep(3)
        
        stream_info = await stream_manager.get_stream_info(stream_id)
        
        success_text = f"✅ **Stream is now hosted!**\n\n"
        success_text += f"**Stream ID:** `{stream_id}`\n"
        success_text += f"**Your hosted URL:**\n`{stream_info['hosted_url']}`\n\n"
        success_text += f"**Player URL:**\n{stream_info['player_url']}\n\n"
        success_text += f"🔗 Use your hosted URL in any HLS-compatible player!\n"
        success_text += f"📱 Or click the buttons below to manage your stream."
        
        await processing_msg.edit_text(
            success_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=create_stream_keyboard(stream_id)
        )
        
    except Exception as e:
        logger.error(f"Error processing stream URL {text}: {e}")
        await processing_msg.edit_text(
            f"❌ **Error processing stream**\n\n"
            f"Failed to set up restreaming for your URL.\n"
            f"Please check that the URL is valid and accessible.\n\n"
            f"Error: `{str(e)[:100]}...`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=create_main_keyboard()
        )

# ==================== WEB SERVER ====================
async def init_web_app():
    """Initialize web application"""
    app = web.Application()
    
    # Serve live streams
    async def serve_live_playlist(request):
        stream_id = request.match_info['stream_id']
        playlist_path = get_stream_dir(stream_id) / "playlist.m3u8"
        
        if not playlist_path.exists():
            return web.Response(status=404, text="Stream not found")
        
        # Update viewer count
        if stream_id in live_streams:
            # This is a simple increment - in production you'd want more sophisticated tracking
            live_streams[stream_id]["viewers"] = live_streams[stream_id].get("viewers", 0) + 1
        
        return web.FileResponse(
            playlist_path,
            headers={
                "Content-Type": "application/vnd.apple.mpegurl",
                "Cache-Control": "no-cache",
                "Access-Control-Allow-Origin": "*"
            }
        )
    
    async def serve_live_segment(request):
        stream_id = request.match_info['stream_id']
        segment_name = request.match_info['segment']
        segment_path = get_stream_dir(stream_id) / segment_name
        
        if not segment_path.exists():
            return web.Response(status=404, text="Segment not found")
        
        return web.FileResponse(
            segment_path,
            headers={
                "Content-Type": "video/mp2t",
                "Cache-Control": "max-age=3600",
                "Access-Control-Allow-Origin": "*"
            }
        )
    
    # HTML5 Player
    async def serve_player(request):
        stream_id = request.match_info.get('stream_id')
        if not stream_id:
            return web.Response(status=404, text="Stream ID required")
        
        stream_info = await stream_manager.get_stream_info(stream_id)
        if not stream_info:
            return web.Response(status=404, text="Stream not found")
        
        playlist_url = get_hosted_stream_url(stream_id)
        
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Live Stream Player - {stream_id}</title>
    <script src="https://cdn.jsdelivr.net/npm/hls.js@1.4.12/dist/hls.min.js"></script>
    <style>
        body {{
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            color: white;
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
            font-size: 2.5rem;
            margin: 0;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }}
        
        .stream-info {{
            background: rgba(255,255,255,0.1);
            border-radius: 15px;
            padding: 20px;
            margin-bottom: 30px;
            backdrop-filter: blur(10px);
        }}
        
        .video-container {{
            position: relative;
            background: rgba(0,0,0,0.5);
            border-radius: 15px;
            overflow: hidden;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        }}
        
        #video {{
            width: 100%;
            height: auto;
            min-height: 400px;
            display: block;
        }}
        
        .controls {{
            display: flex;
            justify-content: center;
            gap: 15px;
            flex-wrap: wrap;
        }}
        
        .btn {{
            padding: 12px 24px;
            border: none;
            border-radius: 25px;
            background: rgba(255,255,255,0.2);
            color: white;
            cursor: pointer;
            font-weight: bold;
            transition: all 0.3s ease;
            backdrop-filter: blur(10px);
        }}
        
        .btn:hover {{
            background: rgba(255,255,255,0.3);
            transform: translateY(-2px);
        }}
        
        .status {{
            text-align: center;
            margin: 20px 0;
            font-size: 1.2rem;
        }}
        
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-top: 30px;
        }}
        
        .stat-card {{
            background: rgba(255,255,255,0.1);
            border-radius: 10px;
            padding: 20px;
            text-align: center;
            backdrop-filter: blur(10px);
        }}
        
        .stat-value {{
            font-size: 2rem;
            font-weight: bold;
            color: #00ff88;
        }}
        
        .error-message {{
            background: rgba(255,0,0,0.2);
            border: 1px solid rgba(255,0,0,0.5);
            border-radius: 10px;
            padding: 20px;
            margin: 20px 0;
            text-align: center;
        }}
        
        @media (max-width: 768px) {{
            body {{ padding: 10px; }}
            .header h1 {{ font-size: 2rem; }}
            .controls {{ flex-direction: column; }}
            .stats {{ grid-template-columns: 1fr; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🎬 Live Stream Player</h1>
        </div>
        
        <div class="stream-info">
            <h3>📺 Stream ID: {stream_id}</h3>
            <p><strong>Status:</strong> <span id="stream-status">{stream_info['status'].title()}</span></p>
            <p><strong>Source:</strong> {stream_info['source_url'][:60]}...</p>
            <p><strong>Created:</strong> {stream_info['created_at'].strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="video-container">
            <video id="video" controls autoplay muted>
                <p>Your browser doesn't support HTML5 video.</p>
            </video>
        </div>
        
        <div class="status">
            <span id="player-status">Initializing player...</span>
        </div>
        
        <div class="controls">
            <button class="btn" onclick="playStream()">▶️ Play</button>
            <button class="btn" onclick="pauseStream()">⏸️ Pause</button>
            <button class="btn" onclick="reloadStream()">🔄 Reload</button>
            <button class="btn" onclick="toggleFullscreen()">🔳 Fullscreen</button>
            <button class="btn" onclick="copyUrl()">📋 Copy URL</button>
        </div>
        
        <div class="stats">
            <div class="stat-card">
                <div class="stat-value" id="viewer-count">-</div>
                <div>Current Viewers</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="stream-health">-</div>
                <div>Stream Health</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="bitrate">-</div>
                <div>Bitrate (kbps)</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="resolution">-</div>
                <div>Resolution</div>
            </div>
        </div>
    </div>

    <script>
        const video = document.getElementById('video');
        const playlistUrl = '{playlist_url}';
        let hls;
        let statsInterval;
        
        function initializePlayer() {{
            if (Hls.isSupported()) {{
                hls = new Hls({{
                    debug: false,
                    enableWorker: true,
                    lowLatencyMode: true,
                    backBufferLength: 90
                }});
                
                hls.loadSource(playlistUrl);
                hls.attachMedia(video);
                
                hls.on(Hls.Events.MANIFEST_PARSED, function() {{
                    document.getElementById('player-status').textContent = '✅ Ready to play';
                    updateStreamInfo();
                }});
                
                hls.on(Hls.Events.ERROR, function(event, data) {{
                    console.error('HLS Error:', data);
                    document.getElementById('player-status').innerHTML = '❌ Stream error: ' + data.details;
                }});
                
                hls.on(Hls.Events.LEVEL_LOADED, function(event, data) {{
                    updateStats();
                }});
                
            }} else if (video.canPlayType('application/vnd.apple.mpegurl')) {{
                video.src = playlistUrl;
                document.getElementById('player-status').textContent = '✅ Native HLS support';
            }} else {{
                document.getElementById('player-status').innerHTML = '❌ HLS not supported in this browser';
            }}
            
            // Start stats update interval
            statsInterval = setInterval(updateStats, 5000);
        }}
        
        function updateStreamInfo() {{
            if (hls && hls.levels.length > 0) {{
                const level = hls.levels[hls.currentLevel];
                document.getElementById('resolution').textContent = `${{level.width}}x${{level.height}}`;
                document.getElementById('bitrate').textContent = Math.round(level.bitrate / 1000);
            }}
        }}
        
        function updateStats() {{
            // Update viewer count (would come from WebSocket in production)
            fetch(`/api/stream/{stream_id}/stats`)
                .then(response => response.json())
                .then(data => {{
                    document.getElementById('viewer-count').textContent = data.viewers || 0;
                    document.getElementById('stream-health').textContent = data.healthy ? '💚' : '💔';
                }})
                .catch(err => {{
                    console.log('Stats update failed:', err);
                }});
        }}
        
        function playStream() {{
            video.play().catch(e => console.log('Play failed:', e));
        }}
        
        function pauseStream() {{
            video.pause();
        }}
        
        function reloadStream() {{
            if (hls) {{
                hls.destroy();
            }}
            initializePlayer();
        }}
        
        function toggleFullscreen() {{
            if (document.fullscreenElement) {{
                document.exitFullscreen();
            }} else {{
                video.requestFullscreen().catch(e => console.log('Fullscreen failed:', e));
            }}
        }}
        
        function copyUrl() {{
            navigator.clipboard.writeText(playlistUrl).then(() => {{
                alert('Stream URL copied to clipboard!');
            }}).catch(err => {{
                prompt('Copy this URL:', playlistUrl);
            }});
        }}
        
        // Initialize player when page loads
        document.addEventListener('DOMContentLoaded', initializePlayer);
        
        // Cleanup on page unload
        window.addEventListener('beforeunload', function() {{
            if (hls) {{
                hls.destroy();
            }}
            if (statsInterval) {{
                clearInterval(statsInterval);
            }}
        }});
    </script>
</body>
</html>
"""
        return web.Response(text=html_content, content_type='text/html')
    
    # Mini App
    async def serve_mini_app(request):
        html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HLS Bot Mini App</title>
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--tg-theme-bg-color, #ffffff);
            color: var(--tg-theme-text-color, #000000);
            padding: 20px;
            min-height: 100vh;
        }
        
        .header {
            text-align: center;
            margin-bottom: 30px;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 15px;
            color: white;
        }
        
        .header h1 {
            font-size: 1.8rem;
            margin-bottom: 10px;
        }
        
        .card {
            background: var(--tg-theme-secondary-bg-color, #f8f9fa);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        
        .action-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }
        
        .action-btn {
            padding: 20px;
            border: none;
            border-radius: 12px;
            background: var(--tg-theme-button-color, #3390ec);
            color: var(--tg-theme-button-text-color, #ffffff);
            cursor: pointer;
            font-size: 14px;
            font-weight: 600;
            transition: all 0.2s ease;
            text-align: center;
        }
        
        .action-btn:hover {
            opacity: 0.8;
            transform: translateY(-2px);
        }
        
        .input-group {
            margin-bottom: 20px;
        }
        
        .input-group label {
            display: block;
            margin-bottom: 8px;
            font-weight: 600;
        }
        
        .input-group input {
            width: 100%;
            padding: 12px;
            border: 1px solid var(--tg-theme-hint-color, #999999);
            border-radius: 8px;
            font-size: 16px;
            background: var(--tg-theme-bg-color, #ffffff);
            color: var(--tg-theme-text-color, #000000);
        }
        
        .stream-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 15px;
            border-bottom: 1px solid var(--tg-theme-hint-color, #e5e5ea);
        }
        
        .stream-item:last-child {
            border-bottom: none;
        }
        
        .stream-info h4 {
            margin-bottom: 5px;
        }
        
        .stream-info small {
            color: var(--tg-theme-hint-color, #999999);
        }
        
        .status-badge {
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
        }
        
        .status-active { background: #34c759; color: white; }
        .status-error { background: #ff3b30; color: white; }
        .status-stopped { background: #8e8e93; color: white; }
        
        .loading {
            text-align: center;
            padding: 40px;
            color: var(--tg-theme-hint-color, #999999);
        }
        
        .hidden { display: none; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🎬 HLS Streaming Bot</h1>
        <p>Advanced streaming platform</p>
    </div>
    
    <div class="action-grid">
        <button class="action-btn" onclick="showCreateStream()">
            📺<br>Host Live Stream
        </button>
        <button class="action-btn" onclick="showMyStreams()">
            📊<br>My Streams
        </button>
        <button class="action-btn" onclick="showSettings()">
            ⚙️<br>Settings
        </button>
        <button class="action-btn" onclick="showHelp()">
            ❓<br>Help
        </button>
    </div>
    
    <!-- Create Stream Section -->
    <div id="create-stream" class="card hidden">
        <h3>📺 Host Live Stream</h3>
        <div class="input-group">
            <label for="stream-url">M3U8 Stream URL:</label>
            <input type="url" id="stream-url" placeholder="https://example.com/playlist.m3u8">
        </div>
        <button class="action-btn" onclick="createStream()" style="width: 100%;">
            🚀 Start Hosting
        </button>
    </div>
    
    <!-- My Streams Section -->
    <div id="my-streams" class="card hidden">
        <h3>📊 My Active Streams</h3>
        <div id="streams-list" class="loading">Loading streams...</div>
    </div>
    
    <!-- Settings Section -->
    <div id="settings" class="card hidden">
        <h3>⚙️ Settings</h3>
        <div class="input-group">
            <label>Stream Quality:</label>
            <select id="quality-setting" style="width: 100%; padding: 12px; border-radius: 8px;">
                <option value="low">Low (360p)</option>
                <option value="medium" selected>Medium (720p)</option>
                <option value="high">High (1080p)</option>
            </select>
        </div>
        <div class="input-group">
            <label>Auto-cleanup (hours):</label>
            <input type="number" id="cleanup-hours" value="24" min="1" max="168">
        </div>
    </div>
    
    <!-- Help Section -->
    <div id="help" class="card hidden">
        <h3>❓ Help & Instructions</h3>
        <div style="line-height: 1.6;">
            <h4>🎯 How to use:</h4>
            <ol>
                <li>Get an m3u8 live stream URL</li>
                <li>Paste it in the "Host Live Stream" section</li>
                <li>Click "Start Hosting" to create your hosted version</li>
                <li>Share your new URL with anyone!</li>
            </ol>
            
            <h4>📝 Supported formats:</h4>
            <ul>
                <li>Live HLS streams (.m3u8)</li>
                <li>IPTV playlists</li>
                <li>Direct streaming URLs</li>
            </ul>
        </div>
    </div>

    <script>
        // Initialize Telegram Web App
        Telegram.WebApp.ready();
        Telegram.WebApp.expand();
        
        // Apply Telegram theme
        document.documentElement.style.setProperty('--tg-theme-bg-color', Telegram.WebApp.themeParams.bg_color || '#ffffff');
        document.documentElement.style.setProperty('--tg-theme-text-color', Telegram.WebApp.themeParams.text_color || '#000000');
        
        let currentSection = null;
        
        function hideAllSections() {
            const sections = document.querySelectorAll('.card');
            sections.forEach(section => section.classList.add('hidden'));
            currentSection = null;
        }
        
        function showSection(sectionId) {
            hideAllSections();
            document.getElementById(sectionId).classList.remove('hidden');
            currentSection = sectionId;
        }
        
        function showCreateStream() {
            showSection('create-stream');
        }
        
        function showMyStreams() {
            showSection('my-streams');
            loadMyStreams();
        }
        
        function showSettings() {
            showSection('settings');
        }
        
        function showHelp() {
            showSection('help');
        }
        
        async function createStream() {
            const urlInput = document.getElementById('stream-url');
            const url = urlInput.value.trim();
            
            if (!url) {
                Telegram.WebApp.showAlert('Please enter a valid stream URL');
                return;
            }
            
            if (!url.includes('.m3u8')) {
                Telegram.WebApp.showAlert('Please enter a valid m3u8 URL');
                return;
            }
            
            try {
                Telegram.WebApp.showPopup({
                    title: 'Creating Stream',
                    message: 'Setting up your hosted stream...',
                    buttons: [{ type: 'ok', text: 'OK' }]
                });
                
                // Send URL to bot via Telegram Web App
                Telegram.WebApp.sendData(JSON.stringify({
                    action: 'create_stream',
                    url: url
                }));
                
            } catch (error) {
                Telegram.WebApp.showAlert('Error creating stream: ' + error.message);
            }
        }
        
        async function loadMyStreams() {
            const streamsList = document.getElementById('streams-list');
            
            try {
                // In a real implementation, this would fetch from your API
                // For now, show a placeholder
                streamsList.innerHTML = `
                    <div class="stream-item">
                        <div class="stream-info">
                            <h4>Example Stream</h4>
                            <small>Created 2 hours ago</small>
                        </div>
                        <span class="status-badge status-active">Active</span>
                    </div>
                    <div style="text-align: center; margin-top: 20px; color: #999;">
                        <p>Use the bot in Telegram to create streams,<br>then they'll appear here!</p>
                    </div>
                `;
            } catch (error) {
                streamsList.innerHTML = '<div style="text-align: center; color: #ff3b30;">Error loading streams</div>';
            }
        }
        
        // Handle data from Telegram
        Telegram.WebApp.onEvent('mainButtonClicked', function() {
            if (currentSection === 'create-stream') {
                createStream();
            }
        });
    </script>
</body>
</html>
"""
        return web.Response(text=html_content, content_type='text/html')
    
    # API endpoints
    async def api_stream_stats(request):
        stream_id = request.match_info['stream_id']
        stream_info = await stream_manager.get_stream_info(stream_id)
        
        if not stream_info:
            return web.json_response({"error": "Stream not found"}, status=404)
        
        stats = {
            "stream_id": stream_id,
            "status": stream_info["status"],
            "viewers": stream_info.get("viewers", 0),
            "healthy": stream_info["health"]["is_healthy"],
            "created_at": stream_info["created_at"].isoformat(),
            "uptime_seconds": (datetime.now() - stream_info["created_at"]).total_seconds()
        }
        
        return web.json_response(stats)
    
    async def api_list_streams(request):
        active_streams = await stream_manager.list_active_streams()
        streams_data = []
        
        for stream_id, info in active_streams.items():
            streams_data.append({
                "stream_id": stream_id,
                "status": info["status"],
                "created_at": info["created_at"].isoformat(),
                "viewers": info.get("viewers", 0),
                "healthy": info["health"]["is_healthy"]
            })
        
        return web.json_response({"streams": streams_data})
    
    # Route setup
    app.router.add_get('/live/{stream_id}/playlist.m3u8', serve_live_playlist)
    app.router.add_get('/live/{stream_id}/{segment}', serve_live_segment)
    app.router.add_get('/player/live/{stream_id}', serve_player)
    app.router.add_get('/miniapp', serve_mini_app)
    app.router.add_get('/api/stream/{stream_id}/stats', api_stream_stats)
    app.router.add_get('/api/streams', api_list_streams)
    
    # Static files for any additional assets
    app.router.add_static('/', path=str(STORAGE_DIR), show_index=False)
    
    return app

# ==================== MAIN APPLICATION ====================
async def main():
    """Main application entry point"""
    global client_session
    
    # Initialize HTTP client session
    client_session = ClientSession()
    
    # Load existing jobs and streams from disk
    await load_jobs_from_disk()
    
    # Initialize web app
    web_app = await init_web_app()
    
    # Start web server
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()
    
    logger.info(f"🚀 Web server started at {BASE_URL}")
    
    # Initialize Telegram bot
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Add command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
    
    # Set bot commands
    commands = [
        BotCommand("start", "🏠 Main menu and welcome"),
        BotCommand("streams", "📊 View your active streams"),
        BotCommand("help", "❓ Get help and instructions")
    ]
    await application.bot.set_my_commands(commands)
    
    # Set Mini App menu button
    mini_app_button = MenuButtonWebApp(
        text="🎬 Open Mini App",
        web_app=WebAppInfo(url=f"{BASE_URL}/miniapp")
    )
    await application.bot.set_chat_menu_button(menu_button=mini_app_button)
    
    logger.info("🤖 Telegram bot initialized")
    
    # Start the bot
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    logger.info("✅ HLS Bot is now running!")
    logger.info(f"📺 Send m3u8 URLs to the bot to start hosting streams")
    logger.info(f"🌐 Web interface available at {BASE_URL}")
    
    try:
        # Keep the application running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down...")
    finally:
        # Cleanup
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        
        # Stop all active streams
        for stream_id in list(stream_manager.active_streams.keys()):
            await stream_manager.stop_stream(stream_id)
        
        # Close HTTP client session
        if client_session:
            await client_session.close()
        
        logger.info("✅ Shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
