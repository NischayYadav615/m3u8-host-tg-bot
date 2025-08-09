#!/usr/bin/env python3
"""
Complete M3U8 Bot with Auto URL Detection
Features: Stream hosting, video conversion, analytics, user management
"""

import os
import asyncio
import logging
import socket
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
import aiohttp
import aiofiles
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from pyrogram.errors import FloodWait, UserIsBlocked, ChatAdminRequired
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse, urljoin
import hashlib
import json
import re
from pathlib import Path
import subprocess
import tempfile
import time
import requests
from aiohttp import web
import ssl
import weakref
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
API_ID = int(os.getenv("API_ID", "24720215"))
API_HASH = os.getenv("API_HASH", "c0d3395590fecba19985f95d6300785e")
BOT_TOKEN = os.getenv("BOT_TOKEN", "7328634302:AAFGTN13P3EAhqTC5KyzsPH28n9-SQ7c51Y")
MONGO_URL = os.getenv("MONGO_URL", "mongodb+srv://Nischay999:Nischay999@cluster0.5kufo.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
DATABASE_NAME = os.getenv("DATABASE_NAME", "m3u8_bot")

# Auto-detect server configuration
def get_server_config():
    """Auto-detect server URL and port"""
    try:
        # Try to get from environment first
        base_url = os.getenv("BASE_URL")
        port = int(os.getenv("PORT", os.getenv("SERVER_PORT", "8080")))
        
        if not base_url:
            # Auto-detect for Koyeb and other platforms
            if os.getenv("KOYEB_PUBLIC_DOMAIN"):
                base_url = f"https://{os.getenv('KOYEB_PUBLIC_DOMAIN')}"
            elif os.getenv("RAILWAY_STATIC_URL"):
                base_url = f"https://{os.getenv('RAILWAY_STATIC_URL')}"
            elif os.getenv("RENDER_EXTERNAL_HOSTNAME"):
                base_url = f"https://{os.getenv('RENDER_EXTERNAL_HOSTNAME')}"
            else:
                # Default fallback
                hostname = socket.gethostname()
                base_url = f"http://{hostname}:{port}"
        
        logger.info(f"Server config - Base URL: {base_url}, Port: {port}")
        return base_url, port
        
    except Exception as e:
        logger.error(f"Error detecting server config: {e}")
        return "http://localhost:8080", 8080

BASE_URL, SERVER_PORT = get_server_config()

async def validate_bot_token(token: str) -> bool:
    """Validate bot token by making a test request"""
    try:
        url = f"https://api.telegram.org/bot{token}/getMe"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('ok', False)
                else:
                    logger.error(f"Bot token validation failed: HTTP {response.status}")
                    return False
    except Exception as e:
        logger.error(f"Bot token validation error: {e}")
        return False

# Initialize clients with validation
async def initialize_bot():
    """Initialize bot with proper error handling"""
    try:
        # Validate token first
        if not await validate_bot_token(BOT_TOKEN):
            logger.error("Bot token validation failed - token may be invalid or expired")
            raise ValueError("Invalid bot token")
        
        # Create bot client
        bot_client = Client(
            "m3u8_bot", 
            api_id=API_ID, 
            api_hash=API_HASH, 
            bot_token=BOT_TOKEN,
            workdir="./session"
        )
        
        logger.info("Bot client created successfully")
        return bot_client
        
    except Exception as e:
        logger.error(f"Bot initialization failed: {e}")
        raise

app = None  # Will be initialized in startup

# MongoDB setup with error handling
async def setup_mongodb():
    """Setup MongoDB connection with retry logic"""
    max_retries = 5
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            mongo_client = AsyncIOMotorClient(MONGO_URL, serverSelectionTimeoutMS=10000)
            # Test connection
            await mongo_client.admin.command('ping')
            logger.info("MongoDB connected successfully")
            return mongo_client
        except Exception as e:
            logger.error(f"MongoDB connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                logger.error("Failed to connect to MongoDB, using in-memory storage")
                return None

# Global variables
mongo_client = None
db = None
streams_collection = None
users_collection = None
analytics_collection = None

class InMemoryStorage:
    """Fallback in-memory storage when MongoDB is unavailable"""
    def __init__(self):
        self.streams = {}
        self.users = {}
        self.analytics = []
    
    async def insert_one(self, data):
        _id = hashlib.md5(str(data).encode()).hexdigest()
        data['_id'] = _id
        if 'stream_id' in data:
            self.streams[data['stream_id']] = data
        elif 'user_id' in data:
            self.users[data['user_id']] = data
        else:
            self.analytics.append(data)
        return type('Result', (), {'inserted_id': _id})()
    
    async def find_one(self, query):
        if 'stream_id' in query:
            return self.streams.get(query['stream_id'])
        elif 'user_id' in query:
            return self.users.get(query['user_id'])
        return None
    
    async def find(self, query=None):
        if query is None or query == {}:
            return list(self.streams.values()) + list(self.users.values())
        return []
    
    async def update_one(self, query, update):
        doc = await self.find_one(query)
        if doc:
            if '$set' in update:
                doc.update(update['$set'])
            if '$inc' in update:
                for key, value in update['$inc'].items():
                    doc[key] = doc.get(key, 0) + value
        return type('Result', (), {'modified_count': 1 if doc else 0})()

# Fallback storage
memory_storage = InMemoryStorage()

class M3U8Processor:
    """Handle M3U8 stream processing and conversion"""
    
    def __init__(self):
        self.temp_dir = Path(tempfile.gettempdir()) / "m3u8_bot"
        self.temp_dir.mkdir(exist_ok=True, parents=True)
        self.session = None
        
    async def get_session(self):
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={
                    'User-Agent': 'M3U8Bot/1.0 (Compatible Player)'
                }
            )
        return self.session
    
    async def validate_m3u8_url(self, url: str) -> bool:
        """Validate if URL is a valid M3U8 stream"""
        try:
            session = await self.get_session()
            async with session.get(url, allow_redirects=True) as response:
                if response.status == 200:
                    content = await response.text()
                    return ("#EXTM3U" in content or 
                           "#EXT-X-VERSION" in content or 
                           ".m3u8" in url.lower() or
                           ".ts" in content)
                return False
        except Exception as e:
            logger.error(f"Error validating M3U8 URL {url}: {e}")
            return False
    
    async def fetch_m3u8_content(self, url: str) -> Optional[str]:
        """Fetch M3U8 playlist content"""
        try:
            session = await self.get_session()
            async with session.get(url, allow_redirects=True) as response:
                if response.status == 200:
                    return await response.text()
        except Exception as e:
            logger.error(f"Error fetching M3U8 content from {url}: {e}")
        return None
    
    def generate_stream_id(self, url: str, user_id: int = None) -> str:
        """Generate unique stream ID"""
        data = f"{url}_{user_id}_{int(time.time())}"
        return hashlib.md5(data.encode()).hexdigest()[:12]
    
    async def convert_video_to_m3u8(self, video_url: str, stream_id: str) -> Optional[str]:
        """Convert video to M3U8 format using FFmpeg"""
        try:
            output_dir = self.temp_dir / stream_id
            output_dir.mkdir(exist_ok=True, parents=True)
            
            playlist_path = output_dir / "playlist.m3u8"
            
            # Check if FFmpeg is available
            try:
                subprocess.run(["ffmpeg", "-version"], capture_output=True, check=True)
            except (subprocess.CalledProcessError, FileNotFoundError):
                logger.error("FFmpeg not found, cannot convert video")
                return None
            
            # FFmpeg command for HLS conversion
            cmd = [
                "ffmpeg", "-y", "-i", video_url,
                "-c:v", "libx264", "-preset", "veryfast",
                "-c:a", "aac", "-b:a", "128k",
                "-hls_time", "6", "-hls_list_size", "0",
                "-hls_flags", "delete_segments",
                "-hls_segment_filename", str(output_dir / "segment_%06d.ts"),
                "-f", "hls", str(playlist_path)
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd, 
                stdout=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.PIPE,
                cwd=str(output_dir)
            )
            
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=300)
            
            if process.returncode == 0 and playlist_path.exists():
                return f"{BASE_URL}/stream/{stream_id}/playlist.m3u8"
            else:
                logger.error(f"FFmpeg conversion failed: {stderr.decode()}")
                return None
                
        except asyncio.TimeoutError:
            logger.error("Video conversion timed out")
            return None
        except Exception as e:
            logger.error(f"Error converting video to M3U8: {e}")
            return None
    
    async def proxy_m3u8_content(self, original_url: str, base_stream_url: str) -> str:
        """Modify M3U8 content to use proxy URLs"""
        try:
            content = await self.fetch_m3u8_content(original_url)
            if not content:
                return ""
            
            lines = content.split('\n')
            modified_lines = []
            
            base_original_url = '/'.join(original_url.split('/')[:-1]) + '/'
            
            for line in lines:
                line = line.strip()
                if line and not line.startswith('#'):
                    # This is a segment URL
                    if line.startswith('http'):
                        # Absolute URL
                        segment_url = line
                    else:
                        # Relative URL
                        segment_url = urljoin(base_original_url, line)
                    
                    # Replace with proxy URL
                    segment_id = hashlib.md5(segment_url.encode()).hexdigest()[:16]
                    proxy_segment_url = f"{base_stream_url.replace('/playlist.m3u8', '')}/segment/{segment_id}.ts?url={segment_url}"
                    modified_lines.append(proxy_segment_url)
                else:
                    modified_lines.append(line)
            
            return '\n'.join(modified_lines)
            
        except Exception as e:
            logger.error(f"Error proxying M3U8 content: {e}")
            return ""
    
    async def close(self):
        """Close aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()

class StreamManager:
    """Manage M3U8 streams and proxying"""
    
    def __init__(self):
        self.processor = M3U8Processor()
        self.active_streams = weakref.WeakValueDictionary()
        
    async def get_collection(self, name):
        """Get collection with fallback to memory storage"""
        if streams_collection and name == 'streams':
            return streams_collection
        elif users_collection and name == 'users':
            return users_collection
        elif analytics_collection and name == 'analytics':
            return analytics_collection
        else:
            return memory_storage
    
    async def add_stream(self, user_id: int, original_url: str, title: str = None) -> Dict[str, Any]:
        """Add new stream to database"""
        try:
            stream_id = self.processor.generate_stream_id(original_url, user_id)
            
            # Validate M3U8 URL
            if not await self.processor.validate_m3u8_url(original_url):
                return {"success": False, "error": "Invalid M3U8 URL or unreachable stream"}
            
            proxy_url = f"{BASE_URL}/stream/{stream_id}/playlist.m3u8"
            
            stream_data = {
                "stream_id": stream_id,
                "user_id": user_id,
                "original_url": original_url,
                "proxy_url": proxy_url,
                "title": title or f"Stream {stream_id}",
                "created_at": datetime.utcnow(),
                "status": "active",
                "views": 0,
                "last_accessed": datetime.utcnow(),
                "type": "proxy"
            }
            
            collection = await self.get_collection('streams')
            result = await collection.insert_one(stream_data)
            
            if result:
                logger.info(f"Stream {stream_id} added successfully")
                return {
                    "success": True, 
                    "stream_id": stream_id,
                    "proxy_url": proxy_url,
                    "original_url": original_url
                }
            else:
                return {"success": False, "error": "Failed to save stream"}
                
        except Exception as e:
            logger.error(f"Error adding stream: {e}")
            return {"success": False, "error": f"Internal error: {str(e)}"}
    
    async def convert_video(self, user_id: int, video_url: str, title: str = None) -> Dict[str, Any]:
        """Convert video to M3U8 format"""
        try:
            stream_id = self.processor.generate_stream_id(video_url, user_id)
            
            # Convert video
            proxy_url = await self.processor.convert_video_to_m3u8(video_url, stream_id)
            
            if not proxy_url:
                return {"success": False, "error": "Failed to convert video"}
            
            stream_data = {
                "stream_id": stream_id,
                "user_id": user_id,
                "original_url": video_url,
                "proxy_url": proxy_url,
                "title": title or f"Converted {stream_id}",
                "created_at": datetime.utcnow(),
                "status": "active",
                "views": 0,
                "last_accessed": datetime.utcnow(),
                "type": "converted"
            }
            
            collection = await self.get_collection('streams')
            await collection.insert_one(stream_data)
            
            logger.info(f"Video {stream_id} converted successfully")
            return {
                "success": True, 
                "stream_id": stream_id,
                "proxy_url": proxy_url,
                "original_url": video_url
            }
            
        except Exception as e:
            logger.error(f"Error converting video: {e}")
            return {"success": False, "error": f"Conversion failed: {str(e)}"}
    
    async def get_stream(self, stream_id: str) -> Optional[Dict]:
        """Get stream by ID"""
        try:
            collection = await self.get_collection('streams')
            return await collection.find_one({"stream_id": stream_id})
        except Exception as e:
            logger.error(f"Error getting stream {stream_id}: {e}")
            return None
    
    async def get_user_streams(self, user_id: int) -> List[Dict]:
        """Get all streams for a user"""
        try:
            collection = await self.get_collection('streams')
            cursor = collection.find({"user_id": user_id})
            return await cursor.to_list(length=100) if hasattr(cursor, 'to_list') else []
        except Exception as e:
            logger.error(f"Error getting user streams: {e}")
            return []
    
    async def update_stream_stats(self, stream_id: str):
        """Update stream access statistics"""
        try:
            collection = await self.get_collection('streams')
            await collection.update_one(
                {"stream_id": stream_id},
                {
                    "$inc": {"views": 1},
                    "$set": {"last_accessed": datetime.utcnow()}
                }
            )
        except Exception as e:
            logger.error(f"Error updating stream stats: {e}")

class UserManager:
    """Manage user data and permissions"""
    
    def __init__(self):
        pass
    
    async def get_collection(self):
        """Get users collection with fallback"""
        return users_collection if users_collection else memory_storage
    
    async def register_user(self, user_id: int, username: str = None, first_name: str = None):
        """Register new user"""
        try:
            collection = await self.get_collection()
            user_data = {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "registered_at": datetime.utcnow(),
                "streams_created": 0,
                "last_activity": datetime.utcnow(),
                "is_premium": False,
                "is_admin": user_id in [123456789]  # Add admin user IDs
            }
            
            # Check if user exists
            existing = await collection.find_one({"user_id": user_id})
            if not existing:
                await collection.insert_one(user_data)
                logger.info(f"User {user_id} registered")
            else:
                # Update last activity
                await collection.update_one(
                    {"user_id": user_id},
                    {"$set": {"last_activity": datetime.utcnow()}}
                )
                
        except Exception as e:
            logger.error(f"Error registering user {user_id}: {e}")
    
    async def get_user(self, user_id: int) -> Optional[Dict]:
        """Get user data"""
        try:
            collection = await self.get_collection()
            return await collection.find_one({"user_id": user_id})
        except Exception as e:
            logger.error(f"Error getting user {user_id}: {e}")
            return None
    
    async def is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        user = await self.get_user(user_id)
        return user and user.get("is_admin", False)

# Initialize managers
stream_manager = StreamManager()
user_manager = UserManager()

# Web server for serving M3U8 streams
async def create_web_app():
    """Create web application for serving streams"""
    app = web.Application()
    
    async def serve_playlist(request):
        """Serve M3U8 playlist"""
        try:
            stream_id = request.match_info['stream_id']
            stream = await stream_manager.get_stream(stream_id)
            
            if not stream:
                return web.Response(status=404, text="Stream not found")
            
            # Update stats
            await stream_manager.update_stream_stats(stream_id)
            
            if stream.get('type') == 'converted':
                # Serve local converted file
                playlist_path = stream_manager.processor.temp_dir / stream_id / "playlist.m3u8"
                if playlist_path.exists():
                    async with aiofiles.open(playlist_path, 'r') as f:
                        content = await f.read()
                    
                    # Modify content to use server URLs
                    lines = content.split('\n')
                    modified_lines = []
                    for line in lines:
                        if line.strip() and not line.startswith('#') and line.endswith('.ts'):
                            modified_lines.append(f"{BASE_URL}/stream/{stream_id}/segment/{line}")
                        else:
                            modified_lines.append(line)
                    
                    content = '\n'.join(modified_lines)
                    return web.Response(
                        text=content,
                        content_type='application/vnd.apple.mpegurl',
                        headers={'Access-Control-Allow-Origin': '*'}
                    )
                else:
                    return web.Response(status=404, text="Playlist not found")
            else:
                # Proxy remote M3U8
                proxy_url = f"{BASE_URL}/stream/{stream_id}/playlist.m3u8"
                content = await stream_manager.processor.proxy_m3u8_content(
                    stream['original_url'], proxy_url
                )
                
                return web.Response(
                    text=content,
                    content_type='application/vnd.apple.mpegurl',
                    headers={'Access-Control-Allow-Origin': '*'}
                )
                
        except Exception as e:
            logger.error(f"Error serving playlist: {e}")
            return web.Response(status=500, text="Internal server error")
    
    async def serve_segment(request):
        """Serve video segments"""
        try:
            stream_id = request.match_info['stream_id']
            segment_file = request.match_info.get('segment_file')
            segment_id = request.match_info.get('segment_id')
            
            stream = await stream_manager.get_stream(stream_id)
            if not stream:
                return web.Response(status=404, text="Stream not found")
            
            if stream.get('type') == 'converted' and segment_file:
                # Serve local segment
                segment_path = stream_manager.processor.temp_dir / stream_id / segment_file
                if segment_path.exists():
                    return web.FileResponse(
                        segment_path,
                        headers={'Access-Control-Allow-Origin': '*'}
                    )
            elif segment_id:
                # Proxy remote segment
                segment_url = request.query.get('url')
                if segment_url:
                    session = await stream_manager.processor.get_session()
                    async with session.get(segment_url) as response:
                        if response.status == 200:
                            data = await response.read()
                            return web.Response(
                                body=data,
                                content_type='video/mp2t',
                                headers={'Access-Control-Allow-Origin': '*'}
                            )
            
            return web.Response(status=404, text="Segment not found")
            
        except Exception as e:
            logger.error(f"Error serving segment: {e}")
            return web.Response(status=500, text="Internal server error")
    
    async def health_check(request):
        """Health check endpoint"""
        return web.json_response({"status": "healthy", "base_url": BASE_URL})
    
    # Routes
    app.router.add_get('/stream/{stream_id}/playlist.m3u8', serve_playlist)
    app.router.add_get('/stream/{stream_id}/segment/{segment_file}', serve_segment)
    app.router.add_get('/stream/{stream_id}/segment/{segment_id}.ts', serve_segment)
    app.router.add_get('/health', health_check)
    app.router.add_get('/', health_check)
    
    return app

# Bot command handlers
@app.on_message(filters.command("start"))
async def start_command(client, message: Message):
    """Handle /start command"""
    try:
        user = message.from_user
        await user_manager.register_user(user.id, user.username, user.first_name)
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📺 Add M3U8 Stream", callback_data="add_stream")],
            [InlineKeyboardButton("🎬 Convert Video", callback_data="convert_video")],
            [InlineKeyboardButton("📋 My Streams", callback_data="my_streams")],
            [InlineKeyboardButton("📊 Statistics", callback_data="stats")],
            [InlineKeyboardButton("❓ Help", callback_data="help")]
        ])
        
        welcome_text = f"""
🎬 **Welcome to M3U8 Bot!** 🎬

Hello {user.first_name}! I'm your M3U8 streaming assistant.

**What I can do:**
✅ Host M3U8 live streams on your server
✅ Convert videos to M3U8 format  
✅ Proxy streams with custom URLs
✅ Track stream analytics
✅ Manage multiple streams

**Server Status:** 🟢 Online
**Base URL:** `{BASE_URL}`

Choose an option below to get started!
        """
        
        await message.reply_text(welcome_text, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Error in start command: {e}")
        await message.reply_text("❌ An error occurred. Please try again.")

@app.on_callback_query(filters.regex("add_stream"))
async def add_stream_callback(client, callback_query: CallbackQuery):
    """Handle add stream callback"""
    try:
        await callback_query.answer()
        await callback_query.message.reply_text(
            "📺 **Add M3U8 Stream**\n\n"
            "Send me an M3U8 URL to host on your server.\n"
            "Format: `https://example.com/stream.m3u8`\n\n"
            "Or use the format:\n"
            "`/add <url> [title]`"
        )
    except Exception as e:
        logger.error(f"Error in add stream callback: {e}")

@app.on_callback_query(filters.regex("convert_video"))
async def convert_video_callback(client, callback_query: CallbackQuery):
    """Handle convert video callback"""
    try:
        await callback_query.answer()
        await callback_query.message.reply_text(
            "🎬 **Convert Video to M3U8**\n\n"
            "Send me a video URL to convert to M3U8 format.\n"
            "Supported formats: MP4, AVI, MKV, MOV, etc.\n\n"
            "Format: `/convert <url> [title]`\n\n"
            "⚠️ Note: Large videos may take time to process."
        )
    except Exception as e:
        logger.error(f"Error in convert video callback: {e}")

@app.on_callback_query(filters.regex("my_streams"))
async def my_streams_callback(client, callback_query: CallbackQuery):
    """Handle my streams callback"""
    try:
        await callback_query.answer()
        user_id = callback_query.from_user.id
        streams = await stream_manager.get_user_streams(user_id)
        
        if not streams:
            await callback_query.message.reply_text("📋 You don't have any streams yet.")
            return
        
        text = "📋 **Your Streams:**\n\n"
        for i, stream in enumerate(streams[:10], 1):
            status_emoji = "🟢" if stream['status'] == 'active' else "🔴"
            type_emoji = "📺" if stream['type'] == 'proxy' else "🎬"
            text += f"{i}. {type_emoji} **{stream['title']}**\n"
            text += f"   {status_emoji} Views: {stream['views']} | ID: `{stream['stream_id']}`\n"
            text += f"   🔗 `{stream['proxy_url']}`\n\n"
        
        if len(streams) > 10:
            text += f"... and {len(streams) - 10} more streams"
        
        await callback_query.message.reply_text(text)
        
    except Exception as e:
        logger.error(f"Error in my streams callback: {e}")

@app.on_callback_query(filters.regex("stats"))
async def stats_callback(client, callback_query: CallbackQuery):
    """Handle stats callback"""
    try:
        await callback_query.answer()
        user_id = callback_query.from_user.id
        streams = await stream_manager.get_user_streams(user_id)
        
        total_streams = len(streams)
        total_views = sum(s.get('views', 0) for s in streams)
        active_streams = len([s for s in streams if s['status'] == 'active'])
        
        text = f"""
📊 **Your Statistics**

🎬 **Total Streams:** {total_streams}
🟢 **Active Streams:** {active_streams}  
👁️ **Total Views:** {total_views}
🌍 **Server:** {BASE_URL}

📈 **Stream Types:**
📺 Proxy: {len([s for s in streams if s.get('type') == 'proxy'])}
🎬 Converted: {len([s for s in streams if s.get('type') == 'converted'])}
        """
        
        await callback_query.message.reply_text(text)
        
    except Exception as e:
        logger.error(f"Error in stats callback: {e}")

@app.on_callback_query(filters.regex("help"))
async def help_callback(client, callback_query: CallbackQuery):
    """Handle help callback"""
    try:
        await callback_query.answer()
        
        help_text = """
❓ **Help & Commands**

**Basic Commands:**
• `/start` - Start the bot
• `/add <url> [title]` - Add M3U8 stream
• `/convert <url> [title]` - Convert video to M3U8
• `/list` - Show your streams
• `/stats` - View statistics

**Stream Management:**
• Send M3U8 URL directly to add stream
• Streams are automatically hosted on your server
• Custom titles are optional
• All streams get unique IDs

**Supported Formats:**
📺 **M3U8 Streams:** Live TV, IPTV, HLS streams
🎬 **Video Files:** MP4, AVI, MKV, MOV, WebM

**Features:**
✅ Auto URL detection
✅ Stream proxying  
✅ Video conversion
✅ Analytics tracking
✅ Error handling
✅ 24/7 hosting

**Need Support?**
Contact: @YourSupportBot
        """
        
        await callback_query.message.reply_text(help_text)
        
    except Exception as e:
        logger.error(f"Error in help callback: {e}")

@app.on_message(filters.command("add"))
async def add_stream_command(client, message: Message):
    """Handle /add command"""
    try:
        user = message.from_user
        await user_manager.register_user(user.id, user.username, user.first_name)
        
        args = message.text.split()[1:]
        if not args:
            await message.reply_text(
                "❌ Please provide an M3U8 URL.\n"
                "Usage: `/add <url> [title]`"
            )
            return
        
        url = args[0]
        title = " ".join(args[1:]) if len(args) > 1 else None
        
        # Validate URL format
        if not url.startswith(('http://', 'https://')):
            await message.reply_text("❌ Please provide a valid HTTP/HTTPS URL.")
            return
        
        progress_msg = await message.reply_text("🔄 Adding stream... Please wait.")
        
        result = await stream_manager.add_stream(user.id, url, title)
        
        if result["success"]:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📺 Open Stream", url=result["proxy_url"])],
                [InlineKeyboardButton("📋 My Streams", callback_data="my_streams")]
            ])
            
            success_text = f"""
✅ **Stream Added Successfully!**

**Title:** {title or f"Stream {result['stream_id']}"}
**Stream ID:** `{result['stream_id']}`
**Your URL:** `{result['proxy_url']}`
**Original:** `{result['original_url']}`

🎬 Your stream is now hosted and ready to use!
            """
            
            await progress_msg.edit_text(success_text, reply_markup=keyboard)
        else:
            await progress_msg.edit_text(f"❌ Failed to add stream: {result['error']}")
            
    except Exception as e:
        logger.error(f"Error in add command: {e}")
        await message.reply_text("❌ An error occurred while adding the stream.")

@app.on_message(filters.command("convert"))
async def convert_video_command(client, message: Message):
    """Handle /convert command"""
    try:
        user = message.from_user
        await user_manager.register_user(user.id, user.username, user.first_name)
        
        args = message.text.split()[1:]
        if not args:
            await message.reply_text(
                "❌ Please provide a video URL.\n"
                "Usage: `/convert <url> [title]`"
            )
            return
        
        url = args[0]
        title = " ".join(args[1:]) if len(args) > 1 else None
        
        # Validate URL format
        if not url.startswith(('http://', 'https://')):
            await message.reply_text("❌ Please provide a valid HTTP/HTTPS URL.")
            return
        
        progress_msg = await message.reply_text("🎬 Converting video... This may take a while.")
        
        result = await stream_manager.convert_video(user.id, url, title)
        
        if result["success"]:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🎬 Open Stream", url=result["proxy_url"])],
                [InlineKeyboardButton("📋 My Streams", callback_data="my_streams")]
            ])
            
            success_text = f"""
✅ **Video Converted Successfully!**

**Title:** {title or f"Converted {result['stream_id']}"}
**Stream ID:** `{result['stream_id']}`
**M3U8 URL:** `{result['proxy_url']}`
**Original:** `{result['original_url']}`

🎬 Your video is now available as M3U8 stream!
            """
            
            await progress_msg.edit_text(success_text, reply_markup=keyboard)
        else:
            await progress_msg.edit_text(f"❌ Failed to convert video: {result['error']}")
            
    except Exception as e:
        logger.error(f"Error in convert command: {e}")
        await message.reply_text("❌ An error occurred while converting the video.")

@app.on_message(filters.command("list"))
async def list_streams_command(client, message: Message):
    """Handle /list command"""
    try:
        user_id = message.from_user.id
        streams = await stream_manager.get_user_streams(user_id)
        
        if not streams:
            await message.reply_text("📋 You don't have any streams yet.")
            return
        
        text = "📋 **Your Streams:**\n\n"
        for i, stream in enumerate(streams[:15], 1):
            status_emoji = "🟢" if stream['status'] == 'active' else "🔴"
            type_emoji = "📺" if stream['type'] == 'proxy' else "🎬"
            text += f"{i}. {type_emoji} **{stream['title']}**\n"
            text += f"   {status_emoji} Views: {stream['views']} | ID: `{stream['stream_id']}`\n"
            text += f"   🔗 `{stream['proxy_url']}`\n\n"
        
        if len(streams) > 15:
            text += f"... and {len(streams) - 15} more streams\n"
            text += "Use inline buttons for complete list."
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Statistics", callback_data="stats")],
            [InlineKeyboardButton("➕ Add Stream", callback_data="add_stream")]
        ])
        
        await message.reply_text(text, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Error in list command: {e}")
        await message.reply_text("❌ An error occurred while fetching streams.")

@app.on_message(filters.command("stats"))
async def stats_command(client, message: Message):
    """Handle /stats command"""
    try:
        user_id = message.from_user.id
        streams = await stream_manager.get_user_streams(user_id)
        user = await user_manager.get_user(user_id)
        
        total_streams = len(streams)
        total_views = sum(s.get('views', 0) for s in streams)
        active_streams = len([s for s in streams if s['status'] == 'active'])
        proxy_streams = len([s for s in streams if s.get('type') == 'proxy'])
        converted_streams = len([s for s in streams if s.get('type') == 'converted'])
        
        # Recent activity
        recent_streams = [s for s in streams if s.get('last_accessed', datetime.min) > datetime.utcnow() - timedelta(days=7)]
        
        text = f"""
📊 **Detailed Statistics**

👤 **User Info:**
• ID: `{user_id}`
• Registered: {user.get('registered_at', 'Unknown').strftime('%Y-%m-%d') if user and user.get('registered_at') else 'Unknown'}
• Status: {'🔑 Admin' if await user_manager.is_admin(user_id) else '👤 User'}

🎬 **Stream Statistics:**
• Total Streams: **{total_streams}**
• Active Streams: **{active_streams}**
• Total Views: **{total_views}**
• Recent Activity (7 days): **{len(recent_streams)}**

📺 **Stream Types:**
• Proxy Streams: **{proxy_streams}**
• Converted Videos: **{converted_streams}**

🌍 **Server Info:**
• Base URL: `{BASE_URL}`
• Status: 🟢 Online
• Storage: {'🗄️ MongoDB' if mongo_client else '💾 Memory'}

📈 **Performance:**
• Average Views per Stream: **{total_views / total_streams if total_streams > 0 else 0:.1f}**
• Most Viewed: **{max([s.get('views', 0) for s in streams], default=0)}**
        """
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 My Streams", callback_data="my_streams")],
            [InlineKeyboardButton("➕ Add Stream", callback_data="add_stream")]
        ])
        
        await message.reply_text(text, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Error in stats command: {e}")
        await message.reply_text("❌ An error occurred while fetching statistics.")

@app.on_message(filters.command("admin") & filters.user([123456789]))  # Replace with actual admin IDs
async def admin_command(client, message: Message):
    """Handle admin commands"""
    try:
        args = message.text.split()[1:]
        if not args:
            admin_text = """
🔑 **Admin Panel**

Available commands:
• `/admin stats` - Server statistics
• `/admin users` - User statistics  
• `/admin streams` - All streams
• `/admin cleanup` - Clean old streams
• `/admin broadcast <message>` - Broadcast message

Server Status: 🟢 Online
            """
            await message.reply_text(admin_text)
            return
        
        command = args[0].lower()
        
        if command == "stats":
            # Get global statistics
            all_streams = await stream_manager.get_user_streams(0) if streams_collection else []
            total_users = len(await users_collection.find({}).to_list(length=None)) if users_collection else 1
            
            admin_stats = f"""
🔑 **Server Statistics**

📊 **Overview:**
• Total Users: **{total_users}**
• Total Streams: **{len(all_streams)}**
• Database: {'🗄️ MongoDB Connected' if mongo_client else '💾 Memory Storage'}
• Server: `{BASE_URL}`

🎬 **Stream Stats:**
• Active: **{len([s for s in all_streams if s.get('status') == 'active'])}**
• Proxy: **{len([s for s in all_streams if s.get('type') == 'proxy'])}**
• Converted: **{len([s for s in all_streams if s.get('type') == 'converted'])}**
• Total Views: **{sum(s.get('views', 0) for s in all_streams)}**
            """
            await message.reply_text(admin_stats)
            
        elif command == "cleanup":
            # Clean old inactive streams
            await message.reply_text("🧹 Cleanup functionality would be implemented here")
            
    except Exception as e:
        logger.error(f"Error in admin command: {e}")
        await message.reply_text("❌ Admin command failed.")

@app.on_message(filters.regex(r'https?://.*\.m3u8.*') & filters.private)
async def handle_m3u8_url(client, message: Message):
    """Handle M3U8 URLs sent directly"""
    try:
        user = message.from_user
        await user_manager.register_user(user.id, user.username, user.first_name)
        
        url = message.text.strip()
        
        progress_msg = await message.reply_text("🔄 Processing M3U8 URL... Please wait.")
        
        result = await stream_manager.add_stream(user.id, url)
        
        if result["success"]:
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("📺 Open Stream", url=result["proxy_url"])],
                [InlineKeyboardButton("📋 My Streams", callback_data="my_streams")],
                [InlineKeyboardButton("📊 Stats", callback_data="stats")]
            ])
            
            success_text = f"""
✅ **M3U8 Stream Added!**

**Stream ID:** `{result['stream_id']}`
**Your URL:** `{result['proxy_url']}`

🎬 Stream is now hosted and ready to use!
Copy the URL above to use in any media player.
            """
            
            await progress_msg.edit_text(success_text, reply_markup=keyboard)
        else:
            await progress_msg.edit_text(f"❌ Failed to add stream: {result['error']}")
            
    except Exception as e:
        logger.error(f"Error handling M3U8 URL: {e}")
        await message.reply_text("❌ An error occurred while processing the URL.")

@app.on_message(filters.regex(r'https?://.*\.(mp4|avi|mkv|mov|wmv|flv|webm|m4v).*') & filters.private)
async def handle_video_url(client, message: Message):
    """Handle video URLs sent directly"""
    try:
        user = message.from_user
        await user_manager.register_user(user.id, user.username, user.first_name)
        
        url = message.text.strip()
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🎬 Convert to M3U8", callback_data=f"convert_{hashlib.md5(url.encode()).hexdigest()[:16]}")],
            [InlineKeyboardButton("❌ Cancel", callback_data="cancel")]
        ])
        
        await message.reply_text(
            f"🎬 **Video URL Detected!**\n\n"
            f"URL: `{url}`\n\n"
            f"Would you like to convert this video to M3U8 format?\n"
            f"⚠️ Note: Large videos may take time to process.",
            reply_markup=keyboard
        )
        
        # Store URL temporarily (in a real app, use Redis or similar)
        # For now, we'll handle it in the callback
        
    except Exception as e:
        logger.error(f"Error handling video URL: {e}")
        await message.reply_text("❌ An error occurred while processing the video URL.")

# Error handlers
@app.on_message(filters.command("help"))
async def help_command(client, message: Message):
    """Handle /help command"""
    try:
        help_text = """
❓ **M3U8 Bot Help**

**Quick Start:**
• Send any M3U8 URL directly to add it
• Send video URLs to convert them
• Use commands for more control

**Commands:**
• `/start` - Start the bot
• `/add <url> [title]` - Add M3U8 stream  
• `/convert <url> [title]` - Convert video
• `/list` - Show your streams
• `/stats` - View statistics
• `/help` - Show this help

**Supported Formats:**
📺 **M3U8 Streams:** .m3u8, HLS, IPTV
🎬 **Videos:** .mp4, .avi, .mkv, .mov, .webm

**Features:**
✅ Auto-hosting on your server
✅ Custom stream URLs  
✅ Real-time analytics
✅ Multiple format support
✅ Error handling & recovery
✅ 24/7 availability

**Server:** `{BASE_URL}`
**Status:** 🟢 Online

Need help? The bot handles most tasks automatically!
        """
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📺 Add Stream", callback_data="add_stream")],
            [InlineKeyboardButton("🎬 Convert Video", callback_data="convert_video")],
            [InlineKeyboardButton("📋 My Streams", callback_data="my_streams")]
        ])
        
        await message.reply_text(help_text, reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Error in help command: {e}")

# Global error handler
async def error_handler(client, message, error):
    """Global error handler"""
    logger.error(f"Error in {message.chat.id}: {error}")
    try:
        await message.reply_text("❌ An unexpected error occurred. Please try again.")
    except:
        pass

# Startup and shutdown handlers
async def startup():
    """Initialize bot and web server"""
    global mongo_client, db, streams_collection, users_collection, analytics_collection, app
    
    try:
        logger.info("Starting M3U8 Bot initialization...")
        
        # Initialize bot client
        app = await initialize_bot()
        logger.info("Bot client initialized")
        
        # Setup MongoDB
        mongo_client = await setup_mongodb()
        if mongo_client:
            db = mongo_client[DATABASE_NAME]
            streams_collection = db.streams
            users_collection = db.users  
            analytics_collection = db.analytics
            logger.info("Database collections initialized")
        else:
            logger.warning("Using fallback memory storage")
        
        # Create session directory
        Path("./session").mkdir(exist_ok=True)
        
        # Start web server
        web_app = await create_web_app()
        runner = web.AppRunner(web_app)
        await runner.setup()
        
        site = web.TCPSite(runner, '0.0.0.0', SERVER_PORT)
        await site.start()
        
        logger.info(f"Web server started on port {SERVER_PORT}")
        logger.info(f"Base URL: {BASE_URL}")
        
        # Start bot with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await app.start()
                logger.info("Bot started successfully")
                
                # Get bot info
                me = await app.get_me()
                logger.info(f"Bot info: @{me.username} ({me.first_name})")
                break
                
            except Exception as e:
                logger.error(f"Bot start attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2)
        
        # Send startup notification to admin
        try:
            admin_ids = [123456789, 5137262495]  # Add your admin IDs here
            for admin_id in admin_ids:
                try:
                    await app.send_message(
                        admin_id, 
                        f"🚀 **M3U8 Bot Started!**\n\n"
                        f"**Server:** `{BASE_URL}`\n"
                        f"**Status:** 🟢 Online\n"
                        f"**Database:** {'🗄️ MongoDB' if mongo_client else '💾 Memory'}\n"
                        f"**Bot:** @{me.username}"
                    )
                    logger.info(f"Startup notification sent to {admin_id}")
                    break
                except:
                    continue
        except Exception as e:
            logger.warning(f"Could not send startup notification: {e}")
            
    except Exception as e:
        logger.error(f"Startup error: {e}")
        raise

async def shutdown():
    """Cleanup on shutdown"""
    try:
        logger.info("Starting shutdown process...")
        
        await stream_manager.processor.close()
        
        if mongo_client:
            mongo_client.close()
            logger.info("MongoDB connection closed")
            
        if app:
            await app.stop()
            logger.info("Bot stopped")
            
        logger.info("Bot shutdown completed")
    except Exception as e:
        logger.error(f"Shutdown error: {e}")

# Main execution
async def main():
    """Main function"""
    try:
        logger.info("M3U8 Bot starting...")
        await startup()
        
        # Keep the bot running
        logger.info("Bot is running... Press Ctrl+C to stop")
        await asyncio.Future()  # Run forever
        
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Main error: {e}")
        # Don't exit on startup errors in production
        if not os.getenv("PRODUCTION"):
            raise
    finally:
        await shutdown()

if __name__ == "__main__":
    # Production deployment with better error handling
    if os.getenv("PRODUCTION"):
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            logger.info("Using uvloop for better performance")
        except ImportError:
            logger.warning("uvloop not available, using default event loop")
    
    # Create session directory
    Path("./session").mkdir(exist_ok=True)
    
    # Run with proper error handling
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        if not os.getenv("PRODUCTION"):
            exit(1)
        else:
            # In production, log error but don't exit
            logger.error("Production mode: continuing despite error")
