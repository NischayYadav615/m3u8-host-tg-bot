#!/usr/bin/env python3
"""
M3U8 Bot - Clean Version
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
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
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

# Validate bot token format
if not BOT_TOKEN or not re.match(r'^\d+:[A-Za-z0-9_-]{35}$', BOT_TOKEN):
    logger.error("Invalid bot token format")
    exit(1)

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

# Initialize bot client
try:
    app = Client(
        "m3u8_bot", 
        api_id=API_ID, 
        api_hash=API_HASH, 
        bot_token=BOT_TOKEN,
        workdir="./session"
    )
    logger.info("Bot client created")
except Exception as e:
    logger.error(f"Failed to create bot client: {e}")
    app = None

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
            if hasattr(collection, 'find'):
                cursor = collection.find({"user_id": user_id})
                if hasattr(cursor, 'to_list'):
                    return await cursor.to_list(length=100)
                else:
                    return [doc async for doc in cursor]
            else:
                # Memory storage fallback
                return [s for s in collection.streams.values() if s.get('user_id') == user_id]
        except Exception as e:
            logger.error(f"Error getting user streams: {e}")
            return []

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
    app_web = web.Application()
    
    async def serve_playlist(request):
        """Serve M3U8 playlist"""
        try:
            stream_id = request.match_info['stream_id']
            stream = await stream_manager.get_stream(stream_id)
            
            if not stream:
                return web.Response(status=404, text="Stream not found")
            
            # Proxy remote M3U8
            content = await stream_manager.processor.fetch_m3u8_content(stream['original_url'])
            
            if content:
                return web.Response(
                    text=content,
                    content_type='application/vnd.apple.mpegurl',
                    headers={'Access-Control-Allow-Origin': '*'}
                )
            else:
                return web.Response(status=404, text="Stream unavailable")
                
        except Exception as e:
            logger.error(f"Error serving playlist: {e}")
            return web.Response(status=500, text="Internal server error")
    
    async def health_check(request):
        """Health check endpoint"""
        return web.json_response({"status": "healthy", "base_url": BASE_URL})
    
    # Routes
    app_web.router.add_get('/stream/{stream_id}/playlist.m3u8', serve_playlist)
    app_web.router.add_get('/health', health_check)
    app_web.router.add_get('/', health_check)
    
    return app_web

# Bot token validation
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

# Handler functions
async def handle_start_command(client, message: Message):
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

async def handle_add_command(client, message: Message):
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
                [InlineKeyboardButton("📋 My Streams", callback_data="my_streams")]
            ])
            
            success_text = f"""
✅ **M3U8 Stream Added!**

**Stream ID:** `{result['stream_id']}`
**Your URL:** `{result['proxy_url']}`

🎬 Stream is now hosted and ready to use!
            """
            
            await progress_msg.edit_text(success_text, reply_markup=keyboard)
        else:
            await progress_msg.edit_text(f"❌ Failed to add stream: {result['error']}")
            
    except Exception as e:
        logger.error(f"Error handling M3U8 URL: {e}")
        await message.reply_text("❌ An error occurred while processing the URL.")

async def handle_add_stream_callback(client, callback_query: CallbackQuery):
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

async def handle_my_streams_callback(client, callback_query: CallbackQuery):
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

async def handle_help_callback(client, callback_query: CallbackQuery):
    """Handle help callback"""
    try:
        await callback_query.answer()
        
        help_text = """
❓ **Help & Commands**

**Basic Commands:**
• `/start` - Start the bot
• `/add <url> [title]` - Add M3U8 stream
• `/list` - Show your streams
• `/help` - Show help

**Stream Management:**
• Send M3U8 URL directly to add stream
• Streams are automatically hosted on your server
• Custom titles are optional

**Features:**
✅ Auto URL detection
✅ Stream proxying  
✅ Analytics tracking
✅ Error handling
✅ 24/7 hosting

**Server:** `{BASE_URL}`
        """
        
        await callback_query.message.reply_text(help_text)
        
    except Exception as e:
        logger.error(f"Error in help callback: {e}")

# Register handlers function
async def register_handlers():
    """Register all bot handlers after app is initialized"""
    if not app:
        logger.error("Cannot register handlers: app is None")
        return
    
    try:
        # Command handlers
        app.add_handler(MessageHandler(handle_start_command, filters.command("start")))
        app.add_handler(MessageHandler(handle_add_command, filters.command("add")))
        
        # URL handlers
        app.add_handler(MessageHandler(handle_m3u8_url, filters.regex(r'https?://.*\.m3u8.*') & filters.private))
        
        # Callback handlers
        app.add_handler(CallbackQueryHandler(handle_add_stream_callback, filters.regex("add_stream")))
        app.add_handler(CallbackQueryHandler(handle_my_streams_callback, filters.regex("my_streams")))
        app.add_handler(CallbackQueryHandler(handle_help_callback, filters.regex("help")))
        
        logger.info("Bot handlers registered successfully")
        
    except Exception as e:
        logger.error(f"Error registering handlers: {e}")

# Startup function
async def startup():
    """Initialize bot and web server"""
    global mongo_client, db, streams_collection, users_collection, analytics_collection
    
    try:
        logger.info("Starting M3U8 Bot initialization...")
        
        # Validate bot token first
        if not await validate_bot_token(BOT_TOKEN):
            logger.error("Bot token validation failed - token may be invalid or expired")
            logger.error("Please get a new token from @BotFather and update BOT_TOKEN environment variable")
            raise ValueError("Invalid bot token")
        
        # Ensure session directory exists
        Path("./session").mkdir(exist_ok=True)
        
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
        
        # Start web server
        web_app = await create_web_app()
        runner = web.AppRunner(web_app)
        await runner.setup()
        
        site = web.TCPSite(runner, '0.0.0.0', SERVER_PORT)
        await site.start()
        
        logger.info(f"Web server started on port {SERVER_PORT}")
        logger.info(f"Base URL: {BASE_URL}")
        
        # Start bot with retry logic and register handlers
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await app.start()
                logger.info("Bot started successfully")
                
                # Get bot info
                me = await app.get_me()
                logger.info(f"Bot info: @{me.username} ({me.first_name})")
                
                # Register handlers after successful start
                await register_handlers()
                
                break
                
            except Exception as e:
                logger.error(f"Bot start attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2)
        
        logger.info("Bot initialization completed successfully!")
            
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
