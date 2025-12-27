import os
import sys
import time
import math
import logging
import boto3
import asyncio
import threading
from dotenv import load_dotenv
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiohttp import web

# Fix encoding
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# Config
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")

WASABI_ACCESS_KEY = os.getenv("WASABI_ACCESS_KEY")
WASABI_SECRET_KEY = os.getenv("WASABI_SECRET_KEY")
WASABI_BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME")
WASABI_REGION = os.getenv("WASABI_REGION", "us-east-1")
PRESIGNED_URL_EXPIRY = int(os.getenv("PRESIGNED_URL_EXPIRY", "604800"))

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PORT = int(os.getenv("PORT", "8080"))

DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot_running = False

# Wasabi client
s3 = boto3.client(
    "s3",
    endpoint_url=f"https://s3.{WASABI_REGION}.wasabisys.com",
    aws_access_key_id=WASABI_ACCESS_KEY,
    aws_secret_access_key=WASABI_SECRET_KEY,
    region_name=WASABI_REGION
)

# Bot
app = Client(
    "wasabi_uploader_bot",
    bot_token=BOT_TOKEN,
    api_id=API_ID,
    api_hash=API_HASH
)

# Helpers
def human_bytes(size):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024

def generate_presigned_url(file_key, expiration=PRESIGNED_URL_EXPIRY):
    try:
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': WASABI_BUCKET_NAME, 'Key': file_key},
            ExpiresIn=expiration
        )
        return presigned_url
    except Exception as e:
        logger.error(f"Error generating presigned URL: {str(e)}")
        return None

def make_bar(current, total, length=20):
    percent = current / total
    filled = int(length * percent)
    bar = "█" * filled + "░" * (length - filled)
    return bar, percent * 100

async def progress(current, total, msg, start, status, filename):
    now = time.time()
    diff = now - start

    if int(diff) % 5 != 0:
        return

    speed = current / diff if diff > 0 else 0
    eta = (total - current) / speed if speed > 0 else 0

    bar, percent = make_bar(current, total)

    text = (
        f"🚀 {status}...\n\n"
        f"📁 File Name: `{filename}`\n"
        f"👀 File Size: {human_bytes(total)}\n"
        f"⚡ Speed: {human_bytes(speed)}/s\n"
        f"⏳ ETA: {math.ceil(eta)} sec\n\n"
        f"`{bar}` {percent:.2f}%"
    )

    try:
        await msg.edit(text)
    except:
        pass

# Upload progress
class UploadProgress:
    def __init__(self, msg, filename, filesize):
        self.msg = msg
        self.filename = filename
        self.filesize = filesize
        self.start = time.time()
        self.uploaded = 0
        self.last = 0

    def __call__(self, bytes_amount):
        self.uploaded += bytes_amount
        now = time.time()

        if now - self.last < 5:
            return

        self.last = now

        speed = self.uploaded / (now - self.start)
        eta = (self.filesize - self.uploaded) / speed if speed > 0 else 0
        bar, percent = make_bar(self.uploaded, self.filesize)

        text = (
            f"🚀 Uploading...\n\n"
            f"📁 File Name: `{self.filename}`\n"
            f"👀 File Size: {human_bytes(self.filesize)}\n"
            f"⚡ Speed: {human_bytes(speed)}/s\n"
            f"⏳ ETA: {math.ceil(eta)} sec\n\n"
            f"`{bar}` {percent:.2f}%"
        )

        try:
            asyncio.get_event_loop().create_task(
                self.msg.edit(text)
            )
        except:
            pass

# Commands
@app.on_message(filters.command("start"))
async def start(_, message):
    await message.reply(
        "📤 Send me a video or file\n"
        "I will upload it to Wasabi cloud and give you play/download buttons 🎬⬇️\n\n"
        "Commands:\n"
        "/myfiles - View all your uploaded files"
    )

@app.on_message(filters.command("myfiles"))
async def myfiles(_, message):
    user_id = str(message.from_user.id)
    
    try:
        response = s3.list_objects_v2(
            Bucket=WASABI_BUCKET_NAME,
            Prefix=f"{user_id}/"
        )
        
        if 'Contents' not in response or len(response['Contents']) == 0:
            await message.reply("📁 You haven't uploaded any files yet!")
            return
        
        files_text = "📁 **Your Uploaded Files:**\n\n"
        
        for obj in response['Contents']:
            file_key = obj['Key']
            file_name = file_key.replace(f"{user_id}/", "")
            file_size = obj['Size']
            presigned_url = generate_presigned_url(file_key)
            
            size_str = human_bytes(file_size)
            
            files_text += f"📄 `{file_name}`\n"
            files_text += f"   Size: {size_str}\n"
            if presigned_url:
                files_text += f"   [Download]({presigned_url})\n"
            files_text += f"   `/delete_file {file_name}`\n\n"
        
        await message.reply(files_text, disable_web_page_preview=True)
        
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")

@app.on_message(filters.command("delete_file"))
async def delete_file(_, message):
    user_id = str(message.from_user.id)
    is_admin = user_id == str(ADMIN_ID)
    
    try:
        if is_admin and len(message.command) == 3:
            target_user_id = message.command[1]
            file_name = message.command[2]
        else:
            target_user_id = user_id
            file_name = message.command[1]
    except IndexError:
        if is_admin:
            await message.reply("❌ Usage:\nUser: `/delete_file filename.mp4`\nAdmin: `/delete_file user_id filename.mp4`")
        else:
            await message.reply("❌ Usage: `/delete_file filename.mp4`")
        return
    
    file_key = f"{target_user_id}/{file_name}"
    
    try:
        if target_user_id != user_id and not is_admin:
            await message.reply("❌ You can only delete your own files!")
            return
        
        response = s3.list_objects_v2(
            Bucket=WASABI_BUCKET_NAME,
            Prefix=file_key
        )
        
        if 'Contents' not in response or len(response['Contents']) == 0:
            await message.reply(f"❌ File '{file_name}' not found!")
            return
        
        s3.delete_object(Bucket=WASABI_BUCKET_NAME, Key=file_key)
        
        if is_admin and target_user_id != user_id:
            await message.reply(f"✅ File '{file_name}' from user {target_user_id} deleted successfully!")
        else:
            await message.reply(f"✅ File '{file_name}' deleted successfully!")
        
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")

@app.on_message(filters.command("all_files"))
async def all_files(_, message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("❌ You don't have permission to use this command!")
        return
    
    try:
        response = s3.list_objects_v2(Bucket=WASABI_BUCKET_NAME)
        
        if 'Contents' not in response or len(response['Contents']) == 0:
            await message.reply("📁 No files in storage!")
            return
        
        total_size = 0
        user_stats = {}
        
        for obj in response['Contents']:
            file_key = obj['Key']
            file_size = obj['Size']
            total_size += file_size
            
            user_id = file_key.split('/')[0]
            
            if user_id not in user_stats:
                user_stats[user_id] = {'count': 0, 'size': 0}
            
            user_stats[user_id]['count'] += 1
            user_stats[user_id]['size'] += file_size
        
        files_text = "📊 **All Files Statistics:**\n\n"
        files_text += f"💾 **Total Storage Used:** {human_bytes(total_size)}\n"
        files_text += f"👥 **Total Users:** {len(user_stats)}\n"
        files_text += f"📁 **Total Files:** {len(response['Contents'])}\n\n"
        
        files_text += "**Per User Breakdown:**\n\n"
        
        sorted_users = sorted(user_stats.items(), key=lambda x: x[1]['size'], reverse=True)
        
        for user_id, stats in sorted_users:
            files_text += f"👤 User ID: `{user_id}`\n"
            files_text += f"   Files: {stats['count']}\n"
            files_text += f"   Storage: {human_bytes(stats['size'])}\n\n"
        
        await message.reply(files_text)
        
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")

@app.on_message(filters.command("total_files"))
async def total_files(_, message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("❌ You don't have permission to use this command!")
        return
    
    try:
        response = s3.list_objects_v2(Bucket=WASABI_BUCKET_NAME)
        
        if 'Contents' not in response or len(response['Contents']) == 0:
            await message.reply("📁 No files in storage!")
            return
        
        files_text = ""
        
        for obj in response['Contents']:
            file_key = obj['Key']
            file_name = file_key.split('/')[-1]
            file_size = obj['Size']
            user_id = file_key.split('/')[0]
            
            files_text += f"📁 File: `{file_name}`\n"
            files_text += f"💾 Size: {human_bytes(file_size)}\n"
            files_text += f"/delete_file {user_id} {file_name}\n\n"
        
        await message.reply(files_text)
        
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")

# Media handler
@app.on_message(filters.video | filters.document)
async def handle_media(_, message):
    media = message.video or message.document

    original_name = media.file_name or f"file_{message.id}.mp4"
    safe_name = original_name.replace(" ", "_")

    user_folder = str(message.from_user.id)
    r2_key = f"{user_folder}/{safe_name}"
    local_path = os.path.join(DOWNLOAD_DIR, safe_name)

    status_msg = await message.reply("🚀 Downloading...")

    start_time = time.time()

    await message.download(
        file_name=local_path,
        progress=progress,
        progress_args=(
            status_msg,
            start_time,
            "Downloading",
            safe_name
        )
    )

    file_size = os.path.getsize(local_path)

    upload_progress = UploadProgress(
        status_msg,
        safe_name,
        file_size
    )

    s3.upload_file(
        local_path,
        WASABI_BUCKET_NAME,
        r2_key,
        Callback=upload_progress,
        ExtraArgs={"ContentType": media.mime_type or "application/octet-stream"}
    )

    os.remove(local_path)

    public_link = generate_presigned_url(r2_key)
    
    if not public_link:
        await status_msg.edit("❌ Error: Could not generate download link!")
        return

    response_text = (
        f"✅ **Upload Completed!**\n\n"
        f"📁 File Name: `{safe_name}`\n"
        f"💾 File Size: {human_bytes(file_size)}\n\n"
        f"🔗 **Download Link:**\n"
        f"{public_link}\n\n"
        f"⏱️ Link expires in 7 days"
    )

    await status_msg.edit(
        response_text,
        disable_web_page_preview=False
    )

# Health check endpoint
async def health_check(request):
    health_status = {
        "status": "healthy" if bot_running else "starting",
        "bot_running": bot_running,
        "timestamp": time.time()
    }
    return web.json_response(health_status)

async def start_health_server():
    app_web = web.Application()
    app_web.router.add_get('/health', health_check)
    
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    
    logger.info(f"Health check server started on port {PORT}")
    return runner

async def main():
    global bot_running
    
    try:
        runner = await start_health_server()
        
        logger.info("🤖 Starting Telegram bot...")
        async with app:
            bot_running = True
            logger.info("✅ Bot is running with Wasabi storage...")
            await app.idle()
            
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        bot_running = False
        raise
    finally:
        bot_running = False

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)
