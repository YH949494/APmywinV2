import os
from pymongo import MongoClient
from telegram import Update, Message
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    ContextTypes, 
    filters,
)

# --- Secrets --- 
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is not set. Use: fly secrets set BOT_TOKEN=...")
if not MONGO_URL:
    raise ValueError("MONGO_URL is not set. Use: fly secrets set MONGO_URL=...")

# --- Mongo ---
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]
mywin_posts = db["mywin_posts"]
reactions_collection = db["mywin_reacts"]

def add_xp(user_id: int, xp: int, game_name: str | None = None):
    update = {"$inc": {"xp": xp, "weekly_xp": xp, "monthly_xp": xp}}
    if game_name:
        update["$inc"][f"games.{game_name}"] = xp
    users_collection.update_one({"_id": user_id}, update, upsert=True)

async def filter_mywin_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return

    caption = (message.caption or "").strip().lower()

    # Check if message contains allowed image
    has_image = (
        message.photo
        or (message.document and message.document.mime_type.startswith("image"))
    )

    # Allow only if it has image AND proper caption
    if has_image and caption.startswith("#mywin"):
        parts = caption.split("#mywin", 1)
        game_name = parts[1].strip() if len(parts) > 1 else ""
        if game_name:
            add_xp(message.from_user.id, 20, game_name)
            await message.reply_text(f"✅ +20 XP for {game_name}!")
            return

    # Delete everything else
    await message.delete()

def main():
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    app_bot.add_handler(MessageHandler(filters.ALL, filter_mywin_media))
    app_bot.run_polling(poll_interval=5)

if __name__ == "__main__":
    main()
