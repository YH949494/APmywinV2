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

# --- MongoDB ---
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]
mywin_posts = db["mywin_posts"]
reactions_collection = db["mywin_reacts"]

# --- XP function ---
def add_xp(user_id: int, xp: int, game_name: str | None = None):
    """Add XP to user, weekly, monthly, and per-game."""
    update = {"$inc": {"xp": xp, "weekly_xp": xp, "monthly_xp": xp}}
    if game_name:
        game_key = game_name.replace(" ", "_").lower()  # normalize for MongoDB
        update["$inc"][f"games.{game_key}"] = xp
    users_collection.update_one({"_id": user_id}, update, upsert=True)

# --- Filter media ---
async def filter_mywin_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return

    # Only allow messages with images
    has_image = (
        message.photo
        or (message.document and message.document.mime_type.startswith("image"))
    )
    caption = (message.caption or "").strip()

    # Validate caption
    if has_image and caption.lower().startswith("#mywin"):
        parts = caption.split("#mywin", 1)
        game_name = parts[1].strip() if len(parts) > 1 else ""
        user_id = getattr(message.from_user, "id", None)
        if game_name and user_id:
            add_xp(user_id, 20, game_name)
            await message.reply_text(f"✅ +20 XP for {game_name}!")
            # Optional: track valid #mywin posts
            mywin_posts.update_one(
                {"_id": message.message_id},
                {"$set": {"user_id": user_id, "game_name": game_name}},
                upsert=True
            )
            return

    # Delete all invalid submissions
    await message.delete()

# --- Reaction handler ---
async def handle_reactions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mr = getattr(update, "message_reaction", None)
    if not mr:
        return

    original: Message = mr.message
    if not original:
        return

    # Only count reactions on tracked #mywin posts
    post = mywin_posts.find_one({"_id": original.message_id})
    if not post:
        return

    user_id = mr.user.id
    message_id = original.message_id

    # Give +2 XP only once per user per post
    if reactions_collection.find_one({"user_id": user_id, "message_id": message_id}):
        return

    add_xp(user_id, 2)
    reactions_collection.insert_one({"user_id": user_id, "message_id": message_id})

# --- Main ---
def main():
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    # Handle all messages with media filter
    app_bot.add_handler(MessageHandler(filters.ALL, filter_mywin_media))
    # Handle reactions
    app_bot.add_handler(MessageHandler(filters.ALL, handle_reactions))
    # Run bot
    app_bot.run_polling(poll_interval=5)

if __name__ == "__main__":
    main()
