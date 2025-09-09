import os
from pymongo import MongoClient
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters, ChatMemberHandler
)

# ----------------------------
# Config
# ----------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")

if not BOT_TOKEN or not MONGO_URL:
    raise ValueError("BOT_TOKEN and MONGO_URL must be set in environment.")

# ----------------------------
# Mongo Setup
# ----------------------------
client = MongoClient(MONGO_URL)
db = client["referral_bot"]      # Same DB as AP Bot
users_collection = db["users"]   # Users XP collection
reactions_collection = db["mywin_reacts"]  # Track who reacted to which post

# ----------------------------
# #mywin Message Handler
# ----------------------------
async def filter_mywin_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return

    caption = (message.caption or "").strip().lower()
    has_image = (
        message.photo
        or (message.document and message.document.mime_type.startswith("image"))
    )

    if has_image and caption.startswith("#mywin"):
        parts = caption.split("#mywin", 1)
        game_name = parts[1].strip() if len(parts) > 1 else ""
        if game_name:
            # Add XP to main bot fields
            users_collection.update_one(
                {"user_id": message.from_user.id},
                {"$inc": {"xp": 20, "weekly_xp": 20, "monthly_xp": 20}},
                upsert=True
            )
            return  # Valid submission, do not delete

    # Delete all other messages
    await message.delete()

# ----------------------------
# Reaction Handler
# ----------------------------
async def handle_reaction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reaction = getattr(update, "message_reaction", None)
    if not reaction:
        return

    user_id = reaction.user.id
    msg = reaction.message
    if not msg:
        return

    # Give XP only if first reaction by this user on this message
    if reactions_collection.find_one({"user_id": user_id, "message_id": msg.message_id}):
        return  # Already counted

    users_collection.update_one(
        {"user_id": user_id},
        {"$inc": {"xp": 2, "weekly_xp": 2, "monthly_xp": 2}},
        upsert=True
    )
    reactions_collection.insert_one({"user_id": user_id, "message_id": msg.message_id})

# ----------------------------
# Run Bot
# ----------------------------
def main():
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()

    # Handle all messages in #mywin group
    app_bot.add_handler(MessageHandler(filters.ALL, filter_mywin_media))

    # Handle reactions (one-time XP per user per message)
    app_bot.add_handler(MessageHandler(filters.ALL, handle_reaction, block=False))

    print("✅ #mywin bot running...")
    app_bot.run_polling(poll_interval=5)

if __name__ == "__main__":
    main()
