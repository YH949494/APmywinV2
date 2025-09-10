import os
from pymongo import MongoClient
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, ContextTypes, filters

# ----------------------------
# Config
# ----------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URL = os.environ.get("MONGO_URL")

# ----------------------------
# MongoDB Setup
# ----------------------------
client = MongoClient(MONGO_URL)
db = client["referral_bot"]
users_collection = db["users"]

# ----------------------------
# MyWin Media Handler
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
            # Increment XP in the same fields as main bot
            users_collection.update_one(
                {"user_id": message.from_user.id},
                {"$inc": {"xp": 20, "weekly_xp": 20, "monthly_xp": 20}},
                upsert=True
            )
            return

    # Delete everything else
    await message.delete()

# ----------------------------
# Run Bot
# ----------------------------
def main():
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    app_bot.add_handler(MessageHandler(filters.ALL, filter_mywin_media))
    app_bot.run_polling(poll_interval=5)

if __name__ == "__main__":
    main()
