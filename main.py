import os
from datetime import datetime
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
mywin_posts = db["mywin_posts"]  # track valid mywin posts

TAG_PATTERN = re.compile(r'^\s*#(?P<tag>mywin|comebackisreal)\s+(?P<game>.+)$', re.IGNORECASE)

# ----------------------------
# MyWin Media Handler
# ----------------------------
async def filter_mywin_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return

    caption = (message.caption or "").strip().lower()

    # detect if it has image
    has_image = (
        message.photo
        or (message.document and message.document.mime_type.startswith("image"))
    )

    # extract unique file id
    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_unique_id  # highest quality photo
    elif message.document:
        file_id = message.document.file_unique_id

    # only allow posts with image + proper caption
    m = TAG_PATTERN.match(caption_raw)
    if has_image and file_id and m:
        tag = m.group("tag").lower()         # "mywin" or "comebackisreal"
        game_name = m.group("game").strip()  # preserve user’s casing

        if game_name:
            # check duplicate by file_id
            if mywin_posts.find_one({"file_id": file_id}):
                await message.delete()
                return

            # insert record into mywin_posts
            mywin_posts.insert_one({
                "file_id": file_id,
                "user_id": message.from_user.id,
                "game_name": game_name,
                "ts": datetime.utcnow()
            })

            # increment XP (first time only)
            users_collection.update_one(
                {"user_id": message.from_user.id},
                {"$inc": {"xp": 20, "weekly_xp": 20, "monthly_xp": 20}},
                upsert=True
            )
            return

    # delete anything else
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
