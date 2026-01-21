import os
import re
from datetime import datetime, timezone
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
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
mywin_posts = db["mywin_posts"]  # track valid mywin/comeback posts
xp_events = db["xp_events"]

# Accept either hashtag (case-insensitive), require at least one space + game name
TAG_PATTERN = re.compile(r'^\s*#(?P<tag>mywin|comebackisreal)\s+(?P<game>.+)$', re.IGNORECASE)

def ensure_indexes():
    xp_events.create_index(
        [("user_id", ASCENDING), ("unique_key", ASCENDING)],
        unique=True,
        name="uq_xp_user_unique_key",
    )

# ----------------------------
# MyWin / ComebackIsReal Media Handler
# ----------------------------
async def filter_mywin_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return

    caption_raw = (message.caption or "").strip()  # keep original case for game name

    # detect if it has image
    has_image = bool(
        message.photo
        or (message.document and message.document.mime_type and message.document.mime_type.startswith("image"))
    )

    # extract unique file id
    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_unique_id  # highest quality photo
    elif message.document:
        file_id = message.document.file_unique_id

    # validate caption against accepted tags
    m = TAG_PATTERN.match(caption_raw)
    if has_image and file_id and m:
        tag = m.group("tag").lower()         # "mywin" or "comebackisreal"
        game_name = m.group("game").strip()  # preserve user’s casing

        if game_name:
            # check duplicate by file_id
            if mywin_posts.find_one({"file_id": file_id}):
                await message.delete()
                return

            # insert record
            now = datetime.now(timezone.utc)         
            mywin_posts.insert_one({
                "file_id": file_id,
                "user_id": message.from_user.id,
                "tag": tag,
                "game_name": game_name,
                "ts": now,
            })

            reason = "mywin_submission" if tag == "mywin" else "comeback_submission"
            xp_event = {
                "user_id": message.from_user.id,
                "xp": 20,
                "reason": reason,
                "unique_key": f"mywin:{file_id}",
                "ts": now,
                "created_at": now,
                "meta": {
                    "file_id": file_id,
                    "tag": tag,
                    "game_name": game_name,
                },
            }
            try:
                xp_events.insert_one(xp_event)
            except DuplicateKeyError:
                pass
            return

    # delete anything else
    await message.delete()

# ----------------------------
# Run Bot
# ----------------------------
def main():
    ensure_indexes() 
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    # (Optional but recommended) only process photos or image documents to reduce noise:
    img_filter = (filters.PHOTO | filters.Document.IMAGE)
    app_bot.add_handler(MessageHandler(img_filter, filter_mywin_media))
    app_bot.run_polling(poll_interval=5)

if __name__ == "__main__":
    main()
