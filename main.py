import logging
import os
import re
from datetime import datetime, timezone
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, ContextTypes, filters

from backfill_mywin import backfill_mywin_to_xp_events
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
events = db["events"]
members = db["members"]
admin_cache = db["admin_cache"]

# Accept either hashtag (case-insensitive), require at least one space + game name
TAG_PATTERN = re.compile(r'^\s*#(?P<tag>mywin|comebackisreal)\s+(?P<game>.+)$', re.IGNORECASE)

def ensure_indexes():
    try:
        xp_events.create_index(
            [("user_id", ASCENDING), ("unique_key", ASCENDING)],
            unique=True,
            name="uq_xp_user_unique_key",
        )
        events.create_index(
            [("type", ASCENDING), ("uid", ASCENDING), ("chat_id", ASCENDING), ("message_id", ASCENDING)],
            unique=True,
            name="uq_events_type_uid_chat_message",
        )
        members.create_index(
            [("uid", ASCENDING)],
            unique=True,
            name="uq_members_uid",
        )     
    except DuplicateKeyError:
        pass


def _parse_bool(value: str) -> bool:
    return (value or "").lower() in {"1", "true", "yes", "on"}


def _run_settle_jobs():
    for name in (
        "settle_pending_referrals_with_cache_clear",
        "settle_referral_snapshots_with_cache_clear",
        "settle_xp_snapshots_with_cache_clear",
    ):
        func = globals().get(name)
        if callable(func):
            func()


def run_mywin_backfill_if_enabled():
    if os.getenv("RUNNER_MODE") != "worker":
        return
    if os.getenv("RUN_MYWIN_BACKFILL") != "1":
        return

    if admin_cache.find_one({"_id": "mywin_backfill_done", "done": True}):
        logging.info("[MYWIN_BACKFILL] skipped (already marked done)")
        return

    dry_run = _parse_bool(os.getenv("MYWIN_BACKFILL_DRY_RUN", "0"))
    xp_per_post = int(os.getenv("MYWIN_XP_PER_POST", "20"))
    batch_limit = int(os.getenv("MYWIN_BACKFILL_BATCH", "1000"))

    logging.info(
        "[MYWIN_BACKFILL] start dry_run=%s xp_per_post=%s batch_limit=%s",
        dry_run,
        xp_per_post,
        batch_limit,
    )
    try:
        stats = backfill_mywin_to_xp_events(
            db,
            xp_per_post=xp_per_post,
            batch_limit=batch_limit,
            dry_run=dry_run,
        )
        _run_settle_jobs()
        if not dry_run:
            admin_cache.update_one(
                {"_id": "mywin_backfill_done"},
                {
                    "$set": {
                        "done": True,
                        "ts": datetime.now(timezone.utc),
                        "inserted": stats["inserted"],
                        "dup": stats["dup"],
                    }
                },
                upsert=True,
            )
        logging.info(
            "[MYWIN_BACKFILL] done scanned=%s inserted=%s dup=%s skipped=%s errors=%s",
            stats["scanned"],
            stats["inserted"],
            stats["dup"],
            stats["skipped"],
            stats["errors"],
        )
    except Exception as exc:
        logging.exception("[MYWIN_BACKFILL] failed err=%s", exc)

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

            member_result = members.update_one(
                {"uid": message.from_user.id},
                {
                    "$setOnInsert": {
                        "uid": message.from_user.id,
                        "level": 1,
                        "role": "member",
                        "affiliate_status": "none",
                        "kpi": {
                            "mywin": 0,
                            "cbir": 0,
                        },
                    }
                },
                upsert=True,
            )
            if member_result.upserted_id is not None:
                logging.info("member_upsert=1 uid=%s", message.from_user.id)
         
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


            event_doc = {
                "type": "MYWIN_VALID",
                "uid": message.from_user.id,
                "chat_id": message.chat_id,
                "message_id": message.message_id,
                "ts": now,
                "tags": ["mywin"] if tag == "mywin" else ["cbir"],
                "meta": {
                    "tag": tag,
                    "game_name": game_name,
                },
            }
            try:
                events.insert_one(event_doc)
                logging.info(
                    "event_written=1 type=%s uid=%s chat_id=%s message_id=%s",
                    event_doc["type"],
                    event_doc["uid"],
                    event_doc["chat_id"],
                    event_doc["message_id"],
                )
            except DuplicateKeyError:
                logging.info(
                    "event_dedup=1 type=%s uid=%s chat_id=%s message_id=%s",
                    event_doc["type"],
                    event_doc["uid"],
                    event_doc["chat_id"],
                    event_doc["message_id"],
                )         
            return

    # delete anything else
    await message.delete()

# ----------------------------
# Run Bot
# ----------------------------
def main():
    ensure_indexes()
    run_mywin_backfill_if_enabled()
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    # (Optional but recommended) only process photos or image documents to reduce noise:
    img_filter = (filters.PHOTO | filters.Document.IMAGE)
    app_bot.add_handler(MessageHandler(img_filter, filter_mywin_media))
    app_bot.run_polling(poll_interval=5)

if __name__ == "__main__":
    main()
