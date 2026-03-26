import logging
import os
import re
from datetime import datetime, timezone
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, ContextTypes, filters

from mywin_quality import (
    analyze_mywin_image,
    decide_mywin_image_quality,
    is_near_duplicate_hash,
    load_mywin_quality_config,
    log_mywin_quality,
    store_hash_record,
)
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
mywin_image_hashes = db["mywin_image_hashes"]

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
        mywin_image_hashes.create_index(
            [("created_at", ASCENDING)],
            name="idx_mywin_image_hashes_created_at",
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
        quality_decision = "PASS"

        if game_name:
            if tag == "mywin":
                cfg = load_mywin_quality_config()
                if cfg.enabled:
                    try:
                        media_file_id = message.photo[-1].file_id if message.photo else message.document.file_id
                        telegram_file = await context.bot.get_file(media_file_id)
                        image_bytes = bytes(await telegram_file.download_as_bytearray())
                        metrics = analyze_mywin_image(image_bytes)
                        duplicate_match = is_near_duplicate_hash(
                            mywin_image_hashes,
                            metrics.image_hash,
                            cfg.duplicate_hamming_threshold,
                            cfg.duplicate_lookback_days,
                        )
                        decision = decide_mywin_image_quality(metrics, duplicate_match, cfg)
                        quality_decision = decision.decision
                        log_mywin_quality(message.from_user.id, decision)
                        if quality_decision == "REJECT":
                            store_hash_record(
                                mywin_image_hashes,
                                message.from_user.id,
                                message.message_id,
                                metrics.image_hash,
                                quality_decision,
                            )
                            await message.delete()
                            return
                        store_hash_record(
                            mywin_image_hashes,
                            message.from_user.id,
                            message.message_id,
                            metrics.image_hash,
                            quality_decision,
                        )
                    except Exception as exc:
                        logging.exception(
                            "[MYWIN][QUALITY] decision=PASS reason=analysis_error user_id=%s err=%s",
                            message.from_user.id,
                            exc,
                        )

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
                "quality_decision": quality_decision,
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
         
            if quality_decision == "PASS":
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
    app_bot = ApplicationBuilder().token(BOT_TOKEN).build()
    # (Optional but recommended) only process photos or image documents to reduce noise:
    img_filter = (filters.PHOTO | filters.Document.IMAGE)
    app_bot.add_handler(MessageHandler(img_filter, filter_mywin_media))
    app_bot.run_polling(poll_interval=5)

if __name__ == "__main__":
    main()
