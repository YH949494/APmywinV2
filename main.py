import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError 
from telegram import Update, ChatPermissions
from telegram.ext import ApplicationBuilder, MessageHandler, ContextTypes, filters 

from mywin_quality import analyze_image_quality
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
moderation_events = db["moderation_events"]

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
        moderation_events.create_index(
            [("type", ASCENDING), ("uid", ASCENDING), ("chat_id", ASCENDING), ("message_id", ASCENDING)],
            unique=True,
            name="uq_moderation_type_uid_chat_message",
        )
        moderation_events.create_index([("ts", ASCENDING)], name="ix_moderation_ts")
        mywin_posts.create_index([("file_id", ASCENDING)], unique=True, name="uq_mywin_file_id")
    except DuplicateKeyError:
        pass


def _parse_bool(value: str) -> bool:
    return (value or "").lower() in {"1", "true", "yes", "on"}


def _parse_int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _parse_float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


async def _restrict_user_24h(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    message = update.message
    if not message or not message.from_user:
        return False

    mute_hours = _parse_int_env("MYWIN_LOW_QUALITY_MUTE_HOURS", 24)
    until_dt = datetime.now(timezone.utc) + timedelta(hours=mute_hours)
    try:
        await context.bot.restrict_chat_member(
            chat_id=message.chat_id,
            user_id=message.from_user.id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until_dt,
        )
        return True
    except Exception as exc:
        logging.exception(
            "[MYWIN_MOD] mute_failed uid=%s chat_id=%s err=%s",
            message.from_user.id,
            message.chat_id,
            exc,
        )
        return False


def _get_member_set_on_insert(uid: int) -> dict:
    return {
        "uid": uid,
        "level": 1,
        "role": "member",
        "affiliate_status": "none",
        "kpi": {"mywin": 0, "cbir": 0},
        "moderation": {
            "mywin_low_quality_count": 0,
            "last_low_quality_reason": None,
            "last_low_quality_at": None,
            "mywin_reject_count": 0,
            "mywin_duplicate_count": 0,
            "mywin_invalid_caption_count": 0,
            "mywin_rate_limited_count": 0,
        },
    }


def _insert_moderation_event(message, now: datetime, reason: str, quality_meta: dict):
    try:
        moderation_events.insert_one({
            "type": "MYWIN_REJECT",
            "uid": message.from_user.id,
            "chat_id": message.chat_id,
            "message_id": message.message_id,
            "ts": now,
            "meta": {
                "reason": reason,
                **(quality_meta or {}),
            },
        })
    except DuplicateKeyError:
        pass


def _is_shadow_banned(uid: int) -> bool:
    threshold = _parse_int_env("MYWIN_SHADOW_BAN_THRESHOLD", 8)
    if threshold <= 0:
        return False
    doc = members.find_one({"uid": uid}, {"moderation.mywin_low_quality_count": 1})
    count = int((((doc or {}).get("moderation") or {}).get("mywin_low_quality_count") or 0))
    return count >= threshold


def _extract_media_dimensions(message):
    width = None
    height = None
    if message.photo:
        width = getattr(message.photo[-1], "width", None)
        height = getattr(message.photo[-1], "height", None)
    elif message.document:
        width = getattr(message.document, "width", None)
        height = getattr(message.document, "height", None)
    return width, height


def _is_rate_limited(uid: int, now: datetime):
    min_gap_seconds = _parse_float_env("MYWIN_RATE_LIMIT_SECONDS", 10.0)
    doc = members.find_one({"uid": uid}, {"moderation.last_submission_at": 1})
    last_submission = (((doc or {}).get("moderation") or {}).get("last_submission_at"))
    if min_gap_seconds > 0 and isinstance(last_submission, datetime):
        delta = (now - last_submission).total_seconds()
        if delta < min_gap_seconds:
            return True, delta

    filter_query = {"uid": uid}
    if isinstance(last_submission, datetime):
        filter_query["moderation.last_submission_at"] = last_submission
    else:
        filter_query["$or"] = [
            {"moderation.last_submission_at": {"$exists": False}},
            {"moderation.last_submission_at": None},
        ]

    try:
        update_result = members.update_one(
            filter_query,
            {"$setOnInsert": _get_member_set_on_insert(uid), "$set": {"moderation.last_submission_at": now}},
            upsert=True,
        )
    except DuplicateKeyError:
        latest = members.find_one({"uid": uid}, {"moderation.last_submission_at": 1})
        latest_submission = (((latest or {}).get("moderation") or {}).get("last_submission_at"))
        if isinstance(latest_submission, datetime):
            delta = (now - latest_submission).total_seconds()
            return (delta < min_gap_seconds) if min_gap_seconds > 0 else False, delta
        return False, 0.0
    if update_result.matched_count == 0 and update_result.upserted_id is None:
        latest = members.find_one({"uid": uid}, {"moderation.last_submission_at": 1})
        latest_submission = (((latest or {}).get("moderation") or {}).get("last_submission_at"))
        if isinstance(latest_submission, datetime):
            delta = (now - latest_submission).total_seconds()
            return (delta < min_gap_seconds) if min_gap_seconds > 0 else False, delta
    return False, 0.0


async def _handle_low_quality_reject(update: Update, context: ContextTypes.DEFAULT_TYPE, reason: str, quality_meta: dict, count_as_low_quality: bool = True):
    message = update.message
    if not message or not message.from_user:
        return

    now = datetime.now(timezone.utc)
    threshold = _parse_int_env("MYWIN_LOW_QUALITY_MUTE_THRESHOLD", 3)
    inc_ops = {"moderation.mywin_reject_count": 1}
    if reason == "rate_limited":
        inc_ops["moderation.mywin_rate_limited_count"] = 1
    if count_as_low_quality:
        inc_ops["moderation.mywin_low_quality_count"] = 1
    member_doc = members.find_one_and_update(
        {"uid": message.from_user.id},
        {
            "$setOnInsert": _get_member_set_on_insert(message.from_user.id),
            "$inc": inc_ops,
            "$set": {
                "moderation.last_low_quality_reason": reason,
                "moderation.last_low_quality_at": now,
            },
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    new_count = int((((member_doc or {}).get("moderation") or {}).get("mywin_low_quality_count")) or 0)
    should_mute = new_count >= threshold
    mute_applied = await _restrict_user_24h(update, context) if should_mute else False

    _insert_moderation_event(
        message,
        now,
        reason,
        {
            "low_quality_count": new_count,
            "mute_triggered": bool(should_mute),
            "mute_applied": bool(mute_applied),
            **(quality_meta or {}),
        },
    )


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

    now = datetime.now(timezone.utc)
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
            if _is_shadow_banned(message.from_user.id):
                await _handle_low_quality_reject(
                    update,
                    context,
                    reason="shadow_banned",
                    quality_meta={},
                    count_as_low_quality=False,
                )
                return

            rate_limited, delta_seconds = _is_rate_limited(message.from_user.id, now)
            if rate_limited:
                await _handle_low_quality_reject(
                    update,
                    context,
                    reason="rate_limited",
                    quality_meta={"seconds_since_last_submit": round(delta_seconds, 3)},
                    count_as_low_quality=False,
                )
                await message.delete()
                return

            min_width = _parse_int_env("MYWIN_MIN_WIDTH", 720)
            min_height = _parse_int_env("MYWIN_MIN_HEIGHT", 720)
            meta_width, meta_height = _extract_media_dimensions(message)
            if (
                isinstance(meta_width, int)
                and isinstance(meta_height, int)
                and (meta_width < min_width or meta_height < min_height)
            ):
                await _handle_low_quality_reject(
                    update,
                    context,
                    reason="low_resolution",
                    quality_meta={
                        "metadata_only": True,
                        "width": meta_width,
                        "height": meta_height,
                    },
                )
                await message.delete()
                return

            tg_file = None
            file_bytes = None
            try:
                if message.photo:
                    tg_file = await context.bot.get_file(message.photo[-1].file_id)
                elif message.document:
                    tg_file = await context.bot.get_file(message.document.file_id)
                if tg_file:
                    file_bytes = await tg_file.download_as_bytearray()
            except Exception as exc:
                logging.exception(
                    "[MYWIN_QUALITY] download_failed uid=%s chat_id=%s message_id=%s err=%s",
                    message.from_user.id,
                    message.chat_id,
                    message.message_id,
                    exc,
                )
                await _handle_low_quality_reject(
                    update,
                    context,
                    reason="download_failed",
                    quality_meta={"error_type": "download_failure"},
                )
                await message.delete()
                return

            file_size_kb = int(round(len(bytes(file_bytes or b"")) / 1024.0))
            min_file_size_kb = _parse_int_env("MYWIN_MIN_FILE_SIZE_KB", 80)
            if file_size_kb < min_file_size_kb:
                await _handle_low_quality_reject(
                    update,
                    context,
                    reason="small_file_size",
                    quality_meta={"file_size_kb": file_size_kb},
                )
                await message.delete()
                return

            quality = analyze_image_quality(bytes(file_bytes or b""))
            if not quality.passed:
                await _handle_low_quality_reject(
                    update,
                    context,
                    reason=quality.reason,
                    quality_meta=quality.as_meta(),
                )
                await message.delete()
                return

            # check duplicate by file_id
            if mywin_posts.find_one({"file_id": file_id}):
                await message.delete()
                return

            # insert record
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
                    "$setOnInsert": _get_member_set_on_insert(message.from_user.id)
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
                "type": reason,
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

    if message.from_user:
        members.update_one(
            {"uid": message.from_user.id},
            {
                "$setOnInsert": {
                    **_get_member_set_on_insert(message.from_user.id),
                },                
                "$inc": {"moderation.mywin_invalid_caption_count": 1},
                "$set": {"moderation.last_invalid_caption_at": now},
            },
            upsert=True,
        )
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
