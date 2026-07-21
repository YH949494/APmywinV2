import logging
import os
import re
import urllib.parse
from datetime import datetime, timezone
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
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

# ----------------------------
# Caption parsing / playback link validation
# ----------------------------
# A hashtag line: "#mywin Game Name" / "#comebackisreal Game Name" (case-insensitive tag).
HASHTAG_LINE_PATTERN = re.compile(r'^#(?P<tag>mywin|comebackisreal)\s+(?P<game>.+)$', re.IGNORECASE)
URL_IN_TEXT_PATTERN = re.compile(r'https?://', re.IGNORECASE)

PLAYBACK_HOST = "rx.apreplay.com"
PLAYBACK_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{5,100}")


def is_valid_game_name(name):
    if not name:
        return False
    name = name.strip()
    if not name:
        return False
    # A "game name" that actually embeds a URL is not a game name (e.g. "#mywin https://...").
    if URL_IN_TEXT_PATTERN.search(name):
        return False
    return True


def validate_playback_url(value):
    """Validate an AdvantPlay Replay playback URL.

    Returns {"canonical_url": ..., "playback_id": ...} or None when invalid.
    """
    if not value or re.search(r'\s', value):
        return None
    try:
        parsed = urllib.parse.urlparse(value)
    except ValueError:
        return None

    if parsed.scheme != "https":
        return None
    try:
        hostname = parsed.hostname
    except ValueError:
        return None
    if not hostname or hostname.lower() != PLAYBACK_HOST:
        return None
    if parsed.username or parsed.password:
        return None
    if parsed.port is not None:
        return None
    if parsed.query or parsed.fragment:
        return None

    path = parsed.path or ""
    if not path.startswith("/"):
        return None
    segments = path.split("/")
    if len(segments) != 2:
        return None
    playback_id = segments[1]
    if not playback_id or not PLAYBACK_ID_PATTERN.fullmatch(playback_id):
        return None

    canonical_url = f"https://{PLAYBACK_HOST}/{playback_id}"
    return {"canonical_url": canonical_url, "playback_id": playback_id}


def parse_mywin_caption(caption):
    """Parse a MyWin/ComebackIsReal caption into a structured submission.

    Supports four formats:
      1. Playback link only.
      2. "#mywin Game Name"
      3. "#comebackisreal Game Name"
      4. "#mywin Game Name" / "#comebackisreal Game Name" followed by a playback link
         on its own line.

    Returns a dict or None when the caption does not match any supported format.
    """
    if caption is None:
        return None

    lines = [line.strip() for line in caption.split("\n")]
    non_empty = [line for line in lines if line]

    if not non_empty or len(non_empty) > 2:
        return None

    if len(non_empty) == 1:
        line = non_empty[0]

        m = HASHTAG_LINE_PATTERN.match(line)
        if m:
            game_name = m.group("game").strip()
            if not is_valid_game_name(game_name):
                return None
            return {
                "tag": m.group("tag").lower(),
                "game_name": game_name,
                "playback_url": None,
                "playback_id": None,
                "submission_format": "tag_and_game",
            }

        validated = validate_playback_url(line)
        if validated:
            return {
                "tag": "mywin",
                "game_name": None,
                "playback_url": validated["canonical_url"],
                "playback_id": validated["playback_id"],
                "submission_format": "playback_url_only",
            }

        return None

    # Two non-empty lines: hashtag + game name, then a playback link.
    tag_line, url_line = non_empty
    m = HASHTAG_LINE_PATTERN.match(tag_line)
    if not m:
        return None

    game_name = m.group("game").strip()
    if not is_valid_game_name(game_name):
        return None

    validated = validate_playback_url(url_line)
    if not validated:
        return None

    return {
        "tag": m.group("tag").lower(),
        "game_name": game_name,
        "playback_url": validated["canonical_url"],
        "playback_id": validated["playback_id"],
        "submission_format": "tag_game_and_playback",
    }


def _migrate_duplicate_playback_ids():
    """Keep the earliest accepted post per playback_id, clearing later duplicates.

    Runs before the unique partial index is created so pre-existing duplicate
    playback_id values (from before deduplication was enforced) don't block
    index creation.
    """
    pipeline = [
        {"$match": {"playback_id": {"$exists": True, "$type": "string"}}},
        {"$sort": {"ts": 1, "_id": 1}},
        {"$group": {"_id": "$playback_id", "ids": {"$push": "$_id"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
    ]
    try:
        duplicate_groups = list(mywin_posts.aggregate(pipeline))
    except Exception:
        logging.exception("[MYWIN_INDEX] failed to audit duplicate playback_id values")
        raise

    for group in duplicate_groups:
        keep_id, *dupe_ids = group["ids"]
        try:
            result = mywin_posts.update_many(
                {"_id": {"$in": dupe_ids}},
                {"$unset": {"playback_id": ""}},
            )
        except Exception:
            logging.exception(
                "[MYWIN_INDEX] failed to clear duplicate playback_id=%s dupe_ids=%s",
                group["_id"], dupe_ids,
            )
            raise
        logging.warning(
            "[MYWIN_INDEX] duplicate_playback_id_migrated playback_id=%s kept=%s cleared=%s modified=%s",
            group["_id"], keep_id, dupe_ids, result.modified_count,
        )


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

    try:
        _migrate_duplicate_playback_ids()
        mywin_posts.create_index(
            [("playback_id", ASCENDING)],
            unique=True,
            partialFilterExpression={
                "playback_id": {
                    "$exists": True,
                    "$type": "string",
                }
            },
            name="uq_mywin_playback_id",
        )
    except Exception:
        logging.exception("[MYWIN_INDEX] failed to create uq_mywin_playback_id index")
        raise


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


def _is_playback_duplicate_error(exc: DuplicateKeyError) -> bool:
    details = exc.details or {}
    key_pattern = details.get("keyPattern") or {}
    if "playback_id" in key_pattern:
        return True
    key_value = details.get("keyValue") or {}
    if "playback_id" in key_value:
        return True
    err_msg = details.get("errmsg") or str(exc)
    return "uq_mywin_playback_id" in err_msg or "playback_id" in err_msg


async def _reject_duplicate_playback_link(message, playback_id, playback_url):
    logging.info(
        "[MYWIN_MODERATION] reason=duplicate_playback_link user_id=%s playback_id=%s "
        "playback_url=%s count_as_low_quality=False",
        message.from_user.id,
        playback_id,
        playback_url,
    )
    await message.delete()


async def _send_playback_button(message, playback_url):
    try:
        await message.reply_text(
            "🎬 Winning playback available",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        "▶️ Watch Winning Playback",
                        url=playback_url,
                    )
                ]
            ]),
            reply_to_message_id=message.message_id,
        )
    except Exception as exc:
        logging.exception(
            "[MYWIN_PLAYBACK_BUTTON] failed to send playback button user_id=%s playback_url=%s err=%s",
            message.from_user.id,
            playback_url,
            exc,
        )


# ----------------------------
# MyWin / ComebackIsReal Media Handler
# ----------------------------
async def filter_mywin_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return

    caption_raw = message.caption or ""  # keep original case/lines for parsing

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

    # validate caption against accepted formats
    parsed = parse_mywin_caption(caption_raw)
    if not (has_image and file_id and parsed):
        # delete anything else
        await message.delete()
        return

    tag = parsed["tag"]                       # "mywin" or "comebackisreal"
    game_name = parsed["game_name"]            # preserve user's casing, or None
    playback_url = parsed["playback_url"]
    playback_id = parsed["playback_id"]
    submission_format = parsed["submission_format"]
    quality_decision = "PASS"

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

    # early playback-id lookup for a faster rejection (final enforcement is the
    # unique partial index on mywin_posts.playback_id, see below)
    if playback_id and mywin_posts.find_one({"playback_id": playback_id}):
        await _reject_duplicate_playback_link(message, playback_id, playback_url)
        return

    # check duplicate by file_id
    if mywin_posts.find_one({"file_id": file_id}):
        await message.delete()
        return

    # insert record
    now = datetime.now(timezone.utc)
    post_doc = {
        "file_id": file_id,
        "user_id": message.from_user.id,
        "tag": tag,
        "game_name": game_name,
        "submission_format": submission_format,
        "quality_decision": quality_decision,
        "ts": now,
        "created_at": now,
    }
    if playback_id:
        post_doc["playback_url"] = playback_url
        post_doc["playback_id"] = playback_id

    try:
        mywin_posts.insert_one(post_doc)
    except DuplicateKeyError as exc:
        if playback_id and _is_playback_duplicate_error(exc):
            await _reject_duplicate_playback_link(message, playback_id, playback_url)
        else:
            await message.delete()
        return

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
                "playback_url": playback_url,
                "playback_id": playback_id,
                "submission_format": submission_format,
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
                "playback_url": playback_url,
                "playback_id": playback_id,
                "submission_format": submission_format,
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

        if playback_url:
            await _send_playback_button(message, playback_url)

# ----------------------------
# Run Bot
# ----------------------------
async def _telegram_error_handler(update, context):
    logging.exception(
        "[TELEGRAM_ERROR] "
        "update=%s "
        "error_type=%s "
        "error=%s",
        update,
        type(context.error).__name__,
        context.error,
    )

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    logging.info(
        "[BOOT] MYWIN_VERSION=2026-07-02-network-debug-v1"
    )

    ensure_indexes()

    app_bot = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .build()
    )

    logging.info(
        "[BOOT] BOT_TOKEN_PRESENT=%s",
        bool(BOT_TOKEN),
    )

    logging.info(
        "[BOOT] MONGO_PRESENT=%s",
        bool(MONGO_URL),
    )

    # (Optional but recommended) only process photos or image documents to reduce noise:
    img_filter = (filters.PHOTO | filters.Document.IMAGE)
    app_bot.add_handler(MessageHandler(img_filter, filter_mywin_media))

    logging.info(
        "[BOOT] ERROR_HANDLER_REGISTERING"
    )

    app_bot.add_error_handler(_telegram_error_handler)

    logging.info(
        "[BOOT] ERROR_HANDLER_REGISTERED"
    )

    logging.info(
        "[BOOT] STARTING_POLLING"
    )

    app_bot.run_polling(
        poll_interval=5,
        timeout=30,
        read_timeout=30,
        write_timeout=30,
        connect_timeout=30,
        pool_timeout=30,
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    main()
