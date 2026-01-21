import logging
from datetime import datetime, timezone

from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError

logger = logging.getLogger(__name__)


def _to_utc_aware(value):
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def backfill_mywin_to_xp_events(
    db,
    *,
    xp_per_post: int = 20,
    batch_limit: int = 1000,
    dry_run: bool = False,
) -> dict:
    xp_events = db["xp_events"]
    mywin_posts = db["mywin_posts"]

    xp_events.create_index(
        [("user_id", ASCENDING), ("unique_key", ASCENDING)],
        unique=True,
        name="uq_xp_user_unique_key",
    )

    stats = {"scanned": 0, "inserted": 0, "dup": 0, "skipped": 0, "errors": 0}
    now_utc = datetime.now(timezone.utc)

    cursor = mywin_posts.find({}).sort([("ts", ASCENDING), ("_id", ASCENDING)]).batch_size(
        max(batch_limit, 1)
    )

    batch = []
    for post in cursor:
        batch.append(post)
        if len(batch) >= batch_limit:
            _process_batch(batch, xp_events, stats, xp_per_post, dry_run, now_utc)
            batch = []

    if batch:
        _process_batch(batch, xp_events, stats, xp_per_post, dry_run, now_utc)

    return stats


def _process_batch(posts, xp_events, stats, xp_per_post, dry_run, fallback_ts):
    for post in posts:
        stats["scanned"] += 1
        try:
            file_id = (post.get("file_id") or "").strip()
            user_id = post.get("user_id")
            tag = (post.get("tag") or "").strip().lower()
            game_name = (post.get("game_name") or "").strip()

            if not file_id or user_id is None:
                stats["skipped"] += 1
                continue

            reason = "mywin_submission" if tag == "mywin" else "comeback_submission"
            ts_utc = _to_utc_aware(post.get("ts")) or fallback_ts

            doc = {
                "user_id": int(user_id),
                "xp": xp_per_post,
                "type": reason,
                "reason": reason,
                "unique_key": f"mywin:{file_id}",
                "ts": ts_utc,
                "created_at": ts_utc,
                "meta": {
                    "file_id": file_id,
                    "tag": tag,
                    "game_name": game_name,
                    "source": "backfill_mywin_posts",
                },
            }

            if dry_run:
                stats["inserted"] += 1
                continue

            xp_events.insert_one(doc)
            stats["inserted"] += 1
        except DuplicateKeyError:
            stats["dup"] += 1
        except Exception:
            stats["errors"] += 1
            logger.exception("[MYWIN_BACKFILL] failed to process post", extra={"post_id": post.get("_id")})
