import os
from datetime import datetime, timezone
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

MONGO_URL = os.environ.get("MONGO_URL")
if not MONGO_URL:
    raise SystemExit("Missing MONGO_URL env var")

DB_NAME = os.environ.get("DB_NAME", "referral_bot")
XP_PER_POST = int(os.environ.get("MYWIN_XP", "20"))
BATCH_LIMIT = int(os.environ.get("BATCH_LIMIT", "1000"))
DRY_RUN = os.environ.get("DRY_RUN", "0").lower() in ("1", "true", "yes", "on")

client = MongoClient(MONGO_URL)
db = client[DB_NAME]

mywin_posts = db["mywin_posts"]
xp_events = db["xp_events"]

def ensure_indexes():
    # Idempotency: one XP event per (user_id, unique_key)
    xp_events.create_index(
        [("user_id", ASCENDING), ("unique_key", ASCENDING)],
        unique=True,
        name="uq_xp_user_unique_key",
    )
    # Optional: speed for scanning
    mywin_posts.create_index([("ts", ASCENDING)], name="ix_mywin_ts")
    mywin_posts.create_index([("file_id", ASCENDING)], name="ix_mywin_file_id")

def to_utc_aware(dt):
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None

def main():
    ensure_indexes()

    total = mywin_posts.count_documents({})
    print(f"[backfill] mywin_posts total={total} DRY_RUN={DRY_RUN}")

    inserted = dup = skipped = errors = 0

    # Stream in ts order (oldest first)
    cursor = mywin_posts.find({}).sort([("ts", 1), ("_id", 1)]).batch_size(500)

    batch = []
    for post in cursor:
        batch.append(post)
        if len(batch) >= BATCH_LIMIT:
            i, d, s, e = process_batch(batch)
            inserted += i; dup += d; skipped += s; errors += e
            batch = []

    if batch:
        i, d, s, e = process_batch(batch)
        inserted += i; dup += d; skipped += s; errors += e

    print(f"[backfill] done inserted={inserted} dup={dup} skipped={skipped} errors={errors}")

def process_batch(posts):
    inserted = dup = skipped = errors = 0

    for post in posts:
        try:
            file_id = (post.get("file_id") or "").strip()
            uid = post.get("user_id")
            tag = (post.get("tag") or "").strip().lower()
            game_name = (post.get("game_name") or "").strip()
            ts = to_utc_aware(post.get("ts")) or datetime.now(timezone.utc)

            if not file_id or not uid:
                skipped += 1
                continue

            # Distinguish reasons if you want; keep stable values
            reason = "mywin_submission" if tag == "mywin" else "comeback_submission"

            doc = {
                "user_id": int(uid),
                "xp": XP_PER_POST,
                "reason": reason,
                "unique_key": f"mywin:{file_id}",
                "ts": ts,
                "created_at": ts,
                "meta": {
                    "file_id": file_id,
                    "tag": tag,
                    "game_name": game_name,
                    "source": "backfill_mywin_posts",
                },
            }

            if DRY_RUN:
                inserted += 1
                continue

            xp_events.insert_one(doc)
            inserted += 1

        except DuplicateKeyError:
            dup += 1
            continue
        except Exception as ex:
            errors += 1
            print(f"[backfill][error] _id={post.get('_id')} uid={post.get('user_id')} err={ex}")

    print(f"[backfill] batch inserted={inserted} dup={dup} skipped={skipped} errors={errors}")
    return inserted, dup, skipped, errors

if __name__ == "__main__":
    main()
