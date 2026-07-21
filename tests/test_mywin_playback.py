import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from pymongo.errors import DuplicateKeyError

import main
from mywin_quality import MyWinImageQualityConfig


# ----------------------------------------------------------------------------
# Fakes
# ----------------------------------------------------------------------------
class FakeMywinPosts:
    """Emulates mywin_posts with the unique partial index on playback_id."""

    def __init__(self):
        self.docs = []
        self._next_id = 1
        self._lock = threading.Lock()

    def find_one(self, filt):
        for d in self.docs:
            if all(d.get(k) == v for k, v in filt.items()):
                return d
        return None

    def insert_one(self, doc):
        with self._lock:
            playback_id = doc.get("playback_id")
            if isinstance(playback_id, str):
                for d in self.docs:
                    if d.get("playback_id") == playback_id:
                        raise DuplicateKeyError(
                            'E11000 duplicate key error collection: referral_bot.mywin_posts '
                            'index: uq_mywin_playback_id dup key: { playback_id: "%s" }' % playback_id,
                            code=11000,
                            details={
                                "keyPattern": {"playback_id": 1},
                                "keyValue": {"playback_id": playback_id},
                            },
                        )
            new_doc = dict(doc)
            new_doc["_id"] = self._next_id
            self._next_id += 1
            self.docs.append(new_doc)
        return SimpleNamespace(inserted_id=new_doc["_id"])


class FakeUniqueCollection:
    """Generic fake for xp_events / events with a compound unique key."""

    def __init__(self, unique_keys):
        self.docs = []
        self.unique_keys = unique_keys

    def insert_one(self, doc):
        key = tuple(doc.get(k) for k in self.unique_keys)
        for d in self.docs:
            if tuple(d.get(k) for k in self.unique_keys) == key:
                raise DuplicateKeyError("dup", code=11000, details={})
        self.docs.append(dict(doc))


class FakeMembers:
    def __init__(self):
        self.docs = {}

    def update_one(self, filt, update, upsert=False):
        uid = filt["uid"]
        if uid in self.docs:
            return SimpleNamespace(matched_count=1, upserted_id=None)
        if upsert:
            self.docs[uid] = dict(update.get("$setOnInsert", {}))
            return SimpleNamespace(matched_count=0, upserted_id=uid)
        return SimpleNamespace(matched_count=0, upserted_id=None)


class FakeMessage:
    def __init__(self, caption, user_id=1, chat_id=100, message_id=1, file_unique_id="photo_1"):
        self.caption = caption
        self.photo = [SimpleNamespace(file_unique_id=file_unique_id, file_id=file_unique_id + "_full")]
        self.document = None
        self.from_user = SimpleNamespace(id=user_id)
        self.chat_id = chat_id
        self.message_id = message_id
        self.deleted = False
        self.replies = []

    async def delete(self):
        self.deleted = True

    async def reply_text(self, text, reply_markup=None, reply_to_message_id=None):
        self.replies.append(
            {"text": text, "reply_markup": reply_markup, "reply_to_message_id": reply_to_message_id}
        )


def _make_update(message):
    return SimpleNamespace(message=message)


_FAKE_CONTEXT = SimpleNamespace(bot=SimpleNamespace())


class MyWinPlaybackTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fake_posts = FakeMywinPosts()
        self.fake_xp_events = FakeUniqueCollection(("user_id", "unique_key"))
        self.fake_events = FakeUniqueCollection(("type", "uid", "chat_id", "message_id"))
        self.fake_members = FakeMembers()
        self.fake_image_hashes = SimpleNamespace()

        self._patches = [
            patch.object(main, "mywin_posts", self.fake_posts),
            patch.object(main, "xp_events", self.fake_xp_events),
            patch.object(main, "events", self.fake_events),
            patch.object(main, "members", self.fake_members),
            patch.object(main, "mywin_image_hashes", self.fake_image_hashes),
            patch.object(
                main,
                "load_mywin_quality_config",
                return_value=MyWinImageQualityConfig(enabled=False),
            ),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)

    async def _submit(self, caption, user_id=1, message_id=None, file_unique_id="photo_1"):
        if message_id is None:
            message_id = len(self.fake_posts.docs) + len(self.fake_events.docs) + 1
        message = FakeMessage(
            caption, user_id=user_id, message_id=message_id, file_unique_id=file_unique_id
        )
        await main.filter_mywin_media(_make_update(message), _FAKE_CONTEXT)
        return message

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    async def test_link_only_persists_expected_fields(self):
        message = await self._submit("https://rx.apreplay.com/aT1oUdG2IV")
        self.assertFalse(message.deleted)
        self.assertEqual(len(self.fake_posts.docs), 1)
        doc = self.fake_posts.docs[0]
        self.assertEqual(doc["tag"], "mywin")
        self.assertIsNone(doc["game_name"])
        self.assertEqual(doc["playback_url"], "https://rx.apreplay.com/aT1oUdG2IV")
        self.assertEqual(doc["playback_id"], "aT1oUdG2IV")
        self.assertEqual(doc["submission_format"], "playback_url_only")

    async def test_hashtag_only_remains_valid_without_playback_fields(self):
        message = await self._submit("#mywin Zeus Rising")
        self.assertFalse(message.deleted)
        doc = self.fake_posts.docs[0]
        self.assertEqual(doc["tag"], "mywin")
        self.assertEqual(doc["game_name"], "Zeus Rising")
        self.assertNotIn("playback_url", doc)
        self.assertNotIn("playback_id", doc)

    async def test_replay_fields_written_to_xp_metadata(self):
        await self._submit("#mywin Zeus Rising\nhttps://rx.apreplay.com/aT1oUdG2IV")
        xp_doc = self.fake_xp_events.docs[0]
        self.assertEqual(xp_doc["meta"]["playback_url"], "https://rx.apreplay.com/aT1oUdG2IV")
        self.assertEqual(xp_doc["meta"]["playback_id"], "aT1oUdG2IV")
        self.assertEqual(xp_doc["meta"]["submission_format"], "tag_game_and_playback")
        self.assertEqual(xp_doc["xp"], 20)

    async def test_replay_fields_written_to_mywin_valid_event(self):
        await self._submit("#comebackisreal Zeus Rising\nhttps://rx.apreplay.com/aT1oUdG2IV")
        event_doc = self.fake_events.docs[0]
        self.assertEqual(event_doc["type"], "MYWIN_VALID")
        self.assertEqual(event_doc["meta"]["playback_url"], "https://rx.apreplay.com/aT1oUdG2IV")
        self.assertEqual(event_doc["meta"]["playback_id"], "aT1oUdG2IV")
        self.assertEqual(event_doc["meta"]["submission_format"], "tag_game_and_playback")

    # ------------------------------------------------------------------
    # Duplicate playback link protection
    # ------------------------------------------------------------------
    async def test_first_use_of_playback_id_accepted(self):
        message = await self._submit("https://rx.apreplay.com/aT1oUdG2IV")
        self.assertFalse(message.deleted)
        self.assertEqual(len(self.fake_posts.docs), 1)

    async def test_second_use_same_user_rejected(self):
        await self._submit("https://rx.apreplay.com/aT1oUdG2IV", user_id=1, message_id=1)
        second = await self._submit(
            "https://rx.apreplay.com/aT1oUdG2IV", user_id=1, message_id=2, file_unique_id="photo_2"
        )
        self.assertTrue(second.deleted)
        self.assertEqual(len(self.fake_posts.docs), 1)

    async def test_second_use_different_user_rejected(self):
        await self._submit("https://rx.apreplay.com/aT1oUdG2IV", user_id=1, message_id=1)
        second = await self._submit(
            "https://rx.apreplay.com/aT1oUdG2IV", user_id=2, message_id=2, file_unique_id="photo_2"
        )
        self.assertTrue(second.deleted)
        self.assertEqual(len(self.fake_posts.docs), 1)

    async def test_same_replay_id_different_image_rejected(self):
        await self._submit(
            "#mywin Zeus Rising\nhttps://rx.apreplay.com/aT1oUdG2IV",
            message_id=1,
            file_unique_id="image_a",
        )
        second = await self._submit(
            "#mywin Zeus Rising\nhttps://rx.apreplay.com/aT1oUdG2IV",
            message_id=2,
            file_unique_id="image_b",
        )
        self.assertTrue(second.deleted)
        self.assertEqual(len(self.fake_posts.docs), 1)

    async def test_link_only_then_hashtag_format_is_duplicate(self):
        await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=1, file_unique_id="a")
        second = await self._submit(
            "#mywin Zeus Rising\nhttps://rx.apreplay.com/aT1oUdG2IV", message_id=2, file_unique_id="b"
        )
        self.assertTrue(second.deleted)
        self.assertEqual(len(self.fake_posts.docs), 1)

    async def test_hashtag_format_then_link_only_is_duplicate(self):
        await self._submit(
            "#comebackisreal Zeus Rising\nhttps://rx.apreplay.com/aT1oUdG2IV", message_id=1, file_unique_id="a"
        )
        second = await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=2, file_unique_id="b")
        self.assertTrue(second.deleted)
        self.assertEqual(len(self.fake_posts.docs), 1)

    async def test_hostname_casing_cannot_bypass_dedup(self):
        await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=1, file_unique_id="a")
        second = await self._submit("https://RX.APREPLAY.COM/aT1oUdG2IV", message_id=2, file_unique_id="b")
        self.assertTrue(second.deleted)
        self.assertEqual(len(self.fake_posts.docs), 1)

    async def test_different_case_sensitive_playback_ids_remain_distinct(self):
        first = await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=1, file_unique_id="a")
        second = await self._submit("https://rx.apreplay.com/at1oudg2iv", message_id=2, file_unique_id="b")
        self.assertFalse(first.deleted)
        self.assertFalse(second.deleted)
        self.assertEqual(len(self.fake_posts.docs), 2)

    async def test_duplicate_link_receives_no_xp(self):
        await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=1, file_unique_id="a")
        await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=2, file_unique_id="b")
        self.assertEqual(len(self.fake_xp_events.docs), 1)

    async def test_duplicate_link_writes_no_mywin_valid(self):
        await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=1, file_unique_id="a")
        await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=2, file_unique_id="b")
        self.assertEqual(len(self.fake_events.docs), 1)

    async def test_duplicate_link_sends_no_playback_button(self):
        await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=1, file_unique_id="a")
        second = await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=2, file_unique_id="b")
        self.assertEqual(second.replies, [])

    async def test_duplicate_link_uses_reason_duplicate_playback_link(self):
        await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=1, file_unique_id="a")
        with self.assertLogs(level="INFO") as captured:
            await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=2, file_unique_id="b")
        joined = "\n".join(captured.output)
        self.assertIn("reason=duplicate_playback_link", joined)
        self.assertIn("count_as_low_quality=False", joined)

    async def test_existing_accepted_post_remains_unchanged(self):
        await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=1, file_unique_id="a")
        original = dict(self.fake_posts.docs[0])
        await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=2, file_unique_id="b")
        self.assertEqual(self.fake_posts.docs[0], original)

    def test_concurrent_inserts_same_playback_id_only_one_accepted(self):
        # Validates the atomic unique-constraint guarantee the handler relies
        # on: even if two submissions both pass an early "not a duplicate"
        # lookup, only one insert_one() may ultimately succeed.
        posts = FakeMywinPosts()
        barrier = threading.Barrier(2)
        results = []

        def worker(file_id):
            barrier.wait()
            try:
                posts.insert_one(
                    {
                        "file_id": file_id,
                        "playback_id": "aT1oUdG2IV",
                        "playback_url": "https://rx.apreplay.com/aT1oUdG2IV",
                    }
                )
                results.append("accepted")
            except DuplicateKeyError:
                results.append("rejected")

        threads = [threading.Thread(target=worker, args=(f"file_{i}",)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(sorted(results), ["accepted", "rejected"])
        self.assertEqual(len(posts.docs), 1)

    # ------------------------------------------------------------------
    # Playback button
    # ------------------------------------------------------------------
    async def test_button_sent_for_link_only_submission(self):
        message = await self._submit("https://rx.apreplay.com/aT1oUdG2IV")
        self.assertEqual(len(message.replies), 1)

    async def test_button_sent_for_hashtag_plus_replay(self):
        message = await self._submit("#mywin Zeus Rising\nhttps://rx.apreplay.com/aT1oUdG2IV")
        self.assertEqual(len(message.replies), 1)

    async def test_button_text_exact(self):
        message = await self._submit("https://rx.apreplay.com/aT1oUdG2IV")
        button = message.replies[0]["reply_markup"].inline_keyboard[0][0]
        self.assertEqual(button.text, "▶️ Watch Winning Playback")

    async def test_button_url_equals_canonical_replay_url(self):
        message = await self._submit("https://RX.APREPLAY.COM/aT1oUdG2IV")
        button = message.replies[0]["reply_markup"].inline_keyboard[0][0]
        self.assertEqual(button.url, "https://rx.apreplay.com/aT1oUdG2IV")

    async def test_reply_text_exact(self):
        message = await self._submit("https://rx.apreplay.com/aT1oUdG2IV")
        self.assertEqual(message.replies[0]["text"], "🎬 Winning playback available")

    async def test_reply_references_original_image(self):
        message = await self._submit("https://rx.apreplay.com/aT1oUdG2IV", message_id=42)
        self.assertEqual(message.replies[0]["reply_to_message_id"], 42)

    async def test_no_button_for_hashtag_only_submission(self):
        message = await self._submit("#mywin Zeus Rising")
        self.assertEqual(message.replies, [])

    async def test_button_send_failure_does_not_invalidate_submission(self):
        with patch.object(FakeMessage, "reply_text", side_effect=RuntimeError("telegram down")):
            message = await self._submit("https://rx.apreplay.com/aT1oUdG2IV")
        self.assertFalse(message.deleted)
        self.assertEqual(len(self.fake_posts.docs), 1)

    async def test_button_send_failure_does_not_reverse_xp(self):
        with patch.object(FakeMessage, "reply_text", side_effect=RuntimeError("telegram down")):
            await self._submit("https://rx.apreplay.com/aT1oUdG2IV")
        self.assertEqual(len(self.fake_xp_events.docs), 1)

    async def test_button_send_failure_does_not_delete_mywin_valid(self):
        with patch.object(FakeMessage, "reply_text", side_effect=RuntimeError("telegram down")):
            await self._submit("https://rx.apreplay.com/aT1oUdG2IV")
        self.assertEqual(len(self.fake_events.docs), 1)


if __name__ == "__main__":
    unittest.main()
