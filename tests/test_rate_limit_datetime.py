import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import main


class _FakeMembers:
    def __init__(self, last_submission=None):
        self.last_submission = last_submission
        self.updated_now = None

    def find_one(self, _filter, _projection):
        if self.last_submission is None:
            return {"moderation": {}}
        return {"moderation": {"last_submission_at": self.last_submission}}

    def update_one(self, _filter, update, upsert=False):
        self.updated_now = update["$set"]["moderation.last_submission_at"]
        return SimpleNamespace(matched_count=1, upserted_id=None)


class RateLimitDatetimeTests(unittest.TestCase):
    def test_naive_last_submission_does_not_crash(self):
        now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        last_submission = datetime(2026, 1, 1, 11, 59, 55)
        fake_members = _FakeMembers(last_submission=last_submission)
        with patch.object(main, "members", fake_members), patch.object(main, "_parse_float_env", return_value=10.0):
            limited, delta = main._is_rate_limited(123, now)
        self.assertTrue(limited)
        self.assertEqual(delta, 5.0)

    def test_aware_last_submission_does_not_crash(self):
        now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        last_submission = datetime(2026, 1, 1, 11, 59, 55, tzinfo=timezone.utc)
        fake_members = _FakeMembers(last_submission=last_submission)
        with patch.object(main, "members", fake_members), patch.object(main, "_parse_float_env", return_value=10.0):
            limited, delta = main._is_rate_limited(123, now)
        self.assertTrue(limited)
        self.assertEqual(delta, 5.0)

    def test_missing_last_submission_allows_and_sets_timestamp(self):
        now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        fake_members = _FakeMembers(last_submission=None)
        with patch.object(main, "members", fake_members), patch.object(main, "_parse_float_env", return_value=10.0):
            limited, delta = main._is_rate_limited(123, now)
        self.assertFalse(limited)
        self.assertEqual(delta, 0.0)
        self.assertEqual(fake_members.updated_now, now)

    def test_future_last_submission_negative_delta_is_safe(self):
        now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        last_submission = now + timedelta(seconds=5)
        fake_members = _FakeMembers(last_submission=last_submission)
        with patch.object(main, "members", fake_members), patch.object(main, "_parse_float_env", return_value=10.0):
            limited, delta = main._is_rate_limited(123, now)
        self.assertTrue(limited)
        self.assertEqual(delta, -5.0)


if __name__ == "__main__":
    unittest.main()
