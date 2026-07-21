import unittest

import main


class ParseMyWinCaptionTests(unittest.TestCase):
    def test_valid_mywin_tag_and_game(self):
        result = main.parse_mywin_caption("#mywin Zeus Rising")
        self.assertEqual(
            result,
            {
                "tag": "mywin",
                "game_name": "Zeus Rising",
                "playback_url": None,
                "playback_id": None,
                "submission_format": "tag_and_game",
            },
        )

    def test_valid_comeback_tag_and_game(self):
        result = main.parse_mywin_caption("#comebackisreal Zeus Rising")
        self.assertEqual(result["tag"], "comebackisreal")
        self.assertEqual(result["submission_format"], "tag_and_game")

    def test_hashtag_case_insensitive(self):
        for caption in ("#MYWIN Zeus Rising", "#MyWin Zeus Rising", "#COMEBACKISREAL Zeus"):
            result = main.parse_mywin_caption(caption)
            self.assertIsNotNone(result)
            self.assertIn(result["tag"], {"mywin", "comebackisreal"})

    def test_valid_link_only(self):
        result = main.parse_mywin_caption("https://rx.apreplay.com/aT1oUdG2IV")
        self.assertEqual(
            result,
            {
                "tag": "mywin",
                "game_name": None,
                "playback_url": "https://rx.apreplay.com/aT1oUdG2IV",
                "playback_id": "aT1oUdG2IV",
                "submission_format": "playback_url_only",
            },
        )

    def test_valid_mywin_plus_playback(self):
        caption = "#mywin Zeus Rising\nhttps://rx.apreplay.com/aT1oUdG2IV"
        result = main.parse_mywin_caption(caption)
        self.assertEqual(
            result,
            {
                "tag": "mywin",
                "game_name": "Zeus Rising",
                "playback_url": "https://rx.apreplay.com/aT1oUdG2IV",
                "playback_id": "aT1oUdG2IV",
                "submission_format": "tag_game_and_playback",
            },
        )

    def test_valid_comeback_plus_playback(self):
        caption = "#comebackisreal Zeus Rising\nhttps://rx.apreplay.com/aT1oUdG2IV"
        result = main.parse_mywin_caption(caption)
        self.assertEqual(result["tag"], "comebackisreal")
        self.assertEqual(result["submission_format"], "tag_game_and_playback")
        self.assertEqual(result["playback_id"], "aT1oUdG2IV")

    def test_empty_lines_between_hashtag_and_url_are_ignored(self):
        caption = "\n#mywin Zeus Rising\n\n\nhttps://rx.apreplay.com/aT1oUdG2IV\n\n"
        result = main.parse_mywin_caption(caption)
        self.assertEqual(result["submission_format"], "tag_game_and_playback")

    def test_more_than_two_non_empty_lines_rejected(self):
        caption = "#mywin Zeus Rising\nhttps://rx.apreplay.com/aT1oUdG2IV\nextra third line"
        self.assertIsNone(main.parse_mywin_caption(caption))

    def test_plain_text_rejected(self):
        self.assertIsNone(main.parse_mywin_caption("Big win"))

    def test_plain_text_plus_url_rejected(self):
        caption = "Big win\nhttps://rx.apreplay.com/aT1oUdG2IV"
        self.assertIsNone(main.parse_mywin_caption(caption))

    def test_url_mixed_with_unrelated_text_rejected(self):
        self.assertIsNone(main.parse_mywin_caption("Check my win https://rx.apreplay.com/aT1oUdG2IV"))

    def test_url_on_same_hashtag_line_rejected(self):
        self.assertIsNone(main.parse_mywin_caption("#mywin https://rx.apreplay.com/aT1oUdG2IV"))

    def test_arbitrary_external_link_rejected(self):
        self.assertIsNone(main.parse_mywin_caption("https://youtube.com/example"))

    def test_none_caption_rejected(self):
        self.assertIsNone(main.parse_mywin_caption(None))

    def test_empty_caption_rejected(self):
        self.assertIsNone(main.parse_mywin_caption(""))
        self.assertIsNone(main.parse_mywin_caption("   \n  \n"))


class ValidatePlaybackUrlTests(unittest.TestCase):
    def test_valid_url_accepted(self):
        result = main.validate_playback_url("https://rx.apreplay.com/aT1oUdG2IV")
        self.assertEqual(result["canonical_url"], "https://rx.apreplay.com/aT1oUdG2IV")
        self.assertEqual(result["playback_id"], "aT1oUdG2IV")

    def test_uppercase_hostname_accepted_and_normalized(self):
        result = main.validate_playback_url("https://RX.APREPLAY.COM/aT1oUdG2IV")
        self.assertEqual(result["canonical_url"], "https://rx.apreplay.com/aT1oUdG2IV")
        self.assertEqual(result["playback_id"], "aT1oUdG2IV")

    def test_http_rejected(self):
        self.assertIsNone(main.validate_playback_url("http://rx.apreplay.com/aT1oUdG2IV"))

    def test_wrong_hostname_rejected(self):
        self.assertIsNone(main.validate_playback_url("https://apreplay.com/aT1oUdG2IV"))

    def test_hostname_suffix_attack_rejected(self):
        self.assertIsNone(main.validate_playback_url("https://rx.apreplay.com.evil.com/aT1oUdG2IV"))

    def test_hostname_prefix_attack_rejected(self):
        self.assertIsNone(main.validate_playback_url("https://fake-rx.apreplay.com/aT1oUdG2IV"))

    def test_empty_path_rejected(self):
        self.assertIsNone(main.validate_playback_url("https://rx.apreplay.com/"))

    def test_trailing_slash_rejected(self):
        self.assertIsNone(main.validate_playback_url("https://rx.apreplay.com/aT1oUdG2IV/"))

    def test_multiple_path_segments_rejected(self):
        self.assertIsNone(main.validate_playback_url("https://rx.apreplay.com/id/another-id"))

    def test_query_string_rejected(self):
        self.assertIsNone(main.validate_playback_url("https://rx.apreplay.com/aT1oUdG2IV?redirect=abc"))

    def test_fragment_rejected(self):
        self.assertIsNone(main.validate_playback_url("https://rx.apreplay.com/aT1oUdG2IV#section"))

    def test_username_password_rejected(self):
        self.assertIsNone(main.validate_playback_url("https://user:pass@rx.apreplay.com/aT1oUdG2IV"))

    def test_port_rejected(self):
        self.assertIsNone(main.validate_playback_url("https://rx.apreplay.com:443/aT1oUdG2IV"))

    def test_space_or_extra_text_rejected(self):
        self.assertIsNone(main.validate_playback_url("https://rx.apreplay.com/aT1oUdG2IV extra"))

    def test_playback_id_too_short_rejected(self):
        self.assertIsNone(main.validate_playback_url("https://rx.apreplay.com/abcd"))

    def test_playback_id_too_long_rejected(self):
        self.assertIsNone(main.validate_playback_url("https://rx.apreplay.com/" + "a" * 101))

    def test_playback_id_max_length_accepted(self):
        result = main.validate_playback_url("https://rx.apreplay.com/" + "a" * 100)
        self.assertIsNotNone(result)

    def test_invalid_playback_id_characters_rejected(self):
        self.assertIsNone(main.validate_playback_url("https://rx.apreplay.com/abc$%^123"))


if __name__ == "__main__":
    unittest.main()
