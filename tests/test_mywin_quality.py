import io
import unittest

from PIL import Image, ImageFilter

from mywin_quality import (
    MyWinImageQualityConfig,
    analyze_mywin_image,
    decide_mywin_image_quality,
)


class MyWinQualityTests(unittest.TestCase):
    def _to_bytes(self, image: Image.Image, fmt: str = "PNG") -> bytes:
        buf = io.BytesIO()
        image.save(buf, format=fmt)
        return buf.getvalue()

    def _checker(self, size=(800, 800), block=20):
        image = Image.new("L", size)
        px = image.load()
        for y in range(size[1]):
            for x in range(size[0]):
                px[x, y] = 255 if ((x // block + y // block) % 2) else 0
        return image.convert("RGB")

    def test_tiny_image_reject(self):
        img = self._checker(size=(100, 100))
        metrics = analyze_mywin_image(self._to_bytes(img))
        decision = decide_mywin_image_quality(metrics, duplicate_match=False, cfg=MyWinImageQualityConfig())
        self.assertEqual(decision.decision, "REJECT")

    def test_blank_image_reject(self):
        img = Image.new("RGB", (800, 800), color=(128, 128, 128))
        metrics = analyze_mywin_image(self._to_bytes(img))
        cfg = MyWinImageQualityConfig(min_file_size_bytes=100)
        decision = decide_mywin_image_quality(metrics, duplicate_match=False, cfg=cfg)
        self.assertEqual(decision.decision, "REJECT")

    def test_duplicate_reject(self):
        img = self._checker()
        metrics = analyze_mywin_image(self._to_bytes(img))
        cfg = MyWinImageQualityConfig(min_file_size_bytes=100)
        decision = decide_mywin_image_quality(metrics, duplicate_match=True, cfg=cfg)
        self.assertEqual(decision.decision, "REJECT")

    def test_extreme_blur_small_file_reject(self):
        img = self._checker(size=(400, 400), block=4).filter(ImageFilter.GaussianBlur(radius=6))
        metrics = analyze_mywin_image(self._to_bytes(img, fmt="JPEG"))
        cfg = MyWinImageQualityConfig(min_file_size_bytes=100, reject_blur_threshold=20, reject_blur_max_file_size_bytes=200000)
        decision = decide_mywin_image_quality(metrics, duplicate_match=False, cfg=cfg)
        self.assertEqual(decision.decision, "REJECT")

    def test_moderate_blur_ignore(self):
        img = self._checker(size=(600, 600), block=6).filter(ImageFilter.GaussianBlur(radius=2))
        metrics = analyze_mywin_image(self._to_bytes(img, fmt="JPEG"))
        cfg = MyWinImageQualityConfig(
            min_file_size_bytes=100,
            reject_blur_threshold=2,
            ignore_blur_threshold=40,
            reject_blur_max_file_size_bytes=100,
        )
        decision = decide_mywin_image_quality(metrics, duplicate_match=False, cfg=cfg)
        self.assertEqual(decision.decision, "IGNORE")

    def test_clear_image_pass(self):
        img = self._checker(size=(900, 900), block=10)
        metrics = analyze_mywin_image(self._to_bytes(img, fmt="JPEG"))
        cfg = MyWinImageQualityConfig(min_file_size_bytes=100, ignore_blur_threshold=20)
        decision = decide_mywin_image_quality(metrics, duplicate_match=False, cfg=cfg)
        self.assertEqual(decision.decision, "PASS")


if __name__ == "__main__":
    unittest.main()
