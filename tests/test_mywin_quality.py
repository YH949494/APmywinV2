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

    def _saturated_checker(self, size=(800, 800), block=20):
        """Alternating red/blue checker — high saturation, non-blank stddev."""
        img = Image.new("RGB", size)
        px = img.load()
        for y in range(size[1]):
            for x in range(size[0]):
                px[x, y] = (255, 10, 10) if ((x // block + y // block) % 2) == 0 else (10, 10, 255)
        return img

    # ------------------------------------------------------------------
    # Resolution
    # ------------------------------------------------------------------
    def test_tiny_image_reject(self):
        img = self._checker(size=(100, 100))
        metrics = analyze_mywin_image(self._to_bytes(img))
        decision = decide_mywin_image_quality(metrics, duplicate_match=False, cfg=MyWinImageQualityConfig())
        self.assertEqual(decision.decision, "REJECT")
        self.assertEqual(decision.reason, "small_resolution")

    def test_below_480_reject(self):
        """Images between 300 and 480 px now also fail the stricter gate."""
        img = self._checker(size=(400, 400))
        metrics = analyze_mywin_image(self._to_bytes(img))
        decision = decide_mywin_image_quality(metrics, duplicate_match=False, cfg=MyWinImageQualityConfig())
        self.assertEqual(decision.decision, "REJECT")
        self.assertEqual(decision.reason, "small_resolution")

    # ------------------------------------------------------------------
    # Blank / solid-color
    # ------------------------------------------------------------------
    def test_blank_image_reject(self):
        img = Image.new("RGB", (800, 800), color=(128, 128, 128))
        metrics = analyze_mywin_image(self._to_bytes(img))
        cfg = MyWinImageQualityConfig(min_file_size_bytes=100)
        decision = decide_mywin_image_quality(metrics, duplicate_match=False, cfg=cfg)
        self.assertEqual(decision.decision, "REJECT")
        self.assertEqual(decision.reason, "blank_image")

    # ------------------------------------------------------------------
    # Duplicate
    # ------------------------------------------------------------------
    def test_duplicate_reject(self):
        img = self._checker()
        metrics = analyze_mywin_image(self._to_bytes(img))
        cfg = MyWinImageQualityConfig(min_file_size_bytes=100)
        decision = decide_mywin_image_quality(metrics, duplicate_match=True, cfg=cfg)
        self.assertEqual(decision.decision, "REJECT")
        self.assertEqual(decision.reason, "duplicate_image")

    # ------------------------------------------------------------------
    # Blur  (unconditional — no file-size loophole)
    # ------------------------------------------------------------------
    def test_blur_reject_small_file(self):
        img = self._checker(size=(600, 600), block=4).filter(ImageFilter.GaussianBlur(radius=8))
        metrics = analyze_mywin_image(self._to_bytes(img, fmt="JPEG"))
        # blank_stddev_threshold=0 isolates the blur check; reject_blur_threshold=200 > actual blur ~110
        cfg = MyWinImageQualityConfig(min_file_size_bytes=100, reject_blur_threshold=200, blank_stddev_threshold=0)
        decision = decide_mywin_image_quality(metrics, duplicate_match=False, cfg=cfg)
        self.assertEqual(decision.decision, "REJECT")
        self.assertEqual(decision.reason, "blur")

    def test_blur_reject_large_file(self):
        """Blur rejection applies even to large files (no file-size loophole)."""
        img = self._checker(size=(900, 900), block=4).filter(ImageFilter.GaussianBlur(radius=10))
        metrics = analyze_mywin_image(self._to_bytes(img, fmt="PNG"))
        # blank_stddev_threshold=0 isolates blur; reject_blur_threshold=300 > actual blur ~73
        cfg = MyWinImageQualityConfig(min_file_size_bytes=100, reject_blur_threshold=300, blank_stddev_threshold=0)
        decision = decide_mywin_image_quality(metrics, duplicate_match=False, cfg=cfg)
        self.assertEqual(decision.decision, "REJECT")
        self.assertEqual(decision.reason, "blur")

    # ------------------------------------------------------------------
    # Over-saturation
    # ------------------------------------------------------------------
    def test_over_saturated_reject(self):
        """Alternating red/blue checker has ~96% saturation — well above the 0.82 gate."""
        img = self._saturated_checker(size=(800, 800))
        metrics = analyze_mywin_image(self._to_bytes(img))
        # reject_blur_threshold=1 so blur doesn't interfere; max_saturation_mean=0.82 < actual ~0.96
        cfg = MyWinImageQualityConfig(min_file_size_bytes=100, reject_blur_threshold=1, max_saturation_mean=0.82)
        decision = decide_mywin_image_quality(metrics, duplicate_match=False, cfg=cfg)
        self.assertEqual(decision.decision, "REJECT")
        self.assertEqual(decision.reason, "over_saturated")

    def test_normal_saturation_not_rejected(self):
        """A realistic checkerboard screenshot (moderate saturation) passes the saturation gate."""
        img = self._checker(size=(800, 800), block=20)
        metrics = analyze_mywin_image(self._to_bytes(img))
        cfg = MyWinImageQualityConfig(min_file_size_bytes=100, reject_blur_threshold=1, ignore_blur_threshold=1, max_saturation_mean=0.82)
        decision = decide_mywin_image_quality(metrics, duplicate_match=False, cfg=cfg)
        # Grayscale checkerboard has saturation_mean == 0; should not be rejected for over_saturated
        self.assertNotEqual(decision.reason, "over_saturated")

    # ------------------------------------------------------------------
    # Moderate blur → IGNORE
    # ------------------------------------------------------------------
    def test_moderate_blur_ignore(self):
        img = self._checker(size=(600, 600), block=6).filter(ImageFilter.GaussianBlur(radius=2))
        metrics = analyze_mywin_image(self._to_bytes(img, fmt="JPEG"))
        # actual blur_score ~1065; set reject below it and ignore above it
        cfg = MyWinImageQualityConfig(
            min_file_size_bytes=100,
            reject_blur_threshold=2,
            ignore_blur_threshold=2000,
            max_saturation_mean=0.99,
        )
        decision = decide_mywin_image_quality(metrics, duplicate_match=False, cfg=cfg)
        self.assertEqual(decision.decision, "IGNORE")

    # ------------------------------------------------------------------
    # Clear image → PASS
    # ------------------------------------------------------------------
    def test_clear_image_pass(self):
        img = self._checker(size=(900, 900), block=10)
        metrics = analyze_mywin_image(self._to_bytes(img, fmt="JPEG"))
        cfg = MyWinImageQualityConfig(min_file_size_bytes=100, ignore_blur_threshold=20, max_saturation_mean=0.99)
        decision = decide_mywin_image_quality(metrics, duplicate_match=False, cfg=cfg)
        self.assertEqual(decision.decision, "PASS")


if __name__ == "__main__":
    unittest.main()
