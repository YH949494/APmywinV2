import io
import logging
import os
from dataclasses import dataclass
from io import BytesIO
from datetime import datetime, timedelta, timezone

from PIL import Image, ImageFilter, ImageStat, UnidentifiedImageError

try:
    import numpy as np  # type: ignore
    NUMPY_AVAILABLE = True
except Exception:
    np = None
    NUMPY_AVAILABLE = False

try:
    import cv2  # type: ignore
    CV2_AVAILABLE = True
except Exception:
    cv2 = None
    CV2_AVAILABLE = False


@dataclass
class MyWinImageQualityConfig:
    enabled: bool = True
    min_width: int = 300
    min_height: int = 300
    min_file_size_bytes: int = 30720
    reject_blur_threshold: float = 50.0
    ignore_blur_threshold: float = 120.0
    reject_blur_max_file_size_bytes: int = 81920
    blank_stddev_threshold: float = 8.0
    duplicate_hamming_threshold: int = 5
    duplicate_lookback_days: int = 30


@dataclass
class MyWinImageMetrics:
    width: int
    height: int
    file_size: int
    blur_score: float
    blank_stddev: float
    image_hash: str


@dataclass
class MyWinImageDecision:
    decision: str
    reason: str
    metrics: MyWinImageMetrics
    duplicate_match: bool


@dataclass
class QualityDecision:
    passed: bool
    reason: str
    width: int = 0
    height: int = 0
    file_size_kb: int = 0
    sharpness: float = 0.0
    stddev_gray: float = 0.0
    cv2_available: bool = True
    cv2_unavailable: bool = False
    sharpness_method: str = "cv2_laplacian"
    error_type: str = ""

    def as_meta(self) -> dict:
        return {
            "passed": self.passed,
            "reason": self.reason,
            "width": self.width,
            "height": self.height,
            "file_size_kb": self.file_size_kb,
            "sharpness": round(self.sharpness, 2),
            "stddev_gray": round(self.stddev_gray, 2),
            "cv2_available": self.cv2_available,
            "cv2_unavailable": self.cv2_unavailable,
            "sharpness_method": self.sharpness_method,
            "error_type": self.error_type,
        }


def load_mywin_quality_config() -> MyWinImageQualityConfig:
    return MyWinImageQualityConfig(
        enabled=_parse_bool(os.getenv("MYWIN_IMG_FILTER_ENABLED", "1")),
        min_width=int(os.getenv("MYWIN_IMG_MIN_WIDTH", "300")),
        min_height=int(os.getenv("MYWIN_IMG_MIN_HEIGHT", "300")),
        min_file_size_bytes=int(os.getenv("MYWIN_IMG_MIN_FILE_SIZE_BYTES", "30720")),
        reject_blur_threshold=float(os.getenv("MYWIN_IMG_REJECT_BLUR_THRESHOLD", "50")),
        ignore_blur_threshold=float(os.getenv("MYWIN_IMG_IGNORE_BLUR_THRESHOLD", "120")),
        reject_blur_max_file_size_bytes=int(os.getenv("MYWIN_IMG_REJECT_BLUR_MAX_FILE_SIZE_BYTES", "81920")),
        blank_stddev_threshold=float(os.getenv("MYWIN_IMG_BLANK_STDDEV_THRESHOLD", "8")),
        duplicate_hamming_threshold=int(os.getenv("MYWIN_IMG_DUPLICATE_HAMMING_THRESHOLD", "5")),
        duplicate_lookback_days=int(os.getenv("MYWIN_IMG_DUPLICATE_LOOKBACK_DAYS", "30")),
    )


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def analyze_image_quality(file_bytes: bytes) -> QualityDecision:
    min_width = _env_int("MYWIN_MIN_WIDTH", 720)
    min_height = _env_int("MYWIN_MIN_HEIGHT", 720)
    min_file_size_kb = _env_int("MYWIN_MIN_FILE_SIZE_KB", 80)
    min_gray_stddev = _env_float("MYWIN_MIN_GRAY_STDDEV", 18.0)
    min_sharpness = _env_float("MYWIN_MIN_SHARPNESS", 110.0)

    try:
        file_size_kb = int(round(len(file_bytes) / 1024.0))
        pil_img = Image.open(BytesIO(file_bytes)).convert("RGB")
        width, height = pil_img.size

        gray_img = pil_img.convert("L")
        gray = np.array(gray_img) if NUMPY_AVAILABLE and np is not None else None
        if CV2_AVAILABLE and cv2 is not None and gray is not None:
            sharpness = float(cv2.Laplacian(gray, cv2.CV_64F).var())
            sharpness_method = "cv2_laplacian"
        else:
            sharpness = 0.0
            sharpness_method = "skipped_cv2_unavailable"
        stddev_gray = float(ImageStat.Stat(gray_img).stddev[0])
        cv2_unavailable = not (CV2_AVAILABLE and cv2 is not None and gray is not None)

        if width < min_width or height < min_height:
            return QualityDecision(False, "low_resolution", width, height, file_size_kb, sharpness, stddev_gray, CV2_AVAILABLE, cv2_unavailable, sharpness_method)
        if file_size_kb < min_file_size_kb:
            return QualityDecision(False, "small_file_size", width, height, file_size_kb, sharpness, stddev_gray, CV2_AVAILABLE, cv2_unavailable, sharpness_method)
        if stddev_gray < min_gray_stddev:
            return QualityDecision(False, "low_detail_or_near_blank", width, height, file_size_kb, sharpness, stddev_gray, CV2_AVAILABLE, cv2_unavailable, sharpness_method)
        if not cv2_unavailable and sharpness < min_sharpness:
            return QualityDecision(False, "blurry_or_compressed", width, height, file_size_kb, sharpness, stddev_gray, CV2_AVAILABLE, cv2_unavailable, sharpness_method)

        return QualityDecision(True, "pass", width, height, file_size_kb, sharpness, stddev_gray, CV2_AVAILABLE, cv2_unavailable, sharpness_method)
    except (UnidentifiedImageError, OSError, ValueError):
        return QualityDecision(False, "invalid_image", cv2_available=CV2_AVAILABLE, cv2_unavailable=not CV2_AVAILABLE, sharpness_method="none", error_type="decode_failure")
    except Exception as exc:
        logging.exception("[MYWIN_QUALITY] analysis failed err=%s", exc)
        return QualityDecision(False, "analysis_failed", cv2_available=CV2_AVAILABLE, cv2_unavailable=not CV2_AVAILABLE, sharpness_method="none", error_type="analysis_failure")


def analyze_mywin_image(image_bytes: bytes) -> MyWinImageMetrics:
    file_size = len(image_bytes)
    image = Image.open(io.BytesIO(image_bytes)).convert("L")
    width, height = image.size

    # edge-variance blur proxy (higher = sharper)
    edges = image.filter(ImageFilter.FIND_EDGES)
    blur_score = ImageStat.Stat(edges).var[0]

    blank_stddev = ImageStat.Stat(image).stddev[0]
    image_hash = _dhash_hex(image)

    return MyWinImageMetrics(
        width=width,
        height=height,
        file_size=file_size,
        blur_score=blur_score,
        blank_stddev=blank_stddev,
        image_hash=image_hash,
    )


def decide_mywin_image_quality(
    metrics: MyWinImageMetrics,
    duplicate_match: bool,
    cfg: MyWinImageQualityConfig,
) -> MyWinImageDecision:
    if metrics.width < cfg.min_width:
        return MyWinImageDecision("REJECT", "small_resolution", metrics, duplicate_match)
    if metrics.height < cfg.min_height:
        return MyWinImageDecision("REJECT", "small_resolution", metrics, duplicate_match)
    if metrics.file_size < cfg.min_file_size_bytes:
        return MyWinImageDecision("REJECT", "small_file_size", metrics, duplicate_match)
    if metrics.blank_stddev < cfg.blank_stddev_threshold:
        return MyWinImageDecision("REJECT", "blank_image", metrics, duplicate_match)
    if duplicate_match:
        return MyWinImageDecision("REJECT", "duplicate_image", metrics, duplicate_match)
    if (
        metrics.blur_score < cfg.reject_blur_threshold
        and metrics.file_size <= cfg.reject_blur_max_file_size_bytes
    ):
        return MyWinImageDecision("REJECT", "extreme_blur_small_file", metrics, duplicate_match)
    if metrics.blur_score < cfg.ignore_blur_threshold:
        return MyWinImageDecision("IGNORE", "blur", metrics, duplicate_match)
    return MyWinImageDecision("PASS", "clear", metrics, duplicate_match)


def is_near_duplicate_hash(
    collection,
    image_hash: str,
    threshold: int,
    lookback_days: int,
) -> bool:
    now = datetime.now(timezone.utc)
    lookback_start = now - timedelta(days=lookback_days)
    cursor = collection.find(
        {"$or": [{"created_at": {"$gte": lookback_start}}, {"ts": {"$gte": lookback_start}}], "hash": {"$exists": True}},
        {"hash": 1},
    )
    for doc in cursor:
        existing_hash = doc.get("hash")
        if not existing_hash:
            continue
        if _hamming_distance_hex(existing_hash, image_hash) <= threshold:
            return True
    return False


def store_hash_record(collection, user_id: int, message_id: int, image_hash: str, decision: str) -> None:
    collection.insert_one(
        {
            "user_id": user_id,
            "message_id": message_id,
            "hash": image_hash,
            "decision": decision,
            "created_at": datetime.now(timezone.utc),
        }
    )


def log_mywin_quality(user_id: int, decision: MyWinImageDecision) -> None:
    m = decision.metrics
    logging.info(
        "[MYWIN][QUALITY] decision=%s reason=%s user_id=%s width=%s height=%s file_size=%s blur_score=%.2f blank_stddev=%.2f duplicate_match=%s",
        decision.decision,
        decision.reason,
        user_id,
        m.width,
        m.height,
        m.file_size,
        m.blur_score,
        m.blank_stddev,
        decision.duplicate_match,
    )


def _parse_bool(value: str) -> bool:
    return (value or "").lower() in {"1", "true", "yes", "on"}


def _dhash_hex(image: Image.Image, hash_size: int = 8) -> str:
    resized = image.resize((hash_size + 1, hash_size), Image.Resampling.LANCZOS)
    pixels = list(resized.getdata())
    bits = []
    for row in range(hash_size):
        row_start = row * (hash_size + 1)
        for col in range(hash_size):
            left = pixels[row_start + col]
            right = pixels[row_start + col + 1]
            bits.append(1 if left > right else 0)

    value = 0
    for bit in bits:
        value = (value << 1) | bit
    return f"{value:0{hash_size * hash_size // 4}x}"


def _hamming_distance_hex(h1: str, h2: str) -> int:
    xor = int(h1, 16) ^ int(h2, 16)
    return xor.bit_count()
