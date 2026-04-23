import io
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from PIL import Image, ImageFilter, ImageStat


@dataclass
class MyWinImageQualityConfig:
    enabled: bool = True
    min_width: int = 480
    min_height: int = 480
    min_file_size_bytes: int = 51200
    reject_blur_threshold: float = 80.0
    ignore_blur_threshold: float = 200.0
    blank_stddev_threshold: float = 8.0
    max_saturation_mean: float = 0.82
    duplicate_hamming_threshold: int = 10
    duplicate_lookback_days: int = 30


@dataclass
class MyWinImageMetrics:
    width: int
    height: int
    file_size: int
    blur_score: float
    blank_stddev: float
    saturation_mean: float
    image_hash: str


@dataclass
class MyWinImageDecision:
    decision: str
    reason: str
    metrics: MyWinImageMetrics
    duplicate_match: bool


def load_mywin_quality_config() -> MyWinImageQualityConfig:
    return MyWinImageQualityConfig(
        enabled=_parse_bool(os.getenv("MYWIN_IMG_FILTER_ENABLED", "1")),
        min_width=int(os.getenv("MYWIN_IMG_MIN_WIDTH", "480")),
        min_height=int(os.getenv("MYWIN_IMG_MIN_HEIGHT", "480")),
        min_file_size_bytes=int(os.getenv("MYWIN_IMG_MIN_FILE_SIZE_BYTES", "51200")),
        reject_blur_threshold=float(os.getenv("MYWIN_IMG_REJECT_BLUR_THRESHOLD", "80")),
        ignore_blur_threshold=float(os.getenv("MYWIN_IMG_IGNORE_BLUR_THRESHOLD", "200")),
        blank_stddev_threshold=float(os.getenv("MYWIN_IMG_BLANK_STDDEV_THRESHOLD", "8")),
        max_saturation_mean=float(os.getenv("MYWIN_IMG_MAX_SATURATION_MEAN", "0.82")),
        duplicate_hamming_threshold=int(os.getenv("MYWIN_IMG_DUPLICATE_HAMMING_THRESHOLD", "10")),
        duplicate_lookback_days=int(os.getenv("MYWIN_IMG_DUPLICATE_LOOKBACK_DAYS", "30")),
    )


def analyze_mywin_image(image_bytes: bytes) -> MyWinImageMetrics:
    file_size = len(image_bytes)
    image_rgb = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    width, height = image_rgb.size

    saturation_mean = _compute_saturation_mean(image_rgb)

    image_gray = image_rgb.convert("L")
    # edge-variance blur proxy (higher = sharper)
    edges = image_gray.filter(ImageFilter.FIND_EDGES)
    blur_score = ImageStat.Stat(edges).var[0]

    blank_stddev = ImageStat.Stat(image_gray).stddev[0]
    image_hash = _dhash_hex(image_gray)

    return MyWinImageMetrics(
        width=width,
        height=height,
        file_size=file_size,
        blur_score=blur_score,
        blank_stddev=blank_stddev,
        saturation_mean=saturation_mean,
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
    if metrics.blur_score < cfg.reject_blur_threshold:
        return MyWinImageDecision("REJECT", "blur", metrics, duplicate_match)
    if metrics.saturation_mean > cfg.max_saturation_mean:
        return MyWinImageDecision("REJECT", "over_saturated", metrics, duplicate_match)
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
        {"created_at": {"$gte": lookback_start}, "hash": {"$exists": True}},
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
        "[MYWIN][QUALITY] decision=%s reason=%s user_id=%s width=%s height=%s file_size=%s blur_score=%.2f blank_stddev=%.2f saturation_mean=%.3f duplicate_match=%s",
        decision.decision,
        decision.reason,
        user_id,
        m.width,
        m.height,
        m.file_size,
        m.blur_score,
        m.blank_stddev,
        m.saturation_mean,
        decision.duplicate_match,
    )


def _parse_bool(value: str) -> bool:
    return (value or "").lower() in {"1", "true", "yes", "on"}


def _compute_saturation_mean(image_rgb: Image.Image) -> float:
    """Return mean HSV saturation in [0, 1] over a 150×150 downsample."""
    small = image_rgb.resize((150, 150), Image.Resampling.LANCZOS)
    pixels = list(small.getdata())
    total = 0.0
    for r, g, b in pixels:
        max_c = max(r, g, b)
        if max_c > 0:
            total += (max_c - min(r, g, b)) / max_c
    return total / len(pixels)


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
