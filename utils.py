from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, Optional
from urllib.parse import urljoin, urlparse

import requests


def today_yyyy_mm_dd() -> str:
    return date.today().isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def build_logger(log_path: str) -> logging.Logger:
    ensure_dir(os.path.dirname(log_path) or ".")
    logger = logging.getLogger("flipkart_scraper")
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers if script is imported/run multiple times.
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)

    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def random_user_agent(user_agents: list[str]) -> str:
    return random.choice(user_agents)


def polite_sleep(delay_range_seconds: tuple[float, float]) -> float:
    low, high = delay_range_seconds
    delay = random.uniform(low, high)
    time.sleep(delay)
    return delay


_PRICE_RE = re.compile(r"(\d[\d,]*)")


def parse_inr_price(value: Optional[str]) -> Optional[int]:
    """
    Parse values like '₹12,999' or '12,999' into integer rupees.
    Returns None if parsing fails.
    """
    if not value:
        return None
    m = _PRICE_RE.search(value.replace("\xa0", " ").strip())
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


def compute_discount_pct(original_price: Optional[int], sale_price: Optional[int]) -> Optional[float]:
    if not original_price or not sale_price:
        return None
    if original_price <= 0 or sale_price < 0:
        return None
    if sale_price > original_price:
        return 0.0
    return round(((original_price - sale_price) / original_price) * 100.0, 2)


def normalize_url(base_url: str, href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if not href:
        return None
    abs_url = urljoin(base_url, href)
    # Drop fragments; keep query (Flipkart uses tracking params sometimes)
    parsed = urlparse(abs_url)
    return parsed._replace(fragment="").geturl()


def safe_json_dump(path: str, data: Any) -> None:
    ensure_dir(os.path.dirname(path) or ".")
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def extract_json_object_after_marker(text: str, marker: str) -> Optional[Dict[str, Any]]:
    """
    Extract a JSON object that appears in a JS assignment like:
      window.__INITIAL_STATE__ = { ... };

    Uses brace matching so it does not rely on fragile regex across large payloads.
    Returns None when marker/object can't be found or JSON parsing fails.
    """
    start = text.find(marker)
    if start < 0:
        return None
    i = start + len(marker)

    # Skip whitespace
    while i < len(text) and text[i].isspace():
        i += 1
    if i >= len(text) or text[i] != "{":
        return None

    depth = 0
    in_str = False
    esc = False
    end: Optional[int] = None

    for j in range(i, len(text)):
        ch = text[j]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue

        if ch == '"':
            in_str = True
            continue

        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = j + 1
                break

    if end is None:
        return None

    try:
        return json.loads(text[i:end])
    except Exception:
        return None


@dataclass(frozen=True)
class HttpResult:
    url: str
    status_code: int
    text: str


def fetch_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout_seconds: float,
    max_retries: int,
    backoff_base_seconds: float,
    user_agents: list[str],
    logger: logging.Logger,
) -> HttpResult:
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        ua = random_user_agent(user_agents)
        # Flipkart can be sensitive to "synthetic" header combinations. Try a small
        # set of header profiles, starting from the most minimal.
        header_profiles = [
            {"User-Agent": ua},
            {"User-Agent": ua, "Accept-Language": "en-IN,en;q=0.9"},
            {"User-Agent": ua, "Accept-Language": "en-IN,en;q=0.9", "Referer": "https://www.flipkart.com/"},
        ]
        headers = header_profiles[min(attempt - 1, len(header_profiles) - 1)]
        try:
            resp = session.get(url, headers=headers, timeout=timeout_seconds)
            status = int(resp.status_code)
            text = resp.text or ""

            if status >= 400:
                snippet = (text[:240] or "").replace("\n", " ")
                raise requests.HTTPError(f"HTTP {status}. Body snippet: {snippet}")

            return HttpResult(url=url, status_code=status, text=text)
        except Exception as exc:  # noqa: BLE001 - deliberate: enterprise-grade resilience
            last_exc = exc
            sleep_s = backoff_base_seconds ** (attempt - 1) + random.uniform(0.0, 0.35)
            logger.warning(
                "Request failed (attempt %s/%s). url=%s err=%s. Backing off %.2fs",
                attempt,
                max_retries,
                url,
                repr(exc),
                sleep_s,
            )
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed to fetch after {max_retries} retries: {url}. Last error: {last_exc!r}")


def unique_by(items: Iterable[Dict[str, Any]], key: str) -> list[Dict[str, Any]]:
    seen: set[Any] = set()
    out: list[Dict[str, Any]] = []
    for item in items:
        v = item.get(key)
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(item)
    return out

