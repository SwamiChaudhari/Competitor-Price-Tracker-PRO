from __future__ import annotations

import logging
import random
import re
from dataclasses import asdict, dataclass
from typing import Optional
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

import requests
from bs4 import BeautifulSoup

import config
from utils import (
    build_logger,
    compute_discount_pct,
    extract_json_object_after_marker,
    fetch_with_retries,
    normalize_url,
    parse_inr_price,
    polite_sleep,
    safe_json_dump,
    today_yyyy_mm_dd,
    unique_by,
)


@dataclass
class Product:
    product_name: str
    original_price: Optional[int]
    sale_price: Optional[int]
    discount_pct: Optional[float]
    url: str


def _normalize_discount(
    original_price: Optional[int], sale_price: Optional[int], discount_pct: Optional[float]
) -> Optional[float]:
    """
    Keep discounts realistic for downstream analytics.
    - Prefer computed discount from prices when possible.
    - Clamp final value to 0..90 (validator requirement).
    """
    computed = compute_discount_pct(original_price, sale_price)
    d = computed if computed is not None else discount_pct
    if d is None:
        return None
    try:
        d = float(d)
    except Exception:
        return None
    if d < 0:
        d = 0.0
    if d > 90:
        d = 90.0
    return round(d, 2)


def _with_page(url: str, page: int) -> str:
    parsed = urlparse(url)
    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    q["page"] = str(page)
    new_query = urlencode(q, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def _clean_text(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    out = " ".join(s.split())
    return out or None


def _extract_from_card(card: BeautifulSoup, logger: logging.Logger) -> Optional[Product]:
    """
    Flipkart markup changes frequently. This function uses multiple selector fallbacks
    and never throws; it returns None if required fields can't be extracted.
    """
    try:
        # Name selectors (common variants)
        name_el = (
            card.select_one("div.RG5Slk")
            or card.select_one("div.KzDlHZ")
            or card.select_one("div._4rR01T")
            or card.select_one("a.IRpwTa")
            or card.select_one("a.s1Q9rs")
        )
        name = _clean_text(name_el.get_text(strip=True) if name_el else None)

        # URL: usually an <a> around the title/image
        link_el = (
            card.select_one("a.k7wcnx")
            or card.select_one("a.CGtC98")
            or card.select_one("a._1fQZEK")
            or card.select_one("a.IRpwTa")
            or card.select_one("a.s1Q9rs")
            or card.select_one("a[href]")
        )
        url = normalize_url(config.BASE_URL, link_el.get("href") if link_el else None)

        # Prices
        sale_el = (
            card.select_one("div.hZ3P6w.DeU9vF")
            or card.select_one("div.Nx9bqj")
            or card.select_one("div._30jeq3")
        )
        orig_el = (
            card.select_one("div.kRYCnD.gxR4EY")
            or card.select_one("div.yRaY8j")
            or card.select_one("div._3I9_wc")
            or card.select_one("div._3fSRat")
        )

        sale_price = parse_inr_price(sale_el.get_text(" ", strip=True) if sale_el else None)
        original_price = parse_inr_price(orig_el.get_text(" ", strip=True) if orig_el else None)

        # Discount can appear as "6% off" alongside price; read the tightest block.
        discount_el = (
            card.select_one("div.oFEPlD")
            or card.select_one("div.QiMO5r")
            or card.select_one("div.UkUFwK span")
            or card.select_one("div._3Ay6Sb span")
            or card.select_one("div.VDgUmc")
        )
        discount_text = _clean_text(discount_el.get_text(" ", strip=True) if discount_el else None)

        discount_pct: Optional[float] = None
        if discount_text and "%" in discount_text:
            # e.g. "23% off"
            m = re.search(r"(\d+(?:\.\d+)?)\s*%", discount_text)
            if m:
                try:
                    discount_pct = float(m.group(1))
                except Exception:
                    discount_pct = None

        discount_pct = _normalize_discount(original_price, sale_price, discount_pct)

        # Some listings show only one price; keep schema consistent (no empty prices).
        if sale_price is not None and original_price is None:
            original_price = sale_price
            discount_pct = _normalize_discount(original_price, sale_price, discount_pct) or 0.0

        if not name or not url:
            return None

        return Product(
            product_name=name,
            original_price=original_price,
            sale_price=sale_price,
            discount_pct=discount_pct,
            url=url,
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("Card parse failed: %s", repr(exc))
        return None


def _extract_products_from_page(html: str, logger: logging.Logger) -> list[Product]:
    # Prefer extracting from embedded state (more stable than CSS selectors).
    state = extract_json_object_after_marker(html, "window.__INITIAL_STATE__ = ")
    if isinstance(state, dict):
        products = _extract_products_from_state(state, logger)
        if products:
            return products

    soup = BeautifulSoup(html, "lxml")

    # Product "cards" can be in several container shapes.
    candidates = soup.select("div.tUxRFH")  # common list card wrapper
    if not candidates:
        candidates = soup.select("div._1AtVbE")  # generic listing blocks
    if not candidates:
        candidates = soup.select("div[data-id]")  # fallback

    products: list[Product] = []
    for card in candidates:
        p = _extract_from_card(card, logger)
        if p:
            products.append(p)
    return products


def _extract_products_from_state(state: dict, logger: logging.Logger) -> list[Product]:
    def walk(obj):
        if isinstance(obj, dict):
            yield obj
            for v in obj.values():
                yield from walk(v)
        elif isinstance(obj, list):
            for it in obj:
                yield from walk(it)

    def to_product(d: dict) -> Optional[Product]:
        # Common Flipkart structures:
        # - { productBaseInfoV1: { title, productUrl, ... }, ... pricing ... }
        # - { productUrl, title/productName, pricing: { finalPrice/mrp/discountPercentage } }
        base = d.get("productBaseInfoV1") if isinstance(d.get("productBaseInfoV1"), dict) else None
        src = base or d

        name = _clean_text(src.get("title") or src.get("productName") or src.get("name"))
        url = normalize_url(config.BASE_URL, src.get("productUrl") or src.get("url"))
        if not name or not url:
            return None

        pricing = d.get("pricing") if isinstance(d.get("pricing"), dict) else src.get("pricing")
        sale_price = None
        original_price = None
        discount_pct = None

        if isinstance(pricing, dict):
            final_price = pricing.get("finalPrice") or pricing.get("finalPriceV2")
            mrp = pricing.get("mrp") or pricing.get("mrpV2")
            if isinstance(final_price, dict):
                sale_price = final_price.get("value") or final_price.get("amount")
            if isinstance(mrp, dict):
                original_price = mrp.get("value") or mrp.get("amount")
            if pricing.get("discountPercentage") is not None:
                try:
                    discount_pct = float(pricing.get("discountPercentage"))
                except Exception:
                    discount_pct = None

        # Ensure ints where possible
        if isinstance(sale_price, str):
            sale_price = parse_inr_price(sale_price)
        if isinstance(original_price, str):
            original_price = parse_inr_price(original_price)

        if sale_price is not None:
            try:
                sale_price = int(sale_price)
            except Exception:
                sale_price = None
        if original_price is not None:
            try:
                original_price = int(original_price)
            except Exception:
                original_price = None

        if discount_pct is None:
            discount_pct = compute_discount_pct(original_price, sale_price)

        return Product(
            product_name=name,
            original_price=original_price,
            sale_price=sale_price,
            discount_pct=discount_pct,
            url=url,
        )

    products: list[Product] = []
    for d in walk(state):
        # quick filter to reduce overhead
        if not isinstance(d, dict):
            continue
        if "productBaseInfoV1" in d or ("productUrl" in d and ("title" in d or "productName" in d)):
            p = to_product(d)
            if p:
                products.append(p)

    if not products:
        logger.debug("State parsed but no product nodes discovered.")
        return []

    # Dedupe by url and return
    as_dicts = unique_by([asdict(p) for p in products], "url")
    return [Product(**p) for p in as_dicts]


def scrape_flipkart_phones() -> list[dict]:
    run_date = today_yyyy_mm_dd()
    log_path = f"{config.LOGS_DIR}/scrape_{run_date}.log"
    logger = build_logger(log_path)

    raw_path = f"{config.RAW_DATA_DIR}/flipkart_{run_date}.json"
    logger.info("Starting scrape. listing_url=%s target_count=%s", config.LISTING_URL, config.TARGET_COUNT)

    session = requests.Session()
    all_products: list[dict] = []

    for page in range(1, config.MAX_PAGES + 1):
        if len(all_products) >= config.TARGET_COUNT:
            break

        page_url = _with_page(config.LISTING_URL, page)
        delay_s = polite_sleep(config.DELAY_RANGE_SECONDS)
        logger.info("Fetching page=%s delay=%.2fs url=%s", page, delay_s, page_url)

        try:
            res = fetch_with_retries(
                session,
                page_url,
                timeout_seconds=config.TIMEOUT_SECONDS,
                max_retries=config.MAX_RETRIES,
                backoff_base_seconds=config.BACKOFF_BASE_SECONDS,
                user_agents=config.USER_AGENTS,
                logger=logger,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Page fetch failed; skipping page=%s url=%s err=%s", page, page_url, repr(exc))
            # If we're being actively blocked with reCAPTCHA, don't hammer the site.
            if "reCAPTCHA" in repr(exc) or "captcha" in repr(exc).lower():
                logger.error("Detected bot-block (captcha). Stopping early to be polite.")
                break
            continue

        page_products = _extract_products_from_page(res.text, logger)
        logger.info("Parsed page=%s products_found=%s", page, len(page_products))

        # Convert to dicts + de-duplicate by url.
        all_products.extend(asdict(p) for p in page_products)
        all_products = unique_by(all_products, "url")

        # If no products found for 2 pages in a row, bail (likely blocked/markup change).
        if page >= 2 and len(page_products) == 0:
            # Small random jitter before deciding
            if random.random() < 0.7:
                logger.warning("No products parsed on page=%s; markup may have changed.", page)

    final = all_products[: config.TARGET_COUNT]
    safe_json_dump(raw_path, final)
    logger.info("Completed scrape. saved=%s count=%s", raw_path, len(final))
    return final


def _amazon_with_page(url: str, page: int) -> str:
    parsed = urlparse(url)
    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    q["page"] = str(page)
    new_query = urlencode(q, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def _extract_amazon_products_from_page(html: str, logger: logging.Logger) -> list[Product]:
    soup = BeautifulSoup(html, "lxml")
    results = soup.find_all("div", attrs={"data-component-type": "s-search-result"})
    products: list[Product] = []

    for r in results:
        try:
            # Name: best-effort from the result title block
            h2 = r.find("h2")
            name = _clean_text(h2.get_text(" ", strip=True) if h2 else None)

            # URL: pick the first plausible product link (prefer /dp/)
            url = None
            for a in r.find_all("a", href=True):
                href = a.get("href") or ""
                if "/dp/" in href and not href.startswith("javascript:"):
                    url = normalize_url(config.AMAZON_BASE_URL, href)
                    break
            if not url:
                link_el = r.find("a", href=True)
                url = normalize_url(config.AMAZON_BASE_URL, link_el.get("href") if link_el else None)

            # Amazon prices
            sale_el = r.select_one("span.a-price span.a-offscreen")
            orig_el = r.select_one("span.a-text-price span.a-offscreen")

            sale_price = parse_inr_price(sale_el.get_text(" ", strip=True) if sale_el else None)
            original_price = parse_inr_price(orig_el.get_text(" ", strip=True) if orig_el else None)

            # Handle "No featured offers available" / missing price blocks.
            if sale_price is None:
                # Some results are sponsored/variants with no featured offer.
                continue

            if original_price is None:
                original_price = sale_price

            # On Amazon, "% text" can be unrelated marketing copy (e.g., "400% Ultra Boom Speaker").
            # Use price-based computation only and clamp.
            discount_pct = _normalize_discount(original_price, sale_price, None)

            if not name or not url:
                continue

            products.append(
                Product(
                    product_name=name,
                    original_price=original_price,
                    sale_price=sale_price,
                    discount_pct=discount_pct,
                    url=url,
                )
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Amazon card parse failed: %s", repr(exc))
            continue

    return products


def _is_amazon_block_page(html: str) -> bool:
    h = (html or "").lower()
    return ("api-services-support@amazon.com" in h) or ("to discuss automated access to amazon data" in h)


def scrape_amazon_mobiles() -> list[dict]:
    run_date = today_yyyy_mm_dd()
    log_path = f"{config.LOGS_DIR}/scrape_{run_date}.log"
    logger = build_logger(log_path)

    raw_path = f"{config.RAW_DATA_DIR}/amazon_{run_date}.json"
    logger.info("Starting Amazon scrape. listing_url=%s target_count=%s", config.AMAZON_LISTING_URL, config.AMAZON_TARGET_COUNT)

    session = requests.Session()
    all_products: list[dict] = []

    for page in range(1, config.MAX_PAGES + 1):
        if len(all_products) >= config.AMAZON_TARGET_COUNT:
            break

        page_url = _amazon_with_page(config.AMAZON_LISTING_URL, page)
        delay_s = polite_sleep(config.DELAY_RANGE_SECONDS)
        logger.info("Fetching Amazon page=%s delay=%.2fs url=%s", page, delay_s, page_url)

        try:
            res = fetch_with_retries(
                session,
                page_url,
                timeout_seconds=config.TIMEOUT_SECONDS,
                max_retries=config.MAX_RETRIES,
                backoff_base_seconds=config.BACKOFF_BASE_SECONDS,
                user_agents=config.USER_AGENTS,
                logger=logger,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Amazon page fetch failed; skipping page=%s url=%s err=%s", page, page_url, repr(exc))
            msg = repr(exc).lower()
            if "api-services-support@amazon.com" in msg or "automated access" in msg:
                logger.error("Amazon returned automated-access block page (503). Stopping Amazon early.")
                break
            if "captcha" in msg or "robot" in msg:
                logger.error("Detected bot-block (captcha/robot). Stopping Amazon early to be polite.")
                break
            continue

        if _is_amazon_block_page(res.text):
            logger.error("Amazon block page detected (automated access). Stopping Amazon early.")
            break

        page_products = _extract_amazon_products_from_page(res.text, logger)
        logger.info("Parsed Amazon page=%s products_found=%s", page, len(page_products))

        all_products.extend(asdict(p) for p in page_products)
        all_products = unique_by(all_products, "url")

        if page >= 2 and len(page_products) == 0:
            if random.random() < 0.7:
                logger.warning("No Amazon products parsed on page=%s; markup may have changed or blocked.", page)

    final = all_products[: config.AMAZON_TARGET_COUNT]
    safe_json_dump(raw_path, final)
    logger.info("Completed Amazon scrape. saved=%s count=%s", raw_path, len(final))
    return final


def scrape_all() -> list[dict]:
    run_date = today_yyyy_mm_dd()
    log_path = f"{config.LOGS_DIR}/scrape_{run_date}.log"
    logger = build_logger(log_path)

    flipkart = scrape_flipkart_phones()
    amazon = scrape_amazon_mobiles()

    combined = unique_by([*flipkart, *amazon], "url")
    combined = combined[: config.TOTAL_TARGET_COUNT]

    combined_path = f"{config.RAW_DATA_DIR}/mobiles_{run_date}.json"
    safe_json_dump(combined_path, combined)
    logger.info("Saved combined output. saved=%s count=%s", combined_path, len(combined))
    return combined


if __name__ == "__main__":
    scrape_all()

