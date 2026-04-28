from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from glob import glob
from typing import Any, Dict, Iterable, List, Optional, Tuple


_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")
_PRICE_RE = re.compile(r"(\d[\d,]*)")


def _today() -> str:
    return date.today().isoformat()


def _yesterday(today: str) -> str:
    y = date.fromisoformat(today) - timedelta(days=1)
    return y.isoformat()


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _infer_source_from_filename(path: str) -> str:
    base = os.path.basename(path).lower()
    # e.g. flipkart_2026-04-27.json -> flipkart
    if "_" in base:
        return base.split("_", 1)[0]
    return os.path.splitext(base)[0]


def _infer_scrape_date_from_filename(path: str) -> str:
    m = _DATE_RE.search(os.path.basename(path))
    return m.group(1) if m else _today()


def _standardize_name(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    s = " ".join(str(name).strip().lower().split())
    return s or None


def _parse_price(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).replace("\xa0", " ").strip()
    if not s:
        return None
    m = _PRICE_RE.search(s.replace("₹", ""))
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except Exception:
        return None


def _compute_price_drop_pct(mrp: Optional[float], sale_price: Optional[float]) -> Optional[float]:
    if mrp is None or sale_price is None:
        return None
    if mrp <= 0:
        return None
    return (mrp - sale_price) / mrp


def _compute_discount_pct(mrp: Optional[float], sale_price: Optional[float]) -> Optional[float]:
    drop = _compute_price_drop_pct(mrp, sale_price)
    if drop is None:
        return None
    return round(drop * 100.0, 2)


def _as_url(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


@dataclass(frozen=True)
class CleanRow:
    product_name: str
    mrp: float
    sale_price: float
    discount_pct: float
    price_change_pct: Optional[float]
    alert_status: Optional[str]
    source: str
    scrape_date: str
    url: Optional[str]


def _iter_raw_items(raw: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                yield item
    elif isinstance(raw, dict):
        # Allow future alternate JSON shapes like {"items":[...]}
        items = raw.get("items")
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    yield item


def _clean_item(
    item: Dict[str, Any],
    *,
    source: str,
    scrape_date: str,
    price_change_pct: Optional[float] = None,
    alert_status: Optional[str] = None,
) -> Optional[CleanRow]:
    name = _standardize_name(item.get("product_name") or item.get("name") or item.get("title"))
    mrp = _parse_price(item.get("mrp") if "mrp" in item else item.get("original_price"))
    sale = _parse_price(item.get("sale_price") if "sale_price" in item else item.get("price"))
    url = _as_url(item.get("url") or item.get("product_url") or item.get("link"))

    if not name or mrp is None or sale is None:
        return None
    if mrp <= 0 or sale <= 0:
        return None

    # Always recompute to ensure consistent formula:
    # price_drop_pct = (mrp - sale) / mrp
    discount_pct = _compute_discount_pct(mrp, sale)
    if discount_pct is None:
        return None

    return CleanRow(
        product_name=name,
        mrp=mrp,
        sale_price=sale,
        discount_pct=discount_pct,
        price_change_pct=price_change_pct,
        alert_status=alert_status,
        source=source,
        scrape_date=scrape_date,
        url=url,
    )


def _connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prices (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          product_name TEXT NOT NULL,
          mrp REAL NOT NULL,
          sale_price REAL NOT NULL,
          discount_pct REAL NOT NULL,
          price_change_pct REAL,
          alert_status TEXT,
          source TEXT NOT NULL,
          scrape_date TEXT NOT NULL,
          url TEXT
        )
        """.strip()
    )
    # Backfill schema upgrades for existing DBs created before these columns existed.
    # (SQLite supports ADD COLUMN but not IF NOT EXISTS for it.)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(prices)").fetchall()}
    if "price_change_pct" not in cols:
        conn.execute("ALTER TABLE prices ADD COLUMN price_change_pct REAL")
    if "alert_status" not in cols:
        conn.execute("ALTER TABLE prices ADD COLUMN alert_status TEXT")

    # DEDUPE requirement: product+date unique.
    # Practically we include source too so multiple competitors can coexist.
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_prices_product_source_date
        ON prices(product_name, source, scrape_date)
        """.strip()
    )


def _insert_rows(conn: sqlite3.Connection, rows: List[CleanRow]) -> Tuple[int, int]:
    """
    Returns (inserted, skipped_duplicates)
    """
    inserted = 0
    skipped = 0
    sql = """
      INSERT OR IGNORE INTO prices
        (product_name, mrp, sale_price, discount_pct, price_change_pct, alert_status, source, scrape_date, url)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """.strip()

    cur = conn.cursor()
    for r in rows:
        cur.execute(
            sql,
            (
                r.product_name,
                r.mrp,
                r.sale_price,
                r.discount_pct,
                r.price_change_pct,
                r.alert_status,
                r.source,
                r.scrape_date,
                r.url,
            ),
        )
        if cur.rowcount == 1:
            inserted += 1
        else:
            skipped += 1
    conn.commit()
    return inserted, skipped


def _top_10_drops_today(conn: sqlite3.Connection, today: str) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT product_name, mrp, sale_price, discount_pct, source, scrape_date, url
        FROM prices
        WHERE scrape_date = ?
        ORDER BY discount_pct DESC, mrp DESC
        LIMIT 10
        """.strip(),
        (today,),
    )
    return list(cur.fetchall())


def _get_yesterday_prices(
    conn: sqlite3.Connection, *, yesterday: str
) -> Dict[Tuple[str, str], float]:
    """
    Map (product_name, source) -> yesterday sale_price
    """
    cur = conn.execute(
        """
        SELECT product_name, source, sale_price
        FROM prices
        WHERE scrape_date = ?
        """.strip(),
        (yesterday,),
    )
    out: Dict[Tuple[str, str], float] = {}
    for product_name, source, sale_price in cur.fetchall():
        try:
            out[(str(product_name), str(source))] = float(sale_price)
        except Exception:
            continue
    return out


def _compute_price_change_pct(yesterday_sale: float, today_sale: float) -> Optional[float]:
    """
    Positive percent means price dropped vs yesterday.
    Example: yesterday=100, today=90 => 10.0
    """
    if yesterday_sale <= 0:
        return None
    return round(((yesterday_sale - today_sale) / yesterday_sale) * 100.0, 2)


def _backfill_alerts_for_today(
    conn: sqlite3.Connection,
    *,
    today: str,
    y_prices: Dict[Tuple[str, str], float],
) -> int:
    """
    Ensure today's rows have price_change_pct + alert_status even if they were inserted earlier
    (and now hit the unique constraint).
    """
    cur = conn.execute(
        """
        SELECT id, product_name, source, sale_price
        FROM prices
        WHERE scrape_date = ?
        """.strip(),
        (today,),
    )
    rows = cur.fetchall()
    updated = 0
    for row_id, product_name, source, sale_price in rows:
        try:
            key = (str(product_name), str(source))
            today_sale = float(sale_price)
        except Exception:
            continue

        y_sale = y_prices.get(key)
        if y_sale is None:
            change_pct = None
            status = "NEW"
        else:
            change_pct = _compute_price_change_pct(y_sale, today_sale)
            status = "OK"
            if change_pct is not None and change_pct > 5.0:
                status = "HOT"

        conn.execute(
            """
            UPDATE prices
            SET price_change_pct = ?, alert_status = ?
            WHERE id = ?
            """.strip(),
            (change_pct, status, int(row_id)),
        )
        updated += 1

    conn.commit()
    return updated


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="ETL pipeline: raw JSON -> cleaned SQLite (prices.db)")
    parser.add_argument("--raw-dir", default="raw_data", help="Directory containing raw JSON files (default: raw_data)")
    parser.add_argument("--db", default="prices.db", help="SQLite DB path (default: prices.db)")
    args = parser.parse_args(argv)

    t0 = time.time()
    raw_glob = os.path.join(args.raw_dir, "*.json")
    raw_paths = sorted(glob(raw_glob))

    metrics: Dict[str, Any] = {
        "raw_dir": args.raw_dir,
        "raw_files_found": len(raw_paths),
        "raw_items_loaded": 0,
        "rows_cleaned": 0,
        "rows_dropped_invalid": 0,
        "rows_deduped_in_memory": 0,
        "rows_inserted": 0,
        "rows_skipped_duplicates_db": 0,
        "errors": 0,
        "db_path": args.db,
    }

    if not raw_paths:
        print(f"[ETL] No raw files found at {raw_glob!r}")
        return 2

    cleaned_rows: List[CleanRow] = []
    for path in raw_paths:
        try:
            raw = _load_json(path)
        except Exception as exc:
            metrics["errors"] += 1
            print(f"[ETL] Failed to read {path!r}: {exc!r}")
            continue

        source = _infer_source_from_filename(path)
        scrape_date = _infer_scrape_date_from_filename(path)

        for item in _iter_raw_items(raw):
            metrics["raw_items_loaded"] += 1
            row = _clean_item(item, source=source, scrape_date=scrape_date)
            if row is None:
                metrics["rows_dropped_invalid"] += 1
                continue
            cleaned_rows.append(row)
            metrics["rows_cleaned"] += 1

    # DEDUPE in memory (product + source + date)
    unique: Dict[Tuple[str, str, str], CleanRow] = {}
    for r in cleaned_rows:
        unique[(r.product_name, r.source, r.scrape_date)] = r
    metrics["rows_deduped_in_memory"] = len(cleaned_rows) - len(unique)
    cleaned_rows = list(unique.values())

    # SAVE to SQLite
    conn = _connect_db(args.db)
    try:
        _ensure_schema(conn)
        # Price change detection: compare today vs yesterday for same product+source.
        today = _today()
        yesterday = _yesterday(today)
        y_prices = _get_yesterday_prices(conn, yesterday=yesterday)

        enriched: List[CleanRow] = []
        hot = 0
        for r in cleaned_rows:
            if r.scrape_date != today:
                enriched.append(r)
                continue
            y_sale = y_prices.get((r.product_name, r.source))
            if y_sale is None:
                enriched.append(
                    CleanRow(
                        product_name=r.product_name,
                        mrp=r.mrp,
                        sale_price=r.sale_price,
                        discount_pct=r.discount_pct,
                        price_change_pct=None,
                        alert_status="NEW",
                        source=r.source,
                        scrape_date=r.scrape_date,
                        url=r.url,
                    )
                )
                continue

            change_pct = _compute_price_change_pct(y_sale, r.sale_price)
            status = "OK"
            if change_pct is not None and change_pct > 5.0:
                status = "HOT"
                hot += 1

            enriched.append(
                CleanRow(
                    product_name=r.product_name,
                    mrp=r.mrp,
                    sale_price=r.sale_price,
                    discount_pct=r.discount_pct,
                    price_change_pct=change_pct,
                    alert_status=status,
                    source=r.source,
                    scrape_date=r.scrape_date,
                    url=r.url,
                )
            )

        cleaned_rows = enriched
        metrics["hot_alerts_today"] = hot

        inserted, skipped = _insert_rows(conn, cleaned_rows)
        metrics["rows_inserted"] = inserted
        metrics["rows_skipped_duplicates_db"] = skipped
        metrics["rows_backfilled_today"] = _backfill_alerts_for_today(conn, today=today, y_prices=y_prices)

        # NEW QUERY: Top 10 price drops today
        top10 = _top_10_drops_today(conn, today)
    finally:
        conn.close()

    # LOG metrics to console
    elapsed_s = round(time.time() - t0, 2)
    metrics["elapsed_s"] = elapsed_s

    print("\n[ETL] Metrics")
    for k in [
        "raw_dir",
        "raw_files_found",
        "raw_items_loaded",
        "rows_cleaned",
        "rows_dropped_invalid",
        "rows_deduped_in_memory",
        "rows_inserted",
        "rows_skipped_duplicates_db",
        "rows_backfilled_today",
        "hot_alerts_today",
        "errors",
        "db_path",
        "elapsed_s",
    ]:
        print(f"- {k}: {metrics[k]}")

    print(f"\n[ETL] Top 10 price drops today ({_today()})")
    if not top10:
        print("(no rows for today)")
    else:
        for i, r in enumerate(top10, 1):
            print(
                f"{i:>2}. {r['discount_pct']:>6.2f}% | mrp={r['mrp']:.0f} sale={r['sale_price']:.0f} | "
                f"{r['source']} | {r['product_name']}"
            )
            if r["url"]:
                print(f"    {r['url']}")

    print("\n[ETL] HOT alerts query")
    print("SELECT * FROM prices WHERE alert_status='HOT'")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

