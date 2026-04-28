from __future__ import annotations

import argparse
import json
import os
import smtplib
import sqlite3
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from email.message import EmailMessage
from typing import Any, Dict, List, Optional


def _today() -> str:
    return date.today().isoformat()


def _yesterday(today: str) -> str:
    return (date.fromisoformat(today) - timedelta(days=1)).isoformat()


def _money_inr(x: Optional[float]) -> str:
    if x is None:
        return "-"
    try:
        return f"₹{float(x):,.0f}"
    except Exception:
        return "-"


def _pct(x: Optional[float]) -> str:
    if x is None:
        return "-"
    try:
        return f"{float(x):.2f}%"
    except Exception:
        return "-"


@dataclass(frozen=True)
class Alert:
    product_name: str
    source: str
    scrape_date: str
    mrp: float
    sale_price: float
    discount_pct: float
    yesterday_sale_price: Optional[float]
    price_change_pct: Optional[float]  # negative => dropped (today lower than yesterday)
    url: Optional[str]
    reason: str  # "discount" | "price_drop"


def _compute_price_change_pct(yesterday_sale: Optional[float], today_sale: float) -> Optional[float]:
    if yesterday_sale is None:
        return None
    try:
        y = float(yesterday_sale)
        t = float(today_sale)
    except Exception:
        return None
    if y <= 0:
        return None
    # Negative => price dropped
    return round(((t - y) / y) * 100.0, 2)


def _load_candidates(conn: sqlite3.Connection, *, today: str, yesterday: str) -> List[Dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT
          t.product_name,
          t.source,
          t.scrape_date,
          t.mrp,
          t.sale_price,
          t.discount_pct,
          t.url,
          y.sale_price AS yesterday_sale_price
        FROM prices t
        LEFT JOIN prices y
          ON y.product_name = t.product_name
         AND y.source = t.source
         AND y.scrape_date = ?
        WHERE t.scrape_date = ?
        """.strip(),
        (yesterday, today),
    )
    return [dict(r) for r in cur.fetchall()]


def _build_alerts(
    rows: List[Dict[str, Any]],
    *,
    discount_threshold_pct: float,
    price_drop_threshold_pct: float,
) -> List[Alert]:
    alerts: List[Alert] = []
    for r in rows:
        try:
            discount_pct = float(r.get("discount_pct") or 0.0)
            sale_price = float(r.get("sale_price") or 0.0)
            mrp = float(r.get("mrp") or 0.0)
        except Exception:
            continue

        y_sale = r.get("yesterday_sale_price")
        change_pct = _compute_price_change_pct(y_sale, sale_price)

        reason: Optional[str] = None
        if discount_pct > discount_threshold_pct:
            reason = "discount"
        if change_pct is not None and change_pct < price_drop_threshold_pct:
            reason = "price_drop"

        if reason is None:
            continue

        alerts.append(
            Alert(
                product_name=str(r.get("product_name") or "").strip(),
                source=str(r.get("source") or "").strip(),
                scrape_date=str(r.get("scrape_date") or "").strip(),
                mrp=mrp,
                sale_price=sale_price,
                discount_pct=discount_pct,
                yesterday_sale_price=float(y_sale) if y_sale is not None else None,
                price_change_pct=change_pct,
                url=(str(r.get("url")).strip() if r.get("url") else None),
                reason=reason,
            )
        )

    def sort_key(a: Alert) -> tuple:
        # Prefer biggest day-over-day drops (most negative), then biggest discount.
        change = a.price_change_pct
        change_rank = change if change is not None else 9999.0
        return (change_rank, -a.discount_pct, -a.mrp)

    alerts.sort(key=sort_key)
    return alerts


def _template_line(a: Alert) -> str:
    # Template requested: "🚨 iPhone 15 dropped 6.2% to ₹79,999"
    if a.price_change_pct is not None and a.price_change_pct < 0:
        dropped = abs(a.price_change_pct)
        return f"🚨 {a.product_name} dropped {dropped:.1f}% to {_money_inr(a.sale_price)}"
    return f"🚨 {a.product_name} now {_money_inr(a.sale_price)} ({a.discount_pct:.1f}% off)"


def _console_safe(text: str) -> str:
    """
    Windows terminals may not support emoji/unicode in the active codepage.
    Keep console output safe while preserving the emoji for email.
    """
    # Replace the siren emoji first (common failure point).
    text = text.replace("🚨", "[ALERT]")
    try:
        # Encode to stdout encoding to validate; replace undecodable chars.
        enc = getattr(getattr(__import__("sys"), "stdout"), "encoding", None) or "utf-8"
        return text.encode(enc, errors="replace").decode(enc, errors="replace")
    except Exception:
        return text.encode("ascii", errors="replace").decode("ascii", errors="replace")


def _render_html(alerts: List[Alert], *, top_n: int) -> str:
    show = alerts[:top_n]
    rows_html = "\n".join(
        [
            "<tr>"
            f"<td>{i}</td>"
            f"<td>{a.product_name}</td>"
            f"<td>{a.source}</td>"
            f"<td>{_money_inr(a.mrp)}</td>"
            f"<td><b>{_money_inr(a.sale_price)}</b></td>"
            f"<td>{_pct(a.discount_pct)}</td>"
            f"<td>{_pct(a.price_change_pct)}</td>"
            f"<td>{(f'<a href=\"{a.url}\">link</a>' if a.url else '-')}</td>"
            "</tr>"
            for i, a in enumerate(show, 1)
        ]
    )

    return f"""\
<!doctype html>
<html>
  <body style="font-family: Arial, sans-serif;">
    <h2>Price Tracker Alerts ({_today()})</h2>
    <p>{_template_line(show[0]) if show else "No alerts today."}</p>
    <table border="1" cellpadding="8" cellspacing="0" style="border-collapse: collapse;">
      <thead>
        <tr>
          <th>#</th>
          <th>Product</th>
          <th>Source</th>
          <th>MRP</th>
          <th>Sale</th>
          <th>Discount</th>
          <th>DoD Change</th>
          <th>URL</th>
        </tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </body>
</html>
"""


def _load_email_config() -> Dict[str, str]:
    """
    Reads from config.py if present; falls back to env vars.
    Do NOT hardcode secrets.
    """
    cfg: Dict[str, str] = {}

    try:
        import config as project_config  # type: ignore

        for k in [
            "GMAIL_SMTP_USER",
            "GMAIL_APP_PASSWORD",
            "ALERT_EMAIL_TO",
            "ALERT_EMAIL_FROM",
        ]:
            v = getattr(project_config, k, None)
            if isinstance(v, str) and v.strip():
                cfg[k] = v.strip()
    except Exception:
        pass

    env_map = {
        "GMAIL_SMTP_USER": "GMAIL_SMTP_USER",
        "GMAIL_APP_PASSWORD": "GMAIL_APP_PASSWORD",
        "ALERT_EMAIL_TO": "ALERT_EMAIL_TO",
        "ALERT_EMAIL_FROM": "ALERT_EMAIL_FROM",
    }
    for k, env_k in env_map.items():
        if k not in cfg:
            v = os.environ.get(env_k, "").strip()
            if v:
                cfg[k] = v

    # Sensible defaults
    if "ALERT_EMAIL_FROM" not in cfg and "GMAIL_SMTP_USER" in cfg:
        cfg["ALERT_EMAIL_FROM"] = cfg["GMAIL_SMTP_USER"]

    return cfg


def _send_email(*, subject: str, html_body: str, cfg: Dict[str, str]) -> None:
    missing = [k for k in ["GMAIL_SMTP_USER", "GMAIL_APP_PASSWORD", "ALERT_EMAIL_TO", "ALERT_EMAIL_FROM"] if k not in cfg]
    if missing:
        raise RuntimeError(f"Missing email config keys: {missing}. Add them to config.py or set env vars.")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg["ALERT_EMAIL_FROM"]
    msg["To"] = cfg["ALERT_EMAIL_TO"]
    msg.set_content("Your email client does not support HTML.")
    msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=25) as smtp:
        smtp.login(cfg["GMAIL_SMTP_USER"], cfg["GMAIL_APP_PASSWORD"])
        smtp.send_message(msg)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Alert system: email HOT deals, fallback to alerts.json")
    parser.add_argument("--db", default="prices.db", help="SQLite DB path (default: prices.db)")
    parser.add_argument("--output", default="alerts.json", help="Fallback JSON path (default: alerts.json)")
    parser.add_argument("--top", type=int, default=5, help="Include top N alerts in HTML table (default: 5)")
    parser.add_argument("--discount-threshold", type=float, default=20.0, help="Alert if discount_pct > this (default: 20)")
    parser.add_argument(
        "--price-drop-threshold",
        type=float,
        default=-5.0,
        help="Alert if day-over-day price_change_pct < this (negative means drop). Default: -5",
    )
    args = parser.parse_args(argv)

    today = _today()
    yesterday = _yesterday(today)

    conn = sqlite3.connect(args.db)
    try:
        rows = _load_candidates(conn, today=today, yesterday=yesterday)
    finally:
        conn.close()

    alerts = _build_alerts(
        rows,
        discount_threshold_pct=args.discount_threshold,
        price_drop_threshold_pct=args.price_drop_threshold,
    )

    print(f"[ALERT] date={today} candidates={len(rows)} alerts={len(alerts)}")
    if alerts:
        print(f"[ALERT] sample: {_console_safe(_template_line(alerts[0]))}")

    html = _render_html(alerts, top_n=args.top)
    subject = f"Price Tracker Alerts ({today}) - {len(alerts)}"

    cfg = _load_email_config()
    try:
        _send_email(subject=subject, html_body=html, cfg=cfg)
        print("[ALERT] Email sent successfully.")
        return 0
    except Exception as exc:
        payload = {
            "date": today,
            "yesterday": yesterday,
            "alert_count": len(alerts),
            "alerts_top": [asdict(a) for a in alerts[: args.top]],
            "error": repr(exc),
        }
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"[ALERT] Email failed ({exc!r}). Saved fallback: {args.output}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

