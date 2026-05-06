from __future__ import annotations

import argparse
import json
import os
import smtplib
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from email.message import EmailMessage
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st
from streamlit_gsheets import GSheetsConnection


def today_str() -> str:
    return date.today().isoformat()


def yesterday_str(today: str) -> str:
    return (date.fromisoformat(today) - timedelta(days=1)).isoformat()


def money_inr(x: Optional[float]) -> str:
    if x is None:
        return "-"
    try:
        return f"₹{float(x):,.0f}"
    except Exception:
        return "-"


def pct(x: Optional[float]) -> str:
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
    price_change_pct: Optional[float]
    url: Optional[str]
    reason: str


def compute_price_change_pct(yesterday_sale: Optional[float], today_sale: float) -> Optional[float]:
    if yesterday_sale is None:
        return None
    try:
        y = float(yesterday_sale)
        t = float(today_sale)
    except Exception:
        return None
    if y <= 0:
        return None
    return round(((t - y) / y) * 100.0, 2)


@st.cache_data(ttl=60, show_spinner=False)
def load_sheet_df(worksheet: str = "prices") -> pd.DataFrame:
    conn = st.connection("gsheets", type=GSheetsConnection)
    df = conn.read(worksheet=worksheet)
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "product_name",
                "source",
                "scrape_date",
                "mrp",
                "sale_price",
                "discount_pct",
                "price_change_pct",
                "url",
                "alert_status",
            ]
        )

    for col in ["mrp", "sale_price", "discount_pct", "price_change_pct"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "scrape_date" in df.columns:
        df["scrape_date"] = pd.to_datetime(df["scrape_date"], errors="coerce")

    for col in ["product_name", "source", "url", "alert_status"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df


def load_candidates_from_gsheets(today: str, yesterday: str, worksheet: str = "prices") -> List[Dict[str, Any]]:
    df = load_sheet_df(worksheet=worksheet)
    if df.empty or "scrape_date" not in df.columns:
        return []

    today_df = df[df["scrape_date"].dt.date.astype(str) == today].copy()
    if today_df.empty:
        return []

    y_df = df[df["scrape_date"].dt.date.astype(str) == yesterday][
        ["product_name", "source", "sale_price"]
    ].copy()
    y_df = y_df.rename(columns={"sale_price": "yesterday_sale_price"})

    merged = today_df.merge(y_df, on=["product_name", "source"], how="left")
    return merged.to_dict("records")


def build_alerts(
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
        change_pct = compute_price_change_pct(y_sale, sale_price)

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
                yesterday_sale_price=float(y_sale) if y_sale is not None and y_sale != "" else None,
                price_change_pct=change_pct,
                url=(str(r.get("url")).strip() if r.get("url") else None),
                reason=reason,
            )
        )

    alerts.sort(
        key=lambda a: (
            a.price_change_pct if a.price_change_pct is not None else 9999.0,
            -a.discount_pct,
            -a.mrp,
        )
    )
    return alerts


def template_line(a: Alert) -> str:
    if a.price_change_pct is not None and a.price_change_pct < 0:
        dropped = abs(a.price_change_pct)
        return f"🚨 {a.product_name} dropped {dropped:.1f}% to {money_inr(a.sale_price)}"
    return f"🚨 {a.product_name} now {money_inr(a.sale_price)} ({a.discount_pct:.1f}% off)"


def render_html(alerts: List[Alert], *, top_n: int) -> str:
    show = alerts[:top_n]

    rows_html = "\n".join(
        [
            "<tr>"
            f"<td>{i}</td>"
            f"<td>{a.product_name}</td>"
            f"<td>{a.source}</td>"
            f"<td>{money_inr(a.mrp)}</td>"
            f"<td><b>{money_inr(a.sale_price)}</b></td>"
            f"<td>{pct(a.discount_pct)}</td>"
            f"<td>{pct(a.price_change_pct)}</td>"
            f"<td>{(f'<a href=\"{a.url}\">link</a>' if a.url else '-')}</td>"
            "</tr>"
            for i, a in enumerate(show, 1)
        ]
    )

    lead = template_line(show[0]) if show else "No alerts today."

    return f"""<!doctype html>
<html>
  <body style="font-family: Arial, sans-serif;">
    <h2>Price Tracker Alerts ({today_str()})</h2>
    <p>{lead}</p>
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
</html>"""


def load_email_config() -> Dict[str, str]:
    cfg: Dict[str, str] = {}

    try:
        import config as project_config  # type: ignore

        for k in ["GMAIL_SMTP_USER", "GMAIL_APP_PASSWORD", "ALERT_EMAIL_TO", "ALERT_EMAIL_FROM"]:
            v = getattr(project_config, k, None)
            if isinstance(v, str) and v.strip():
                cfg[k] = v.strip()
    except Exception:
        pass

    for k in ["GMAIL_SMTP_USER", "GMAIL_APP_PASSWORD", "ALERT_EMAIL_TO", "ALERT_EMAIL_FROM"]:
        if k not in cfg:
            v = os.environ.get(k, "").strip()
            if v:
                cfg[k] = v

    if "ALERT_EMAIL_FROM" not in cfg and "GMAIL_SMTP_USER" in cfg:
        cfg["ALERT_EMAIL_FROM"] = cfg["GMAIL_SMTP_USER"]

    return cfg


def send_email(*, subject: str, html_body: str, cfg: Dict[str, str]) -> None:
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
    parser = argparse.ArgumentParser(description="Alert system: email HOT deals from Google Sheets")
    parser.add_argument("--worksheet", default="prices", help="Google Sheet worksheet name")
    parser.add_argument("--output", default="alerts.json", help="Fallback JSON path")
    parser.add_argument("--top", type=int, default=5, help="Include top N alerts in HTML table")
    parser.add_argument("--discount-threshold", type=float, default=20.0, help="Alert if discount_pct > this")
    parser.add_argument("--price-drop-threshold", type=float, default=-5.0, help="Alert if day-over-day price_change_pct < this")
    args = parser.parse_args(argv)

    today = today_str()
    yesterday = yesterday_str(today)

    try:
        rows = load_candidates_from_gsheets(today, yesterday, worksheet=args.worksheet)
    except Exception as e:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump({"date": today, "error": repr(e)}, f, ensure_ascii=False, indent=2)
        print(f"[ALERT] Google Sheets read failed ({e!r}). Saved fallback: {args.output}")
        return 2

    alerts = build_alerts(
        rows,
        discount_threshold_pct=args.discount_threshold,
        price_drop_threshold_pct=args.price_drop_threshold,
    )

    print(f"[ALERT] date={today} candidates={len(rows)} alerts={len(alerts)}")
    if alerts:
        print(f"[ALERT] sample: {template_line(alerts[0])}")

    html = render_html(alerts, top_n=args.top)
    subject = f"Price Tracker Alerts ({today}) - {len(alerts)}"

    cfg = load_email_config()
    try:
        send_email(subject=subject, html_body=html, cfg=cfg)
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