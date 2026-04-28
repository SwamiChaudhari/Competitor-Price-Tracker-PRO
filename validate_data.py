from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import date
from glob import glob
from typing import Any, Dict, List, Optional, Tuple


def _today() -> str:
    return date.today().isoformat()


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _atomic_write_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _latest_flipkart_raw(default_dir: str = "raw_data") -> Optional[str]:
    candidates = glob(os.path.join(default_dir, "flipkart_*.json"))
    if not candidates:
        return None
    # Filename sort works for ISO date suffix.
    return sorted(candidates)[-1]


def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _validate_prices(item: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    op = item.get("original_price")
    sp = item.get("sale_price")
    if op is None or sp is None:
        return False, "missing original_price or sale_price"
    if not _is_number(op) or not _is_number(sp):
        return False, "original_price or sale_price is not numeric"
    if op <= 0 or sp <= 0:
        return False, "original_price or sale_price is <= 0"
    if sp > op:
        # Not strictly impossible, but typically means extraction error.
        return False, "sale_price > original_price"
    return True, None


def _validate_discount(item: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    d = item.get("discount_pct")
    if d is None:
        return False, "missing discount_pct"
    if not _is_number(d):
        return False, "discount_pct is not numeric"
    if d < 0 or d > 90:
        return False, "discount_pct out of realistic range (0-90)"
    return True, None


@dataclass
class ValidationResult:
    ok: bool
    source_file: str
    product_count: int
    checks: Dict[str, Any]
    issues_sample: List[Dict[str, Any]]


def validate(path: str, *, min_products: int = 50) -> ValidationResult:
    raw = _load_json(path)
    issues: List[Dict[str, Any]] = []

    if not isinstance(raw, list):
        return ValidationResult(
            ok=False,
            source_file=path,
            product_count=0,
            checks={
                "count_50_plus": {"pass": False, "details": f"Top-level JSON must be a list, got {type(raw).__name__}"},
                "no_empty_prices": {"pass": False, "details": "Skipped (invalid JSON shape)"},
                "discounts_realistic": {"pass": False, "details": "Skipped (invalid JSON shape)"},
            },
            issues_sample=[],
        )

    count = len(raw)

    bad_prices = 0
    bad_discounts = 0
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            issues.append({"index": idx, "reason": f"item is not an object (got {type(item).__name__})"})
            continue

        prices_ok, prices_reason = _validate_prices(item)
        if not prices_ok:
            bad_prices += 1
            issues.append(
                {
                    "index": idx,
                    "reason": prices_reason,
                    "product_name": item.get("product_name"),
                    "url": item.get("url"),
                    "original_price": item.get("original_price"),
                    "sale_price": item.get("sale_price"),
                }
            )

        disc_ok, disc_reason = _validate_discount(item)
        if not disc_ok:
            bad_discounts += 1
            issues.append(
                {
                    "index": idx,
                    "reason": disc_reason,
                    "product_name": item.get("product_name"),
                    "url": item.get("url"),
                    "discount_pct": item.get("discount_pct"),
                }
            )

    checks = {
        "count_50_plus": {"pass": count >= min_products, "value": count, "min_required": min_products},
        "no_empty_prices": {"pass": bad_prices == 0, "bad_count": bad_prices},
        "discounts_realistic": {"pass": bad_discounts == 0, "bad_count": bad_discounts, "range": [0, 90]},
    }
    ok = all(v["pass"] for v in checks.values())

    return ValidationResult(
        ok=ok,
        source_file=path,
        product_count=count,
        checks=checks,
        issues_sample=issues[:25],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Flipkart raw JSON output.")
    parser.add_argument(
        "--input",
        "-i",
        default=None,
        help="Path to raw JSON. If omitted, uses latest raw_data/flipkart_*.json",
    )
    parser.add_argument("--min-products", type=int, default=50, help="Minimum required products (default: 50)")
    parser.add_argument(
        "--output",
        "-o",
        default="data_summary.json",
        help="Summary output path (default: data_summary.json)",
    )
    args = parser.parse_args()

    input_path = args.input or _latest_flipkart_raw()
    if not input_path:
        summary = {
            "ok": False,
            "date": _today(),
            "error": "No input provided and no raw_data/flipkart_*.json found.",
        }
        _atomic_write_json(args.output, summary)
        print(f"Validation failed: {summary['error']}")
        return 2

    result = validate(input_path, min_products=args.min_products)
    summary = {
        "ok": result.ok,
        "date": _today(),
        "source_file": result.source_file,
        "product_count": result.product_count,
        "checks": result.checks,
        "issues_sample": result.issues_sample,
    }
    _atomic_write_json(args.output, summary)

    print(f"Summary saved: {args.output}")
    print(f"OK={result.ok} products={result.product_count}")
    for name, info in result.checks.items():
        print(f"- {name}: {info['pass']}")

    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

