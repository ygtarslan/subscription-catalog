
import json
import os
import re
import shutil
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = ROOT / "docs" / "subscriptions-tr.json"
BACKUP_FILE = ROOT / "data" / "subscriptions-tr.last-known-good.json"
CHANGELOG_FILE = ROOT / "data" / "subscriptions-tr.changes.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SubscriptionCatalogBot/1.0)",
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
}

MAX_ALLOWED_CHANGE_RATIO = Decimal("3.0")
MIN_ALLOWED_CHANGE_RATIO = Decimal("0.20")


@dataclass
class PriceUpdate:
    service_id: str
    plan_id: str
    old_price: Decimal
    new_price: Decimal
    source_url: str
    status: str
    detail: str


def load_catalog() -> dict:
    with DATA_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_catalog(catalog: dict) -> None:
    with DATA_FILE.open("w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)
        f.write("\n")


def ensure_parent_dirs() -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    BACKUP_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHANGELOG_FILE.parent.mkdir(parents=True, exist_ok=True)


def backup_catalog() -> None:
    if DATA_FILE.exists():
        shutil.copy2(DATA_FILE, BACKUP_FILE)
    else:
        BACKUP_FILE.write_text("{}", encoding="utf-8")


def save_changelog(changes: list[dict]) -> None:
    with CHANGELOG_FILE.open("w", encoding="utf-8") as f:
        json.dump(changes, f, ensure_ascii=False, indent=2)
        f.write("\n")


def _gha_escape(value: str) -> str:
    return (
        value.replace("%", "%25")
        .replace("\r", "%0D")
        .replace("\n", "%0A")
        .replace(":", "%3A")
        .replace(",", "%2C")
    )


def print_github_annotations(changes: list[PriceUpdate]) -> None:
    for c in changes:
        level = "notice"
        if c.status in {"parse_failed", "rejected"}:
            level = "warning"
        elif c.status == "error":
            level = "error"

        title = _gha_escape(f"{c.service_id}/{c.plan_id} - {c.status}")
        message = _gha_escape(f"{c.old_price} -> {c.new_price} | {c.detail}")
        print(f"::{level} title={title}::{message}")


def write_step_summary(changes: list[PriceUpdate]) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    status_counts = Counter(c.status for c in changes)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    lines = [
        "## Turkey Catalog Refresh Report",
        "",
        f"- Generated at: `{now}`",
        f"- Total checked plans: `{len(changes)}`",
        "",
        "### Status counts",
        "",
        "| Status | Count |",
        "|---|---:|",
    ]
    for status in sorted(status_counts.keys()):
        lines.append(f"| `{status}` | {status_counts[status]} |")

    lines.extend(
        [
            "",
            "### Per-plan results",
            "",
            "| Service | Plan | Status | Old | New | Detail |",
            "|---|---|---|---:|---:|---|",
        ]
    )

    for c in changes:
        detail = c.detail.replace("\n", " ").replace("|", "\\|")
        lines.append(
            f"| `{c.service_id}` | `{c.plan_id}` | `{c.status}` | `{c.old_price}` | `{c.new_price}` | {detail} |"
        )

    with open(summary_path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text


def normalize_price_string(value: str) -> Optional[Decimal]:
    if not value:
        return None

    cleaned = value.replace("₺", "").replace("TL", "").replace("TRY", "")
    cleaned = cleaned.replace("\xa0", " ").strip()
    cleaned = re.sub(r"\s+", "", cleaned)

    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")

    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def build_hint_patterns(plan: dict, price_fetch: dict) -> list[str]:
    hints: list[str] = []

    configured_hint = price_fetch.get("match_hint")
    if isinstance(configured_hint, str) and configured_hint.strip():
        hints.append(configured_hint.strip())
    elif isinstance(configured_hint, list):
        hints.extend([str(h).strip() for h in configured_hint if str(h).strip()])

    kind_aliases = {
        "student": [r"\bstudent\b", r"\böğrenci\b"],
        "family": [r"\bfamily\b", r"\baile\b"],
        "individual": [r"\bindividual\b", r"\bbireysel\b"],
        "duo": [r"\bduo\b", r"\bikili\b"],
        "annual": [r"\bannual\b", r"\byıllık\b"],
    }
    kind = str(plan.get("kind", "")).strip().lower()
    hints.extend(kind_aliases.get(kind, []))
    return hints


def extract_by_regex(html: str, regex_pattern: str, match_hints: list[str]) -> Optional[Decimal]:
    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)

    hint_positions: list[tuple[int, int]] = []
    for pattern in match_hints:
        if not pattern:
            continue
        try:
            hint_regex = re.compile(pattern, re.IGNORECASE)
        except re.error:
            continue
        hint_positions.extend((m.start(), m.end()) for m in hint_regex.finditer(text))

    # If hints are provided but not found, fail closed.
    if match_hints and not hint_positions:
        return None

    price_regex = re.compile(regex_pattern, re.IGNORECASE)
    price_candidates: list[tuple[int, Decimal]] = []
    for match in price_regex.finditer(text):
        raw = match.group(1) if match.groups() else match.group(0)
        value = normalize_price_string(raw)
        if value is not None:
            price_candidates.append((match.start(), value))

    if not price_candidates:
        return None

    if not hint_positions:
        return price_candidates[0][1]

    def distance_to_nearest_hint(price_pos: int) -> int:
        return min(abs(price_pos - start) for start, _ in hint_positions)

    _, best_value = min(price_candidates, key=lambda c: distance_to_nearest_hint(c[0]))
    return best_value


def extract_by_selector(html: str, selector: str, regex_pattern: Optional[str]) -> Optional[Decimal]:
    soup = BeautifulSoup(html, "lxml")
    node = soup.select_one(selector)
    if not node:
        return None

    text = node.get_text(" ", strip=True)
    if regex_pattern:
        match = re.search(regex_pattern, text, flags=re.IGNORECASE)
        if not match:
            return None
        raw = match.group(1) if match.groups() else match.group(0)
        return normalize_price_string(raw)

    return normalize_price_string(text)


def extract_price(plan: dict) -> tuple[Optional[Decimal], str]:
    price_fetch = plan.get("price_fetch", {})
    strategy = price_fetch.get("strategy")
    url = plan.get("source_url")
    html = fetch_html(url)

    if strategy == "regex":
        match_hints = build_hint_patterns(plan, price_fetch)
        value = extract_by_regex(
            html,
            price_fetch.get("regex", r"([0-9]+(?:[.,][0-9]{2})?)\s*(?:TL|₺)"),
            match_hints,
        )
        return value, "regex"

    if strategy == "selector":
        value = extract_by_selector(
            html,
            price_fetch.get("selector", ""),
            price_fetch.get("regex"),
        )
        return value, "selector"

    return None, "unsupported_strategy"


def validate_price_change(old_price: Decimal, new_price: Decimal) -> tuple[bool, str]:
    if new_price <= 0:
        return False, "new price is <= 0"

    if old_price <= 0:
        return True, "old price was <= 0, accepting new price"

    ratio = new_price / old_price
    if ratio > MAX_ALLOWED_CHANGE_RATIO:
        return False, f"price jump too large: {ratio}"
    if ratio < MIN_ALLOWED_CHANGE_RATIO:
        return False, f"price drop too large: {ratio}"

    return True, "ok"


def quantize_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"))


def denominator_for_monthly(plan: dict) -> Decimal:
    billing_months = int(plan.get("billing_period_months", 1) or 1)
    shareable_seats = int(plan.get("shareable_seats", 1) or 1)
    denom = Decimal(max(1, billing_months * shareable_seats))
    return denom


def old_billed_price(plan: dict) -> Decimal:
    billed = plan.get("billed_amount_try")
    if billed is not None:
        return Decimal(str(billed))
    monthly = Decimal(str(plan.get("monthly_equivalent_try", 0)))
    return quantize_money(monthly * denominator_for_monthly(plan))


def refresh_catalog() -> tuple[dict, list[PriceUpdate]]:
    catalog = load_catalog()
    changes: list[PriceUpdate] = []
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    for service in catalog.get("services", []):
        for plan in service.get("plans", []):
            price_fetch = plan.get("price_fetch", {})
            if not price_fetch.get("enabled", False):
                continue

            old_monthly_price = Decimal(str(plan.get("monthly_equivalent_try", 0)))
            old_billed = old_billed_price(plan)

            try:
                new_price, mode = extract_price(plan)
            except Exception as exc:
                price_fetch["last_checked_at"] = now
                price_fetch["last_status"] = f"error:{type(exc).__name__}"
                changes.append(
                    PriceUpdate(
                        service_id=service["id"],
                        plan_id=plan["id"],
                        old_price=old_monthly_price,
                        new_price=old_monthly_price,
                        source_url=plan.get("source_url", ""),
                        status="error",
                        detail=str(exc),
                    )
                )
                continue

            if new_price is None:
                price_fetch["last_checked_at"] = now
                price_fetch["last_status"] = "parse_failed"
                changes.append(
                    PriceUpdate(
                        service_id=service["id"],
                        plan_id=plan["id"],
                        old_price=old_monthly_price,
                        new_price=old_monthly_price,
                        source_url=plan.get("source_url", ""),
                        status="parse_failed",
                        detail=f"strategy={mode}",
                    )
                )
                continue

            # Fetched value is treated as billed amount in source currency/period.
            new_billed = quantize_money(new_price)
            new_monthly = quantize_money(new_billed / denominator_for_monthly(plan))
            is_valid, detail = validate_price_change(old_billed, new_billed)
            price_fetch["last_checked_at"] = now

            if not is_valid:
                price_fetch["last_status"] = "rejected"
                changes.append(
                    PriceUpdate(
                        service_id=service["id"],
                        plan_id=plan["id"],
                        old_price=old_monthly_price,
                        new_price=new_monthly,
                        source_url=plan.get("source_url", ""),
                        status="rejected",
                        detail=detail,
                    )
                )
                continue

            if new_monthly != old_monthly_price:
                plan["monthly_equivalent_try"] = float(new_monthly)
                plan["billed_amount_try"] = float(new_billed)
                price_fetch["last_status"] = "updated"
                changes.append(
                    PriceUpdate(
                        service_id=service["id"],
                        plan_id=plan["id"],
                        old_price=old_monthly_price,
                        new_price=new_monthly,
                        source_url=plan.get("source_url", ""),
                        status="updated",
                        detail=f"price changed (billed {old_billed} -> {new_billed})",
                    )
                )
            else:
                price_fetch["last_checked_at"] = now
                price_fetch["last_status"] = "ok"
                changes.append(
                    PriceUpdate(
                        service_id=service["id"],
                        plan_id=plan["id"],
                        old_price=old_monthly_price,
                        new_price=new_monthly,
                        source_url=plan.get("source_url", ""),
                        status="ok",
                        detail=f"no change (billed {old_billed} -> {new_billed})",
                    )
                )

    catalog["last_updated"] = now
    return catalog, changes


def main() -> None:
    ensure_parent_dirs()
    backup_catalog()
    catalog, changes = refresh_catalog()
    save_catalog(catalog)
    save_changelog(
        [
            {
                "service_id": c.service_id,
                "plan_id": c.plan_id,
                "old_price": str(c.old_price),
                "new_price": str(c.new_price),
                "source_url": c.source_url,
                "status": c.status,
                "detail": c.detail,
            }
            for c in changes
        ]
    )

    print("Refresh complete")
    for c in changes:
        print(f"[{c.status}] {c.service_id}/{c.plan_id}: {c.old_price} -> {c.new_price} ({c.detail})")
    print_github_annotations(changes)
    write_step_summary(changes)


if __name__ == "__main__":
    main()
