
import json
import re
import shutil
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


def extract_by_regex(html: str, regex_pattern: str, match_hint: Optional[str]) -> Optional[Decimal]:
    text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)

    if match_hint:
        hint_regex = re.compile(match_hint, re.IGNORECASE)
        windows = []
        for match in hint_regex.finditer(text):
            start = max(0, match.start() - 250)
            end = min(len(text), match.end() + 250)
            windows.append(text[start:end])
        search_spaces = windows if windows else [text]
    else:
        search_spaces = [text]

    price_regex = re.compile(regex_pattern, re.IGNORECASE)

    for search_space in search_spaces:
        matches = price_regex.findall(search_space)
        for m in matches:
            raw = m[0] if isinstance(m, tuple) else m
            value = normalize_price_string(raw)
            if value is not None:
                return value
    return None


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
        value = extract_by_regex(
            html,
            price_fetch.get("regex", r"([0-9]+(?:[.,][0-9]{2})?)\s*(?:TL|₺)"),
            price_fetch.get("match_hint"),
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


def refresh_catalog() -> tuple[dict, list[PriceUpdate]]:
    catalog = load_catalog()
    changes: list[PriceUpdate] = []
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    for service in catalog.get("services", []):
        for plan in service.get("plans", []):
            price_fetch = plan.get("price_fetch", {})
            if not price_fetch.get("enabled", False):
                continue

            old_price = Decimal(str(plan.get("monthly_equivalent_try", 0)))

            try:
                new_price, mode = extract_price(plan)
            except Exception as exc:
                price_fetch["last_checked_at"] = now
                price_fetch["last_status"] = f"error:{type(exc).__name__}"
                changes.append(
                    PriceUpdate(
                        service_id=service["id"],
                        plan_id=plan["id"],
                        old_price=old_price,
                        new_price=old_price,
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
                        old_price=old_price,
                        new_price=old_price,
                        source_url=plan.get("source_url", ""),
                        status="parse_failed",
                        detail=f"strategy={mode}",
                    )
                )
                continue

            is_valid, detail = validate_price_change(old_price, new_price)
            price_fetch["last_checked_at"] = now

            if not is_valid:
                price_fetch["last_status"] = "rejected"
                changes.append(
                    PriceUpdate(
                        service_id=service["id"],
                        plan_id=plan["id"],
                        old_price=old_price,
                        new_price=new_price,
                        source_url=plan.get("source_url", ""),
                        status="rejected",
                        detail=detail,
                    )
                )
                continue

            if new_price != old_price:
                plan["monthly_equivalent_try"] = float(new_price)
                if plan.get("billing_period_months", 1) == 1:
                    plan["billed_amount_try"] = float(new_price)
                price_fetch["last_status"] = "updated"
                changes.append(
                    PriceUpdate(
                        service_id=service["id"],
                        plan_id=plan["id"],
                        old_price=old_price,
                        new_price=new_price,
                        source_url=plan.get("source_url", ""),
                        status="updated",
                        detail="price changed",
                    )
                )
            else:
                price_fetch["last_checked_at"] = now
                price_fetch["last_status"] = "ok"
                changes.append(
                    PriceUpdate(
                        service_id=service["id"],
                        plan_id=plan["id"],
                        old_price=old_price,
                        new_price=new_price,
                        source_url=plan.get("source_url", ""),
                        status="ok",
                        detail="no change",
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


if __name__ == "__main__":
    main()
