#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import random
from typing import Dict, Any, List, Optional
from urllib.parse import urlencode
import re

import requests


API_ENDPOINT = "https://app.rakuten.co.jp/services/api/Recipe/CategoryRanking/20170426"
CATEGORY_LIST_ENDPOINT = "https://app.rakuten.co.jp/services/api/Recipe/CategoryList/20170426"

def fetch_category_list(
    app_id: str,
    timeout_sec: int = 15,
    max_retries: int = 3,
    sleep_base: float = 0.6,
) -> Optional[Dict[str, Any]]:
    """Fetch Rakuten Recipe CategoryList JSON."""
    params = {"format": "json", "applicationId": app_id}
    url = f"{CATEGORY_LIST_ENDPOINT}?{urlencode(params)}"

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=timeout_sec)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(sleep_base * attempt + random.random() * 0.3)
                continue
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and "error" in data:
                last_err = RuntimeError(f"API error: {data}")
                time.sleep(sleep_base * attempt + random.random() * 0.3)
                continue
            return data
        except Exception as e:
            last_err = e
            time.sleep(sleep_base * attempt + random.random() * 0.3)

    print(f"[WARN] Failed category list: {last_err}")
    return None


def _flatten_categories(cat_json: Dict[str, Any]) -> List[Dict[str, str]]:
    """Return flat list of categories with ids and names.

    The API returns 'large', 'medium', 'small' lists (depending on availability).
    We build a simple 'path' like '大>中>小' when possible.
    """
    result = (cat_json or {}).get("result") or {}
    large = result.get("large") or []
    medium = result.get("medium") or []
    small = result.get("small") or []

    large_by_id = {str(c.get("categoryId")): c for c in large if c.get("categoryId") is not None}
    medium_by_id = {str(c.get("categoryId")): c for c in medium if c.get("categoryId") is not None}

    out: List[Dict[str, str]] = []

    # large
    for c in large:
        cid = c.get("categoryId")
        name = c.get("categoryName")
        if cid is None or not name:
            continue
        out.append({"categoryId": str(cid), "name": str(name), "path": str(name)})

    # medium
    for c in medium:
        cid = c.get("categoryId")
        name = c.get("categoryName")
        parent = c.get("parentCategoryId")  # largeId
        if cid is None or parent is None or not name:
            continue

        parent_name = large_by_id.get(str(parent), {}).get("categoryName")
        path = f"{parent_name}>{name}" if parent_name else str(name)

        api_id = f"{parent}-{cid}"   # ★ここが本体
        out.append({
            "categoryId": api_id,         # ★ランキングに投げるID
            "displayId": str(cid),        # （表示用）
            "name": str(name),
            "path": path
        })

    # small
# small
    for c in small:
        cid = c.get("categoryId")          # smallId
        name = c.get("categoryName")
        parent = c.get("parentCategoryId") # mediumId
        if cid is None or parent is None or not name:
            continue

        med = medium_by_id.get(str(parent), {})
        med_name = med.get("categoryName")
        large_parent = med.get("parentCategoryId")  # largeId
        large_name = large_by_id.get(str(large_parent), {}).get("categoryName") if large_parent is not None else None

        if large_name and med_name:
            path = f"{large_name}>{med_name}>{name}"
        elif med_name:
            path = f"{med_name}>{name}"
        else:
            path = str(name)

        if large_parent is None:
            continue

        api_id = f"{large_parent}-{parent}-{cid}"  # ★ここが本体
        out.append({
            "categoryId": api_id,      # ★ランキングに投げるID
            "displayId": str(cid),
            "name": str(name),
            "path": path
        })


    # Dedup by id (keep first)
    seen = set()
    deduped: List[Dict[str, str]] = []
    for c in out:
        if c["categoryId"] in seen:
            continue
        seen.add(c["categoryId"])
        deduped.append(c)
    return deduped


def suggest_categories(
    app_id: str,
    query: str,
    limit: int = 8,
) -> List[Dict[str, Any]]:
    """Very simple 'natural language' -> category suggestions.

    We score categories by:
    - full substring match bonus
    - token matches (split by spaces and common separators)
    This is intentionally lightweight (no heavy NLP).
    """
    q = (query or "").strip()
    if not q:
        return []

    cat_json = fetch_category_list(app_id)
    if not cat_json:
        return []

    cats = _flatten_categories(cat_json)

    # tokens: split on whitespace + common separators
    tokens = re.split(r"[\s\u3000,、/・]+", q)
    tokens = [t for t in (t.strip().lower() for t in tokens) if t]

    q_low = q.lower()

    scored: List[Dict[str, Any]] = []
    for c in cats:
        text = f"{c.get('name','')} {c.get('path','')}".lower()
        score = 0
        if q_low in text:
            score += 5
        for t in tokens:
            if t and t in text:
                score += 2
        if score > 0:
            scored.append({**c, "score": score})

    scored.sort(key=lambda x: (-int(x.get("score", 0)), len(x.get("path",""))))
    return scored[:limit]


# -----------------------------
# recipeId -> recipe details (HTML JSON-LD fallback)
# -----------------------------
_RECIPE_URL_TEMPLATE = "https://recipe.rakuten.co.jp/recipe/{recipe_id}/"

def fetch_recipe_by_id(
    recipe_id: str,
    timeout_sec: int = 15,
    max_retries: int = 3,
    sleep_base: float = 0.6,
) -> Optional[Dict[str, Any]]:
    """Fetch a single recipe by scraping the public recipe page's JSON-LD.

    Rakuten's official Recipe APIs center on categories/rankings.
    This helper is a pragmatic fallback when you already know recipeId.
    """
    rid = str(recipe_id).strip()
    if not rid.isdigit():
        return None

    url = _RECIPE_URL_TEMPLATE.format(recipe_id=rid)

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=timeout_sec, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(sleep_base * attempt + random.random() * 0.3)
                continue
            r.raise_for_status()
            html = r.text

            # Find JSON-LD blocks
            blocks = re.findall(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html, flags=re.S|re.I)
            for b in blocks:
                b = b.strip()
                if not b:
                    continue
                try:
                    data = json.loads(b)
                except Exception:
                    continue

                # data can be dict or list
                candidates = data if isinstance(data, list) else [data]
                for obj in candidates:
                    if not isinstance(obj, dict):
                        continue
                    t = obj.get("@type") or obj.get("@TYPE")
                    if t == "Recipe" or (isinstance(t, list) and "Recipe" in t):
                        name = obj.get("name") or ""
                        desc = obj.get("description") or ""
                        ingredients = obj.get("recipeIngredient") or []
                        if not isinstance(ingredients, list):
                            ingredients = []
                        image = obj.get("image")
                        if isinstance(image, list) and image:
                            image_url = str(image[0])
                        elif isinstance(image, str):
                            image_url = image
                        else:
                            image_url = None

                        return {
                            "recipeId": int(rid),
                            "title": name,
                            "description": desc,
                            "materials": ingredients,
                            "time": obj.get("totalTime") or "指定なし",
                            "cost": "指定なし",
                            "rank": "999",
                            "pickup": 0,
                            "image": image_url,
                            "url": url,
                            "publishDay": "unknown",
                            "nickname": "unknown",
                            "shop": 0,
                            "sourceCategoryId": "manual",
                        }

            return None
        except Exception as e:
            last_err = e
            time.sleep(sleep_base * attempt + random.random() * 0.3)

    print(f"[WARN] Failed recipeId={rid}: {last_err}")
    return None


def fetch_category_ranking(
    app_id: str,
    category_id: str,
    timeout_sec: int = 15,
    max_retries: int = 3,
    sleep_base: float = 0.6,
) -> Optional[Dict[str, Any]]:
    """
    Fetch Rakuten Recipe CategoryRanking JSON.
    Returns dict on success, None on failure.
    """
    params = {
        "format": "json",
        "applicationId": app_id,
        "categoryId": category_id,
    }
    url = f"{API_ENDPOINT}?{urlencode(params)}"

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=timeout_sec)
            # Rate limit / transient errors
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(sleep_base * attempt + random.random() * 0.3)
                continue

            r.raise_for_status()
            data = r.json()

            # Sometimes API returns error fields; be defensive
            if isinstance(data, dict) and "error" in data:
                last_err = RuntimeError(f"API error: {data}")
                time.sleep(sleep_base * attempt + random.random() * 0.3)
                continue

            return data
        except Exception as e:
            last_err = e
            time.sleep(sleep_base * attempt + random.random() * 0.3)

    print(f"[WARN] Failed categoryId={category_id}: {last_err}")
    return None


def normalize_recipe(item: Dict[str, Any], source_category_id: str) -> Dict[str, Any]:
    """
    Convert API item into your app-friendly shape (minimal + useful fields).
    """
    return {
        "recipeId": item.get("recipeId"),
        "title": item.get("recipeTitle"),
        "description": item.get("recipeDescription"),
        "materials": item.get("recipeMaterial") or [],
        "time": item.get("recipeIndication"),
        "cost": item.get("recipeCost"),
        "rank": item.get("rank"),
        "pickup": item.get("pickup"),
        "image": item.get("foodImageUrl") or item.get("mediumImageUrl") or item.get("smallImageUrl"),
        "url": item.get("recipeUrl"),
        "publishDay": item.get("recipePublishday"),
        "nickname": item.get("nickname"),
        "shop": item.get("shop"),
        "sourceCategoryId": source_category_id,
    }


def build_stock(
    app_id: str,
    category_ids: List[str],
    target_count: int = 200,
    per_request_sleep: float = 0.8,
) -> Dict[str, Any]:
    """
    Collect recipes across multiple categories until target_count reached.
    Deduplicates by recipeId.
    """
    recipes_by_id: Dict[str, Dict[str, Any]] = {}
    stats = {"requestedCategories": 0, "fetchedItems": 0, "deduped": 0}

    # Shuffle to diversify early results
    category_ids = category_ids[:]
    random.shuffle(category_ids)

    for cid in category_ids:
        if len(recipes_by_id) >= target_count:
            break

        data = fetch_category_ranking(app_id, cid)
        stats["requestedCategories"] += 1

        if not data or "result" not in data:
            time.sleep(per_request_sleep)
            continue

        items = data.get("result") or []
        stats["fetchedItems"] += len(items)

        for item in items:
            rid = item.get("recipeId")
            if rid is None:
                continue
            rid_str = str(rid)

            if rid_str in recipes_by_id:
                stats["deduped"] += 1
                continue

            recipes_by_id[rid_str] = normalize_recipe(item, cid)

            if len(recipes_by_id) >= target_count:
                break

        time.sleep(per_request_sleep)

    return {
        "meta": {
            "targetCount": target_count,
            "actualCount": len(recipes_by_id),
            "stats": stats,
            "generatedAtEpoch": int(time.time()),
        },
        "recipes": list(recipes_by_id.values()),
    }


def main():
    app_id = os.getenv("RAKUTEN_APP_ID")
    if not app_id:
        raise SystemExit(
            "環境変数 RAKUTEN_APP_ID が未設定です。\n"
            "例: Windows PowerShell:  $env:RAKUTEN_APP_ID='あなたのID'\n"
            "    mac/Linux:          export RAKUTEN_APP_ID='あなたのID'\n"
        )

    # まずは適当に広めのカテゴリを回す例（必要に応じて増やしてOK）
    # categoryId は "30" のような数値や "10-275-516" のような階層IDもOK
    category_ids = [
        "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
        "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
        # ここに先生が欲しいカテゴリIDを足していくのが堅いです
    ]

    target_count = 200
    output_path = "recipes_stock.json"

    stock = build_stock(
        app_id=app_id,
        category_ids=category_ids,
        target_count=target_count,
        per_request_sleep=0.8,  # 連打しないためのウェイト
    )

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(stock, f, ensure_ascii=False, indent=2)

    print(f"[OK] Saved: {output_path}")
    print(f"     actualCount = {stock['meta']['actualCount']}")
    print(f"     stats       = {stock['meta']['stats']}")


if __name__ == "__main__":
    main()