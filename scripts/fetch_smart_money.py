"""
Smart Money & Whale Tracker — Dune API Pipeline (query_id 방식)
"""
import os, json, csv, time, requests
from datetime import datetime, timezone

DUNE_API_KEY = os.environ.get("DUNE_API_KEY", "")
BASE_URL = "https://api.dune.com/api/v1"
OUTPUT_DIR = "data/smart-money"
HEADERS = {"x-dune-api-key": DUNE_API_KEY}

QUERY_IDS = {
    "whale_daily": 6798653,
    "whale_top_tokens": 6798657,
}

def fetch_results(query_id, name):
    print(f"\n[{name}] Fetching query {query_id}...")
    for attempt in range(3):
        try:
            resp = requests.get(
                f"{BASE_URL}/query/{query_id}/results",
                headers=HEADERS,
                params={"limit": 10000},
                timeout=30
            )
            if resp.status_code == 200:
                rows = resp.json().get("result", {}).get("rows", [])
                print(f"  ✓ Got {len(rows)} rows")
                return rows
            elif resp.status_code == 429:
                print(f"  ⏳ Rate limited, waiting...")
                time.sleep(5)
            else:
                print(f"  ✗ Error {resp.status_code}: {resp.text[:200]}")
                return []
        except Exception as e:
            print(f"  ✗ {e}")
            time.sleep(2)
    return []

def save_csv(rows, filename):
    if not rows: return
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    fp = os.path.join(OUTPUT_DIR, filename)
    with open(fp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)
    print(f"  ✓ Saved {fp}")

def main():
    print("=" * 55)
    print("  Smart Money & Whale Tracker — Dune Pipeline")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 55)

    if not DUNE_API_KEY:
        print("⚠ DUNE_API_KEY not set!"); return

    all_data = {}
    for name, qid in QUERY_IDS.items():
        rows = fetch_results(qid, name)
        save_csv(rows, f"{name}.csv")
        all_data[name] = rows
        time.sleep(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Process daily
    daily = all_data.get("whale_daily", [])
    daily_out = []
    for r in daily:
        daily_out.append({
            "date": str(r.get("date", ""))[:10],
            "whale_count": int(r.get("whale_count", 0)),
            "whale_volume_usd": round(float(r.get("whale_volume_usd", 0)), 2)
        })

    # Process top tokens
    tokens = all_data.get("whale_top_tokens", [])
    total = sum(float(t.get("volume", 0)) for t in tokens)
    tokens_out = []
    for t in tokens:
        v = float(t.get("volume", 0))
        tokens_out.append({
            "token": t.get("token", "?"),
            "volume": round(v, 2),
            "count": int(t.get("tx_count", 0)),
            "pct": round(v / total * 100, 1) if total > 0 else 0
        })

    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    js = f"""// Auto-generated — {now}
const WHALE_DAILY_DATA = {json.dumps(daily_out, indent=2)};
const WHALE_TOP_TOKENS = {json.dumps(tokens_out, indent=2)};
"""
    fp = os.path.join(OUTPUT_DIR, "smart_money_data.js")
    with open(fp, "w", encoding="utf-8") as f: f.write(js)
    print(f"\n  ✓ Generated {fp}")

    meta = {"last_updated": datetime.now(timezone.utc).isoformat(),
            "query_ids": QUERY_IDS,
            "rows": {k: len(v) for k, v in all_data.items()}}
    with open(os.path.join(OUTPUT_DIR, "meta.json"), "w") as f:
        json.dump(meta, f, indent=2)
    print("\n✅ Pipeline complete!")

if __name__ == "__main__":
    main()
