"""
Narrative & Sector Performance — Dune API Pipeline (query_id 방식)
"""
import os, json, csv, time, requests
from datetime import datetime, timezone

DUNE_API_KEY = os.environ.get("DUNE_API_KEY", "")
BASE_URL = "https://api.dune.com/api/v1"
OUTPUT_DIR = "data/narrative"
HEADERS = {"x-dune-api-key": DUNE_API_KEY}

QUERY_IDS = {
    "narrative_prices": 6798661,
}

SECTORS = {
    "AI": ["FET", "RENDER", "TAO", "NEAR", "WLD"],
    "DePIN": ["HNT", "IOTX", "RNDR", "AR", "FIL"],
    "RWA": ["ONDO", "MKR", "COMP", "SNX", "PENDLE"],
    "Meme": ["DOGE", "SHIB", "PEPE", "WIF", "BONK"],
    "L2": ["ARB", "OP", "STRK", "MATIC", "IMX"],
    "DeFi": ["UNI", "AAVE", "CRV", "LDO", "SUSHI"],
    "GameFi": ["AXS", "SAND", "MANA", "GALA", "ILV"],
    "BTC": ["WBTC"],
}

def fetch_results(query_id, name):
    print(f"\n[{name}] Fetching query {query_id}...")
    for attempt in range(3):
        try:
            resp = requests.get(
                f"{BASE_URL}/query/{query_id}/results",
                headers=HEADERS,
                params={"limit": 50000},
                timeout=30
            )
            if resp.status_code == 200:
                rows = resp.json().get("result", {}).get("rows", [])
                print(f"  ✓ Got {len(rows)} rows")
                return rows
            elif resp.status_code == 429:
                time.sleep(5)
            else:
                print(f"  ✗ Error {resp.status_code}: {resp.text[:200]}")
                return []
        except Exception as e:
            print(f"  ✗ {e}")
            time.sleep(2)
    return []

def process_data(rows):
    # Build token price dict
    token_prices = {}
    for r in rows:
        sym = r.get("symbol", "")
        date = str(r.get("date", ""))[:10]
        price = float(r.get("price", 0))
        if sym not in token_prices:
            token_prices[sym] = {}
        token_prices[sym][date] = price

    all_dates = sorted(set(d for p in token_prices.values() for d in p.keys()))

    # Sector indices (equal-weight, base 100)
    sector_data = {}
    for sec_name, tokens in SECTORS.items():
        daily = []
        base_prices = {}
        for date in all_dates:
            returns = []
            for tok in tokens:
                if tok in token_prices and date in token_prices[tok]:
                    if tok not in base_prices:
                        base_prices[tok] = token_prices[tok][date]
                    if base_prices[tok] > 0:
                        returns.append(token_prices[tok][date] / base_prices[tok])
            if returns:
                daily.append({"date": date, "index": round(sum(returns) / len(returns) * 100, 2)})
        sector_data[sec_name] = daily

    # Individual token 30d returns
    token_returns = []
    for sec_name, tokens in SECTORS.items():
        if sec_name == "BTC": continue
        for tok in tokens:
            if tok in token_prices:
                dates = sorted(token_prices[tok].keys())
                if len(dates) >= 30:
                    end = token_prices[tok][dates[-1]]
                    start = token_prices[tok][dates[-30]]
                    if start > 0:
                        token_returns.append({
                            "token": tok, "sector": sec_name,
                            "return_30d": round((end / start - 1) * 100, 1)
                        })
    token_returns.sort(key=lambda x: x["return_30d"], reverse=True)
    return sector_data, token_returns

def main():
    print("=" * 55)
    print("  Narrative & Sector Performance — Dune Pipeline")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 55)

    if not DUNE_API_KEY:
        print("⚠ DUNE_API_KEY not set!"); return

    rows = fetch_results(QUERY_IDS["narrative_prices"], "narrative_prices")

    if rows:
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Save raw CSV
        fp = os.path.join(OUTPUT_DIR, "token_prices.csv")
        with open(fp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)
        print(f"  ✓ Saved {fp}")

        sector_data, token_returns = process_data(rows)

        now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        js = f"""// Auto-generated — {now}
const SECTOR_DATA = {json.dumps(sector_data, indent=2)};
const TOKEN_RETURNS = {json.dumps(token_returns, indent=2)};
"""
        fp = os.path.join(OUTPUT_DIR, "narrative_data.js")
        with open(fp, "w", encoding="utf-8") as f: f.write(js)
        print(f"\n  ✓ Generated {fp}")

        meta = {"last_updated": datetime.now(timezone.utc).isoformat(),
                "query_ids": QUERY_IDS, "rows": len(rows)}
        with open(os.path.join(OUTPUT_DIR, "meta.json"), "w") as f:
            json.dump(meta, f, indent=2)

    print("\n✅ Pipeline complete!")

if __name__ == "__main__":
    main()
