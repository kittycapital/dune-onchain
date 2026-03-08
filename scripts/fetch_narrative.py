"""
Narrative & Sector Performance — Dune API Pipeline
===================================================
prices.day 테이블에서 섹터별 토큰 가격을 가져와 수익률 계산.
"""

import os, json, csv, time, requests
from datetime import datetime, timezone, timedelta

DUNE_API_KEY = os.environ.get("DUNE_API_KEY", "")
BASE_URL = "https://api.dune.com/api/v1"
OUTPUT_DIR = "data/narrative"
HEADERS = {"x-dune-api-key": DUNE_API_KEY, "Content-Type": "application/json"}

# 섹터별 토큰 정의 (symbol → blockchain, contract_address)
# Dune prices.day 에서 symbol로 조회
SECTORS = {
    "AI": ["FET", "RENDER", "TAO", "NEAR", "WLD"],
    "DePIN": ["HNT", "IOTX", "RNDR", "AR", "FIL"],
    "RWA": ["ONDO", "MKR", "COMP", "SNX", "PENDLE"],
    "Meme": ["DOGE", "SHIB", "PEPE", "WIF", "BONK"],
    "L2": ["ARB", "OP", "STRK", "MATIC", "IMX"],
    "DeFi": ["UNI", "AAVE", "CRV", "LDO", "SUSHI"],
    "GameFi": ["AXS", "SAND", "MANA", "GALA", "ILV"],
    "BTC": ["BTC"]
}

ALL_SYMBOLS = []
for tokens in SECTORS.values():
    ALL_SYMBOLS.extend(tokens)
ALL_SYMBOLS = list(set(ALL_SYMBOLS))

QUERY_PRICES = """
SELECT
  symbol,
  DATE(timestamp) AS date,
  AVG(price) AS price
FROM prices.usd_latest AS latest
JOIN prices.day AS pd
  ON pd.contract_address = latest.contract_address
  AND pd.blockchain = latest.blockchain
WHERE latest.symbol IN ({symbols})
  AND pd.timestamp >= NOW() - INTERVAL '100' DAY
GROUP BY 1, 2
ORDER BY 1, 2
"""

# Simpler approach: query prices.usd directly
QUERY_PRICES_SIMPLE = """
SELECT
  symbol,
  DATE(minute) AS date,
  AVG(price) AS price
FROM prices.usd
WHERE symbol IN ({symbols})
  AND minute >= NOW() - INTERVAL '100' DAY
GROUP BY 1, 2
ORDER BY 1, 2
"""

def make_symbols_str():
    return ", ".join(["'" + s + "'" for s in ALL_SYMBOLS])

def execute_query(sql, name):
    print(f"\n[{name}] Executing query...")
    resp = requests.post(f"{BASE_URL}/query/execute/sql", headers=HEADERS,
        json={"query_sql": sql, "performance": "medium"}, timeout=30)
    if resp.status_code != 200:
        print(f"  ✗ Execute failed: {resp.status_code} {resp.text[:300]}")
        return None
    eid = resp.json().get("execution_id")
    if not eid:
        print(f"  ✗ No execution_id")
        return None
    print(f"  → execution_id: {eid}")
    for attempt in range(60):
        time.sleep(5)
        r = requests.get(f"{BASE_URL}/execution/{eid}/results", headers=HEADERS,
            params={"limit": 50000}, timeout=30)
        if r.status_code != 200: continue
        result = r.json()
        state = result.get("state", "")
        if state == "QUERY_STATE_COMPLETED":
            rows = result.get("result", {}).get("rows", [])
            print(f"  ✓ Got {len(rows)} rows")
            return rows
        elif state == "QUERY_STATE_FAILED":
            print(f"  ✗ Failed: {result.get('error','')}")
            return None
        elif attempt % 4 == 0:
            print(f"  ⏳ {state} (attempt {attempt+1})...")
    print("  ✗ Timeout")
    return None

def process_data(rows):
    """Process raw price rows into sector indices."""
    # Build token price dict: {symbol: {date: price}}
    token_prices = {}
    for r in rows:
        sym = r.get("symbol", "")
        date = str(r.get("date", ""))[:10]
        price = float(r.get("price", 0))
        if sym not in token_prices:
            token_prices[sym] = {}
        token_prices[sym][date] = price

    # Get all dates sorted
    all_dates = set()
    for prices in token_prices.values():
        all_dates.update(prices.keys())
    all_dates = sorted(all_dates)

    # Calculate sector index (equal-weight, normalized to 100)
    sector_data = {}
    for sector_name, tokens in SECTORS.items():
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
                avg_return = sum(returns) / len(returns)
                daily.append({"date": date, "index": round(avg_return * 100, 2)})
        sector_data[sector_name] = daily

    # Individual token returns (30d)
    token_returns = []
    for sector_name, tokens in SECTORS.items():
        if sector_name == "BTC":
            continue
        for tok in tokens:
            if tok in token_prices:
                dates = sorted(token_prices[tok].keys())
                if len(dates) >= 30:
                    end_price = token_prices[tok][dates[-1]]
                    start_price = token_prices[tok][dates[-30]]
                    if start_price > 0:
                        ret = round((end_price / start_price - 1) * 100, 1)
                        token_returns.append({
                            "token": tok, "sector": sector_name, "return_30d": ret
                        })

    token_returns.sort(key=lambda x: x["return_30d"], reverse=True)
    return sector_data, token_returns

def generate_js(sector_data, token_returns):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    js = f"""// Auto-generated by fetch_narrative.py
// Last updated: {now}
const SECTOR_DATA = {json.dumps(sector_data, indent=2)};
const TOKEN_RETURNS = {json.dumps(token_returns, indent=2)};
"""
    fp = os.path.join(OUTPUT_DIR, "narrative_data.js")
    with open(fp, "w", encoding="utf-8") as f:
        f.write(js)
    print(f"\n  ✓ Generated {fp}")

def main():
    print("=" * 55)
    print("  Narrative & Sector Performance — Dune Pipeline")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 55)

    if not DUNE_API_KEY:
        print("\n⚠ DUNE_API_KEY not set! Generating sample data...")
        generate_sample()
        return

    sql = QUERY_PRICES_SIMPLE.format(symbols=make_symbols_str())
    rows = execute_query(sql, "Token Prices")

    if rows:
        sector_data, token_returns = process_data(rows)
        generate_js(sector_data, token_returns)

        # Save raw CSV
        fp = os.path.join(OUTPUT_DIR, "token_prices.csv")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(fp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["symbol", "date", "price"])
            w.writeheader()
            w.writerows(rows)

        meta = {"last_updated": datetime.now(timezone.utc).isoformat(),
                "rows": len(rows), "sectors": list(SECTORS.keys()),
                "tokens": ALL_SYMBOLS}
        with open(os.path.join(OUTPUT_DIR, "meta.json"), "w") as f:
            json.dump(meta, f, indent=2)

    print("\n✅ Pipeline complete!")

def generate_sample():
    import math
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    seed = 99
    def rand():
        nonlocal seed; seed = (seed * 16807) % 2147483647; return (seed - 1) / 2147483646

    sector_data = {}
    for sec_name, tokens in SECTORS.items():
        daily = []
        base = 100
        drift = {"AI": 0.3, "Meme": 0.5, "BTC": 0.15, "DePIN": 0.2, "RWA": 0.1,
                 "L2": -0.05, "DeFi": 0.1, "GameFi": -0.1}.get(sec_name, 0.1)
        vol = {"Meme": 4, "AI": 2.5, "GameFi": 3}.get(sec_name, 1.5)
        for i in range(90):
            d = datetime(2024, 12, 10) + timedelta(days=i)
            base = base * (1 + (rand() - 0.48) * vol / 100 + drift / 100)
            daily.append({"date": d.strftime("%Y-%m-%d"), "index": round(base, 2)})
        sector_data[sec_name] = daily

    token_returns = []
    for sec_name, tokens in SECTORS.items():
        if sec_name == "BTC": continue
        d = sector_data[sec_name]
        sec_ret = (d[-1]["index"] / d[0]["index"] - 1) * 100 if d[0]["index"] > 0 else 0
        for tok in tokens:
            token_returns.append({"token": tok, "sector": sec_name,
                                  "return_30d": round(sec_ret + (rand() - 0.5) * 20, 1)})
    token_returns.sort(key=lambda x: x["return_30d"], reverse=True)

    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    js = f"""// Sample data\n// Generated: {now}
const SECTOR_DATA = {json.dumps(sector_data, indent=2)};
const TOKEN_RETURNS = {json.dumps(token_returns, indent=2)};
"""
    with open(os.path.join(OUTPUT_DIR, "narrative_data.js"), "w") as f:
        f.write(js)
    print("✅ Sample data generated!")

if __name__ == "__main__":
    main()
