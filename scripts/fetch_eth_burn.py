"""
ETH Burn Dashboard — Dune API Data Pipeline
============================================
Dune API로 직접 SQL 실행 → CSV/JS 저장.
Dune에서 수동으로 쿼리 만들 필요 없음 — API 키만 있으면 됨.

Usage:
  export DUNE_API_KEY=your_key
  python scripts/fetch_eth_burn.py
"""

import os
import json
import csv
import time
import requests
from datetime import datetime, timezone

DUNE_API_KEY = os.environ.get("DUNE_API_KEY", "")
BASE_URL = "https://api.dune.com/api/v1"
OUTPUT_DIR = "data/eth-burn"
HEADERS = {"x-dune-api-key": DUNE_API_KEY, "Content-Type": "application/json"}

# ─── SQL Queries ─────────────────────────────────

QUERY_DAILY_BURN = """
SELECT
  date_trunc('day', time) AS date,
  SUM(base_fee_per_gas * gas_used) / 1e18 AS daily_burn_eth
FROM ethereum.blocks
WHERE time >= now() - interval '400' day
GROUP BY 1
ORDER BY 1
"""

QUERY_CUMULATIVE_BURN = """
SELECT
  d.date,
  SUM(d.daily_burn) OVER (ORDER BY d.date) AS cumulative_burn_eth
FROM (
  SELECT
    date_trunc('day', time) AS date,
    SUM(base_fee_per_gas * gas_used) / 1e18 AS daily_burn
  FROM ethereum.blocks
  WHERE time >= DATE '2021-08-05'
  GROUP BY 1
) d
ORDER BY 1
"""

QUERY_TOP_BURNERS = """
SELECT
  COALESCE(
    n.namespace,
    CAST(t."to" AS VARCHAR)
  ) AS protocol_name,
  SUM(t.gas_used * b.base_fee_per_gas) / 1e18 AS burn_eth
FROM ethereum.transactions t
JOIN ethereum.blocks b
  ON t.block_number = b.number
  AND b.time >= now() - interval '30' day
LEFT JOIN ethereum.contracts c
  ON t."to" = c.address
LEFT JOIN dune.namespaces n
  ON c.namespace = n.namespace
  AND n.blockchain = 'ethereum'
WHERE t.block_time >= now() - interval '30' day
GROUP BY 1
ORDER BY 2 DESC
LIMIT 15
"""

# ─── API Functions ───────────────────────────────

def execute_query(sql, name):
    """Execute SQL on Dune and return results."""
    print(f"\n[{name}] Executing query...")

    # Step 1: Execute
    resp = requests.post(
        f"{BASE_URL}/query/execute/sql",
        headers=HEADERS,
        json={
            "query_sql": sql,
            "performance": "medium"
        },
        timeout=30
    )

    if resp.status_code != 200:
        print(f"  ✗ Execute failed: {resp.status_code} {resp.text[:300]}")
        return None

    data = resp.json()
    execution_id = data.get("execution_id")
    if not execution_id:
        print(f"  ✗ No execution_id: {data}")
        return None

    print(f"  → execution_id: {execution_id}")

    # Step 2: Poll for results
    for attempt in range(60):  # max 5 min
        time.sleep(5)
        resp = requests.get(
            f"{BASE_URL}/execution/{execution_id}/results",
            headers=HEADERS,
            params={"limit": 10000},
            timeout=30
        )

        if resp.status_code != 200:
            print(f"  ✗ Poll error: {resp.status_code}")
            continue

        result = resp.json()
        state = result.get("state", "")

        if state == "QUERY_STATE_COMPLETED":
            rows = result.get("result", {}).get("rows", [])
            print(f"  ✓ Got {len(rows)} rows")
            return rows
        elif state == "QUERY_STATE_FAILED":
            error = result.get("error", "Unknown error")
            print(f"  ✗ Query failed: {error}")
            return None
        else:
            if attempt % 4 == 0:
                print(f"  ⏳ State: {state} (attempt {attempt+1})...")

    print(f"  ✗ Timeout after 60 attempts")
    return None


def save_csv(rows, filename):
    """Save rows to CSV."""
    if not rows:
        return
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)
    fieldnames = list(rows[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✓ Saved {filepath}")


def generate_js(daily_rows, cumulative_rows, burner_rows):
    """Generate JS data file for HTML consumption."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Merge daily + cumulative by date
    cum_map = {}
    if cumulative_rows:
        for r in cumulative_rows:
            d = str(r.get("date", ""))[:10]
            cum_map[d] = r.get("cumulative_burn_eth", 0)

    merged = []
    if daily_rows:
        for r in daily_rows:
            d = str(r.get("date", ""))[:10]
            burn = float(r.get("daily_burn_eth", 0))
            issuance = 1800  # PoS ~1800 ETH/day estimate
            merged.append({
                "date": d,
                "daily_burn_eth": round(burn, 2),
                "daily_issuance_eth": issuance,
                "net_change_eth": round(issuance - burn, 2),
                "cumulative_burn_eth": round(float(cum_map.get(d, 0)), 2),
                "staking_ratio_pct": 28.0  # placeholder — can add beacon query later
            })

    # Burners
    burners = []
    if burner_rows:
        total = sum(float(r.get("burn_eth", 0)) for r in burner_rows)
        for r in burner_rows:
            burn = float(r.get("burn_eth", 0))
            burners.append({
                "protocol_name": r.get("protocol_name", "Unknown"),
                "burn_eth": round(burn, 2),
                "burn_pct": round(burn / total * 100, 1) if total > 0 else 0
            })

    now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    js = f"""// Auto-generated by fetch_eth_burn.py
// Last updated: {now_str}

const ETH_BURN_DATA = {json.dumps(merged, indent=2)};

const ETH_TOP_BURNERS = {json.dumps(burners, indent=2)};
"""

    filepath = os.path.join(OUTPUT_DIR, "eth_burn_data.js")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(js)
    print(f"\n  ✓ Generated {filepath}")


# ─── Main ────────────────────────────────────────

def main():
    print("=" * 55)
    print("  ETH Burn Dashboard — Dune API Pipeline")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 55)

    if not DUNE_API_KEY:
        print("\n⚠ DUNE_API_KEY not set!")
        print("  export DUNE_API_KEY=your_key")
        print("  Generating sample data instead...\n")
        generate_sample()
        return

    # Execute queries
    daily_rows = execute_query(QUERY_DAILY_BURN, "Daily Burn")
    save_csv(daily_rows, "eth_daily_burn.csv")

    cumulative_rows = execute_query(QUERY_CUMULATIVE_BURN, "Cumulative Burn")
    save_csv(cumulative_rows, "eth_cumulative_burn.csv")

    burner_rows = execute_query(QUERY_TOP_BURNERS, "Top Burners")
    save_csv(burner_rows, "eth_top_burners.csv")

    # Generate JS
    generate_js(daily_rows, cumulative_rows, burner_rows)

    # Metadata
    meta = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "rows": {
            "daily": len(daily_rows or []),
            "cumulative": len(cumulative_rows or []),
            "burners": len(burner_rows or [])
        }
    }
    with open(os.path.join(OUTPUT_DIR, "meta.json"), "w") as f:
        json.dump(meta, f, indent=2)

    print("\n✅ Pipeline complete!")


def generate_sample():
    """API 키 없을 때 샘플 데이터 생성"""
    import math
    from datetime import timedelta

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    data = []
    cum = 4200000
    seed = 42

    def rand():
        nonlocal seed
        seed = (seed * 16807) % 2147483647
        return (seed - 1) / 2147483646

    start = datetime(2024, 3, 1)
    for i in range(370):
        date = start + timedelta(days=i)
        if date > datetime(2025, 3, 7):
            break
        burn = max(300, 1200 + math.sin(i * 0.05) * 400 + (rand() - 0.3) * 800 + (rand() * 3000 if rand() > 0.92 else 0))
        iss = 1800 + (rand() - 0.5) * 300
        cum += burn
        data.append({
            "date": date.strftime("%Y-%m-%d"),
            "daily_burn_eth": round(burn, 2),
            "daily_issuance_eth": round(iss, 2),
            "net_change_eth": round(iss - burn, 2),
            "cumulative_burn_eth": round(cum),
            "staking_ratio_pct": round(27.0 + (i / 370) * 1.5 + (rand() - 0.5) * 0.1, 2)
        })

    burners = [
        {"protocol_name": "Uniswap V3", "burn_eth": 18420, "burn_pct": 14.2},
        {"protocol_name": "ETH Transfers", "burn_eth": 15890, "burn_pct": 12.3},
        {"protocol_name": "Tether (USDT)", "burn_eth": 9870, "burn_pct": 7.6},
        {"protocol_name": "Uniswap Universal Router", "burn_eth": 8340, "burn_pct": 6.4},
        {"protocol_name": "1inch", "burn_eth": 5620, "burn_pct": 4.3},
        {"protocol_name": "OpenSea", "burn_eth": 4980, "burn_pct": 3.8},
        {"protocol_name": "MetaMask Swap", "burn_eth": 4210, "burn_pct": 3.2},
        {"protocol_name": "Banana Gun", "burn_eth": 3850, "burn_pct": 3.0},
        {"protocol_name": "Aave V3", "burn_eth": 3120, "burn_pct": 2.4},
        {"protocol_name": "USDC (Circle)", "burn_eth": 2760, "burn_pct": 2.1}
    ]

    now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    js = f"""// Sample data (no API key)
// Generated: {now_str}

const ETH_BURN_DATA = {json.dumps(data, indent=2)};

const ETH_TOP_BURNERS = {json.dumps(burners, indent=2)};
"""
    with open(os.path.join(OUTPUT_DIR, "eth_burn_data.js"), "w") as f:
        f.write(js)

    print("✅ Sample data generated!")
    print(f"  → {OUTPUT_DIR}/eth_burn_data.js")


if __name__ == "__main__":
    main()
