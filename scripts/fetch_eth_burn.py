"""
ETH Burn Dashboard — Dune API Data Pipeline
============================================
GitHub Actions에서 매일 실행하여 Dune 쿼리 결과를 CSV로 저장.
생성된 CSV를 eth-burn.html이 fetch하여 Chart.js로 렌더링.

사용법:
  pip install dune-client requests
  export DUNE_API_KEY=your_key_here
  python fetch_eth_burn.py

Dune 쿼리 셋업 필요:
  1. dune.com에서 아래 4개 쿼리를 생성하고 저장
  2. 각 query_id를 아래 QUERY_IDS에 입력
"""

import os
import json
import csv
import time
import requests
from datetime import datetime, timezone

# ─── Configuration ───────────────────────────────
DUNE_API_KEY = os.environ.get("DUNE_API_KEY", "")
BASE_URL = "https://api.dune.com/api/v1"
OUTPUT_DIR = "data"

# Dune에서 쿼리 생성 후 query_id를 여기에 입력
# 아래는 예시 ID — 실제 쿼리 생성 후 교체 필요
QUERY_IDS = {
    "daily_burn": 0,        # 일별 소각량 + 발행량 + 순 변화
    "cumulative_burn": 0,   # 누적 소각량
    "staking_ratio": 0,     # 스테이킹 비율
    "top_burners": 0,       # TOP 소각 프로토콜
}

# ─── Dune SQL Queries (참고용 — Dune UI에서 저장) ──
QUERIES_SQL = {
    "daily_burn": """
-- 일별 ETH 소각량 + PoS 발행량 + 순 공급 변화
-- Dune에서 저장 후 query_id 사용
SELECT
    date_trunc('day', block_time) AS date,
    SUM(base_fee_per_gas * gas_used) / 1e18 AS daily_burn_eth,
    -- PoS issuance: ~1700 ETH/day (consensus layer)
    -- 정확한 값은 beacon chain 테이블에서
    1800 AS daily_issuance_eth,
    1800 - SUM(base_fee_per_gas * gas_used) / 1e18 AS net_change_eth
FROM ethereum.blocks
WHERE block_time >= DATE_ADD('day', -365, CURRENT_DATE)
GROUP BY 1
ORDER BY 1
""",

    "cumulative_burn": """
-- 누적 소각량 (EIP-1559 시작점부터)
SELECT
    date_trunc('day', block_time) AS date,
    SUM(SUM(base_fee_per_gas * gas_used) / 1e18)
        OVER (ORDER BY date_trunc('day', block_time)) AS cumulative_burn_eth
FROM ethereum.blocks
WHERE block_time >= DATE '2021-08-05'
GROUP BY 1
ORDER BY 1
""",

    "staking_ratio": """
-- ETH 스테이킹 비율 (Beacon Chain deposits)
-- ethereum.beacon 테이블 또는 Dune의 staking 스펠 사용
SELECT
    date,
    total_staked_eth,
    total_supply_eth,
    (total_staked_eth / total_supply_eth * 100) AS staking_ratio_pct
FROM dune.ethereum_staking_daily  -- 실제 테이블명 확인 필요
WHERE date >= DATE_ADD('day', -365, CURRENT_DATE)
ORDER BY date
""",

    "top_burners": """
-- 30일간 가스비 소각 TOP 프로토콜
SELECT
    COALESCE(l.name, CAST(t."to" AS VARCHAR)) AS protocol_name,
    SUM(t.gas_used * b.base_fee_per_gas) / 1e18 AS burn_eth,
    SUM(t.gas_used * b.base_fee_per_gas) / 1e18
        / (SELECT SUM(base_fee_per_gas * gas_used) / 1e18
           FROM ethereum.blocks
           WHERE block_time >= DATE_ADD('day', -30, CURRENT_DATE))
        * 100 AS burn_pct
FROM ethereum.transactions t
JOIN ethereum.blocks b ON t.block_number = b.number
LEFT JOIN labels.contracts l ON t."to" = l.address AND l.blockchain = 'ethereum'
WHERE t.block_time >= DATE_ADD('day', -30, CURRENT_DATE)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 15
"""
}

# ─── Helper Functions ────────────────────────────

def dune_get(endpoint, params=None):
    """Dune API GET request with retry"""
    headers = {"x-dune-api-key": DUNE_API_KEY}
    for attempt in range(3):
        try:
            resp = requests.get(
                f"{BASE_URL}{endpoint}",
                headers=headers,
                params=params,
                timeout=30
            )
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  Error {resp.status_code}: {resp.text[:200]}")
                return None
        except Exception as e:
            print(f"  Request error: {e}")
            time.sleep(2)
    return None


def fetch_query_result(query_id, name):
    """
    Get latest result for a saved query.
    Uses 'get latest result' endpoint to minimize credit usage.
    """
    print(f"[{name}] Fetching query {query_id}...")

    result = dune_get(f"/query/{query_id}/results", params={
        "limit": 5000,
    })

    if not result or "result" not in result:
        print(f"  ⚠ No result for {name}")
        return []

    rows = result["result"].get("rows", [])
    print(f"  ✓ Got {len(rows)} rows")
    return rows


def save_csv(rows, filename, fieldnames=None):
    """Save rows to CSV file"""
    if not rows:
        print(f"  ⚠ No data to save for {filename}")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)

    if not fieldnames:
        fieldnames = list(rows[0].keys())

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    print(f"  ✓ Saved {filepath} ({len(rows)} rows)")


def generate_js_data(all_data):
    """
    CSV 대신 JS 파일로 직접 생성하여 HTML에서 바로 로드.
    GitHub Pages에서 CORS 없이 <script src="data/eth_burn_data.js"> 로 사용.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Merge daily_burn + cumulative_burn + staking data
    # 실제로는 쿼리 구조에 따라 조정 필요
    js_content = f"""// Auto-generated by fetch_eth_burn.py
// Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}

const ETH_BURN_DATA = {json.dumps(all_data.get('daily_burn', []), indent=2, default=str)};

const ETH_CUMULATIVE_DATA = {json.dumps(all_data.get('cumulative_burn', []), indent=2, default=str)};

const ETH_STAKING_DATA = {json.dumps(all_data.get('staking_ratio', []), indent=2, default=str)};

const ETH_TOP_BURNERS = {json.dumps(all_data.get('top_burners', []), indent=2, default=str)};
"""

    filepath = os.path.join(OUTPUT_DIR, "eth_burn_data.js")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(js_content)
    print(f"  ✓ Generated {filepath}")


# ─── Main ────────────────────────────────────────

def main():
    print("=" * 50)
    print("ETH Burn Dashboard — Data Pipeline")
    print(f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 50)

    if not DUNE_API_KEY:
        print("⚠ DUNE_API_KEY not set!")
        print("  Set it: export DUNE_API_KEY=your_key")
        print("  Generating sample data instead...")
        generate_sample_data()
        return

    # Check if query IDs are configured
    unconfigured = [k for k, v in QUERY_IDS.items() if v == 0]
    if unconfigured:
        print(f"⚠ Query IDs not configured: {unconfigured}")
        print("  1. Dune.com에서 위 SQL 쿼리를 생성하고 저장")
        print("  2. QUERY_IDS에 query_id를 입력")
        print("  Generating sample data instead...")
        generate_sample_data()
        return

    all_data = {}

    for name, query_id in QUERY_IDS.items():
        rows = fetch_query_result(query_id, name)
        save_csv(rows, f"eth_{name}.csv")
        all_data[name] = rows
        time.sleep(1)  # Rate limit buffer

    # Generate JS data file for direct HTML consumption
    generate_js_data(all_data)

    # Generate metadata
    meta = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "queries": {k: v for k, v in QUERY_IDS.items()},
        "row_counts": {k: len(v) for k, v in all_data.items()}
    }
    with open(os.path.join(OUTPUT_DIR, "meta.json"), "w") as f:
        json.dump(meta, f, indent=2)

    print("\n✅ Pipeline complete!")


def generate_sample_data():
    """Dune API 키 없을 때 샘플 데이터 생성 (개발/테스트용)"""
    import math
    import random

    random.seed(42)
    print("\nGenerating sample data for development...")

    daily_data = []
    cumulative = 4346218
    start = datetime(2024, 3, 1)

    for i in range(370):
        date = datetime(2024, 3, 1)
        date = start.replace(day=1)
        from datetime import timedelta
        date = start + timedelta(days=i)
        if date > datetime(2025, 3, 7):
            break

        base = 1200 + math.sin(i * 0.05) * 400
        noise = (random.random() - 0.3) * 800
        spike = random.random() * 3000 if random.random() > 0.92 else 0
        burn = max(300, base + noise + spike)
        issuance = 1800 + (random.random() - 0.5) * 300
        cumulative += burn
        staking = 27.0 + (i / 370) * 1.5 + (random.random() - 0.5) * 0.1

        daily_data.append({
            "date": date.strftime("%Y-%m-%d"),
            "daily_burn_eth": round(burn, 2),
            "daily_issuance_eth": round(issuance, 2),
            "net_change_eth": round(issuance - burn, 2),
            "cumulative_burn_eth": round(cumulative),
            "staking_ratio_pct": round(staking, 2),
        })

    top_burners = [
        {"protocol_name": "Uniswap V3", "burn_eth": 18420, "burn_pct": 14.2},
        {"protocol_name": "ETH Transfers", "burn_eth": 15890, "burn_pct": 12.3},
        {"protocol_name": "Tether (USDT)", "burn_eth": 9870, "burn_pct": 7.6},
        {"protocol_name": "Uniswap Universal Router", "burn_eth": 8340, "burn_pct": 6.4},
        {"protocol_name": "1inch", "burn_eth": 5620, "burn_pct": 4.3},
        {"protocol_name": "OpenSea", "burn_eth": 4980, "burn_pct": 3.8},
        {"protocol_name": "MetaMask Swap", "burn_eth": 4210, "burn_pct": 3.2},
        {"protocol_name": "Banana Gun", "burn_eth": 3850, "burn_pct": 3.0},
        {"protocol_name": "Aave V3", "burn_eth": 3120, "burn_pct": 2.4},
        {"protocol_name": "USDC (Circle)", "burn_eth": 2760, "burn_pct": 2.1},
    ]

    all_data = {
        "daily_burn": daily_data,
        "cumulative_burn": [{"date": d["date"], "cumulative_burn_eth": d["cumulative_burn_eth"]} for d in daily_data],
        "staking_ratio": [{"date": d["date"], "staking_ratio_pct": d["staking_ratio_pct"]} for d in daily_data],
        "top_burners": top_burners,
    }

    save_csv(daily_data, "eth_daily_burn.csv")
    save_csv(top_burners, "eth_top_burners.csv")
    generate_js_data(all_data)

    print("\n✅ Sample data generated!")
    print("  → data/eth_daily_burn.csv")
    print("  → data/eth_top_burners.csv")
    print("  → data/eth_burn_data.js")


if __name__ == "__main__":
    main()
