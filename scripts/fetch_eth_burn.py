"""
ETH Burn Dashboard — Dune API Pipeline (query_id 방식)
"""
import os, json, csv, time, requests
from datetime import datetime, timezone

DUNE_API_KEY = os.environ.get("DUNE_API_KEY", "")
BASE_URL = "https://api.dune.com/api/v1"
OUTPUT_DIR = "data/eth-burn"
HEADERS = {"x-dune-api-key": DUNE_API_KEY}

QUERY_IDS = {
    "daily_burn": 6798639,
    "top_burners": 6798644,
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
    print("  ETH Burn Dashboard — Dune Pipeline")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 55)

    if not DUNE_API_KEY:
        print("⚠ DUNE_API_KEY not set!"); return

    all_data = {}
    for name, qid in QUERY_IDS.items():
        rows = fetch_results(qid, name)
        save_csv(rows, f"eth_{name}.csv")
        all_data[name] = rows
        time.sleep(1)

    # Generate JS
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    daily = all_data.get("daily_burn", [])
    burners = all_data.get("top_burners", [])

    # Process daily
    daily_out = []
    cumulative = 0
    for r in daily:
        burn = float(r.get("daily_burn_eth", 0))
        cumulative += burn
        daily_out.append({
            "date": str(r.get("date", ""))[:10],
            "daily_burn_eth": round(burn, 2),
            "daily_issuance_eth": 1800,
            "net_change_eth": round(1800 - burn, 2),
            "cumulative_burn_eth": round(cumulative, 2),
            "staking_ratio_pct": 28.0
        })

    # Address → name mapping for top burners
    KNOWN = {
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
        "0xdac17f958d2ee523a2206206994597c13d831ec7": "Tether (USDT)",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH",
        "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "Uniswap V2 Router",
        "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad": "Uniswap Universal Router",
        "0x881d40237659c251811cec9c364ef91dc08d300c": "MetaMask Swap",
        "0x00000000000068f116a894984e2db1123eb395": "Seaport (OpenSea)",
        "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
    }
    burners_out = []
    total_burn = sum(float(r.get("burn_eth", 0)) for r in burners)
    for r in burners:
        addr = str(r.get("protocol_name", "")).lower()
        name_label = KNOWN.get(addr, addr[:10] + "...")
        burn = float(r.get("burn_eth", 0))
        burners_out.append({
            "protocol_name": name_label,
            "burn_eth": round(burn, 2),
            "burn_pct": round(burn / total_burn * 100, 1) if total_burn > 0 else 0
        })

    now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    js = f"""// Auto-generated — {now}
const ETH_BURN_DATA = {json.dumps(daily_out, indent=2)};
const ETH_TOP_BURNERS = {json.dumps(burners_out, indent=2)};
"""
    fp = os.path.join(OUTPUT_DIR, "eth_burn_data.js")
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
