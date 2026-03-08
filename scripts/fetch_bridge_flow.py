"""
Bridge Volume Tracker — Dune API Pipeline (query_id 방식)
"""
import os, json, csv, time, requests
from datetime import datetime, timezone

DUNE_API_KEY = os.environ.get("DUNE_API_KEY", "")
BASE_URL = "https://api.dune.com/api/v1"
OUTPUT_DIR = "data/bridge-flow"
HEADERS = {"x-dune-api-key": DUNE_API_KEY}

QUERY_IDS = {
    "bridge_daily": 6798664,
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

def main():
    print("=" * 55)
    print("  Bridge Volume Tracker — Dune Pipeline")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 55)

    if not DUNE_API_KEY:
        print("⚠ DUNE_API_KEY not set!"); return

    rows = fetch_results(QUERY_IDS["bridge_daily"], "bridge_daily")

    if rows:
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Save raw CSV
        fp = os.path.join(OUTPUT_DIR, "bridge_daily.csv")
        with open(fp, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)
        print(f"  ✓ Saved {fp}")

        # Process: group by date, then by chain
        daily_map = {}
        for r in rows:
            date = str(r.get("date", ""))[:10]
            chain = r.get("chain", "Unknown")
            inflow = float(r.get("inflow_usd", 0))
            outflow = float(r.get("outflow_usd", 0))
            tx = int(r.get("tx_count", 0))

            if date not in daily_map:
                daily_map[date] = {}
            if chain not in daily_map[date]:
                daily_map[date][chain] = {"inflow": 0, "outflow": 0, "count": 0}
            daily_map[date][chain]["inflow"] += inflow
            daily_map[date][chain]["outflow"] += outflow
            daily_map[date][chain]["count"] += tx

        # Convert to list
        daily_out = []
        for date in sorted(daily_map.keys()):
            chains = daily_map[date]
            total_vol = sum(c["inflow"] + c["outflow"] for c in chains.values())
            total_count = sum(c["count"] for c in chains.values())
            chain_data = {}
            for chain, vals in chains.items():
                chain_data[chain] = {
                    "inflow": round(vals["inflow"], 2),
                    "outflow": round(vals["outflow"], 2),
                    "net": round(vals["inflow"] - vals["outflow"], 2),
                    "count": vals["count"]
                }
            daily_out.append({
                "date": date,
                "total_volume": round(total_vol, 2),
                "total_count": total_count,
                "chains": chain_data
            })

        now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        js = f"""// Auto-generated — {now}
const BRIDGE_DAILY_DATA = {json.dumps(daily_out, indent=2)};
"""
        fp = os.path.join(OUTPUT_DIR, "bridge_flow_data.js")
        with open(fp, "w", encoding="utf-8") as f: f.write(js)
        print(f"\n  ✓ Generated {fp}")

        meta = {"last_updated": datetime.now(timezone.utc).isoformat(),
                "query_ids": QUERY_IDS, "rows": len(rows)}
        with open(os.path.join(OUTPUT_DIR, "meta.json"), "w") as f:
            json.dump(meta, f, indent=2)

    print("\n✅ Pipeline complete!")

if __name__ == "__main__":
    main()
