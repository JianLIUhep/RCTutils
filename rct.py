#!/usr/bin/env python3
"""
rct.py – Fetch run / detector information and write a CSV summary.

Handles missing timestamps in detector-flag *effectivePeriods* by substituting
(firstTfTimestamp, lastTfTimestamp) → (timeTrgStart, timeTrgEnd)
→ (timeO2Start, timeO2End) in that order.
"""

import argparse
import csv
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Union

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ───────────────────────────────────────────────────────────────────────────────
# Configuration of extra per-run columns
# ───────────────────────────────────────────────────────────────────────────────

EXTRA_RUN_FIELDS: List[str] = [
    "timeO2Start", "timeO2End",
    "timeTrgStart", "timeTrgEnd",
    "firstTfTimestamp", "lastTfTimestamp",
    "inelasticInteractionRateAvg",
    "inelasticInteractionRateAtStart",
    "inelasticInteractionRateAtMid",
    "inelasticInteractionRateAtEnd",
    "muInelasticInteractionRate",
]

# Which of those are millisecond timestamps
_MS_TIME_FIELDS = {
    "timeO2Start", "timeO2End",
    "timeTrgStart", "timeTrgEnd",
    "firstTfTimestamp", "lastTfTimestamp",
}

# Ordered fall-back list for missing period timestamps
_FALLBACK_PAIRS = [
    ("firstTfTimestamp", "lastTfTimestamp"),
    ("timeTrgStart", "timeTrgEnd"),
    ("timeO2Start", "timeO2End"),
]

# ───────────────────────────────────────────────────────────────────────────────
# Helper utilities
# ───────────────────────────────────────────────────────────────────────────────

def _ms_to_str(ms: Any) -> str:
    # keep strings (they are already readable); still map None → "N/A"
    if ms is None or ms == "N/A":
        return "N/A"
    if isinstance(ms, str):
        return ms
    if isinstance(ms, (int, float)):
        try:
            return datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
        except (OSError, OverflowError, ValueError):
            return str(ms)
    return str(ms)          # fallback for odd types



def fetch_data_pass_ids(api_base_url: str, token: str) -> Dict[str, int]:
    url = f"{api_base_url}/dataPasses?token={token}"
    r = requests.get(url, verify=False, timeout=30)
    r.raise_for_status()
    return {dp["name"]: dp["id"] for dp in r.json().get("data", [])}


def fetch_runs(api_base_url: str, data_pass_id: int, token: str, *,
               extra_fields: Optional[List[str]] = None,
               convert_time: bool = False) -> List[Dict[str, Any]]:
    url = (f"{api_base_url}/runs?filter[dataPassIds][]={data_pass_id}"
           f"&token={token}")
    r = requests.get(url, verify=False, timeout=60)
    r.raise_for_status()
    runs = r.json().get("data", [])

    extra_fields = extra_fields or []
    for run in runs:
        run["detectors_involved"] = run.get("detectors", "").split(",") \
            if run.get("detectors") else []
        for k in extra_fields:
            if k not in run:
                run[k] = "N/A"
            elif convert_time and k in _MS_TIME_FIELDS:
                run[k] = _ms_to_str(run[k])
    return runs


def fetch_detector_flags(flag_api_url: str, data_pass_id: int,
                         run_number: int, detector_id: int,
                         token: str) -> Union[List[dict], List[str]]:
    url = (f"{flag_api_url}?dataPassId={data_pass_id}"
           f"&runNumber={run_number}&dplDetectorId={detector_id}"
           f"&token={token}")
    r = requests.get(url, verify=False, timeout=30)
    r.raise_for_status()
    flags = r.json().get("data", [])
    return ["Not Available"] if not flags else [
        f for f in flags if f.get("effectivePeriods")
    ]


def _fill_missing_period(frm: Any, to: Any, run: Dict[str, Any]) -> (Any, Any):
    """Replace None timestamps using the ordered fall-back list."""
    if frm is not None and to is not None:
        return frm, to
    for start_key, end_key in _FALLBACK_PAIRS:
        s, e = run.get(start_key), run.get(end_key)
        if s not in (None, "N/A") and e not in (None, "N/A"):
            return s, e
    return frm or "N/A", to or "N/A"


def format_flags(flags: Union[List[dict], List[str]],
                 run: Dict[str, Any], *,
                 convert_time: bool = False) -> str:
    """
    Render flags for one detector as text.

    * When period['from'] or ['to'] == None  → substitute timestamps from the
      run itself using the order: firstTf → timeTrg → timeO2.
    * 'Not Available' / 'Not Present' propagated unchanged.
    """
    if flags in (["Not Available"], ["Not Present"]):
        return flags[0]
    if isinstance(flags, str):
        flags = json.loads(flags)

    out: List[str] = []
    for flag in flags:
        for period in flag["effectivePeriods"]:
            frm, to = _fill_missing_period(period.get("from"),
                                           period.get("to"), run)
            if convert_time:
                frm, to = _ms_to_str(frm), _ms_to_str(to)
            out.append(f"{flag['flagType']['method']} (from: {frm} to: {to})")
    return " | ".join(out)


# ───────────────────────────────────────────────────────────────────────────────
# Main workflow
# ───────────────────────────────────────────────────────────────────────────────


def main(cfg_path: str, convert_time: bool) -> None:
    with open(cfg_path) as f:
        cfg = json.load(f)

    api_base_url = cfg["run_api_url"]
    flag_api_url = cfg["flag_api_url"]
    token = cfg["token"]
    data_pass_def = cfg["dataPassNames"]
    detector_ids = cfg["detector_ids"]        # keep JSON order

    data_pass_ids = fetch_data_pass_ids(api_base_url, token)

    for dp_name, dp_info in data_pass_def.items():
        dp_id = data_pass_ids.get(dp_name)
        if dp_id is None:
            print(f"[warn] No data-pass ID found for “{dp_name}”.")
            continue

        runs = fetch_runs(api_base_url, dp_id, token,
                          extra_fields=EXTRA_RUN_FIELDS,
                          convert_time=convert_time)

        lo, hi = dp_info.get("run_range", [None, None])
        if lo is not None:
            runs = [r for r in runs if r["runNumber"] >= lo]
        if hi is not None:
            runs = [r for r in runs if r["runNumber"] <= hi]

        safe = dp_name.replace(" ", "_").replace("/", "_")
        if lo and hi:
            csv_file = f"Runs_{safe}_{lo}_{hi}.csv"
        elif lo:
            csv_file = f"Runs_{safe}_from_{lo}.csv"
        elif hi:
            csv_file = f"Runs_{safe}_to_{hi}.csv"
        else:
            csv_file = f"Runs_{safe}.csv"

        with open(csv_file, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["Run Number"] + EXTRA_RUN_FIELDS + list(detector_ids.keys()))

            for run in runs:
                row = [run["runNumber"]] + [run[k] for k in EXTRA_RUN_FIELDS]
                for det_name, det_id in detector_ids.items():
                    if det_name not in run["detectors_involved"]:
                        row.append("Not present")
                    else:
                        flags = fetch_detector_flags(
                            flag_api_url, dp_id, run["runNumber"], det_id, token
                        )
                        row.append(format_flags(flags, run, convert_time=convert_time))
                w.writerow(row)

        print(f"[ ok ] {len(runs):4d} runs → {csv_file}")


# ───────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Fetch run / detector information and store it in a CSV."
    )
    ap.add_argument("config_file", help="Path to the JSON configuration file")
    ap.add_argument("--convert-time", action="store_true",
                    help="Render millisecond timestamps as ISO UTC strings")
    args = ap.parse_args()

    main(args.config_file, args.convert_time)

