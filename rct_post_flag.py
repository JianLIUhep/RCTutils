#!/usr/bin/env python3
import requests
import json
import argparse
import urllib3
import pandas as pd
import os
import time
from collections import defaultdict

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def load_config(config_file):
    """Load the configuration file."""
    with open(config_file, 'r') as file:
        return json.load(file)


def fetch_data_pass_ids(api_base_url, token):
    """Fetch all data passes and return {name: id}."""
    url = f"{api_base_url}/dataPasses?token={token}"
    response = requests.get(url, verify=False, timeout=30)
    response.raise_for_status()
    data_passes = response.json().get('data', [])
    return {dp['name']: dp['id'] for dp in data_passes}


def fetch_runs(api_base_url, data_pass_id, token):
    """Fetch runs for a given data pass ID."""
    url = f"{api_base_url}/runs?filter[dataPassIds][]={data_pass_id}&token={token}"
    response = requests.get(url, verify=False, timeout=30)
    response.raise_for_status()
    runs = response.json().get('data', [])

    # Extract detectors involved in each run, robust against spaces
    for run in runs:
        dets = run.get('detectors', '')
        run['detectors_involved'] = [d.strip() for d in dets.split(',') if d.strip()]

    return runs


def is_run_excluded(run_number, excluded_runs):
    return run_number in excluded_runs


def read_csv_file(csv_file):
    df = pd.read_csv(csv_file)
    return df.to_dict(orient='records')


def read_interval_file(interval_file):
    """
    Expected format (space separated):
      <run> <tmin> <tmax> [# comment]
    """
    intervals = []
    with open(interval_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            comment = None
            if "#" in line:
                main, comment_part = line.split("#", 1)
                main = main.strip()
                comment = comment_part.strip()
            else:
                main = line

            if not main:
                continue

            parts = main.split()
            if len(parts) < 3:
                continue

            try:
                run = int(parts[0])
                tmin = int(parts[1])
                tmax = int(parts[2])
            except ValueError:
                continue

            intervals.append({"run": run, "tmin": tmin, "tmax": tmax, "comment": comment})

    return intervals


def produce_minutes(csv_data, outputFile, flagTypeIdPass, noDiff):
    n_runs = 0
    n_good_runs = 0
    n_bad_tracking = 0
    n_lim_acc_runs = 0
    n_lim_acc_no_rep_runs = 0
    n_bad_pid_runs = 0
    n_no_det_data_runs = 0
    n_unknown_runs = 0

    runs = []
    good_runs = []
    bad_tracking = []
    lim_acc_runs = []
    lim_acc_no_rep_runs = []
    bad_pid_runs = []
    no_det_data_runs = []
    unknown_runs = []

    f = open(outputFile, "a")
    f.write('\nRuns: ')
    for row in csv_data:
        if row.get('post') != 'ok':
            continue
        run_number = row["run_number"]
        runs.append(run_number)
        n_runs += 1

        val = row[flagTypeIdPass]
        if val == 9:
            n_good_runs += 1
            good_runs.append(run_number)
        if val == 7:
            n_bad_tracking += 1
            bad_tracking.append(run_number)
        if val == 5:
            n_lim_acc_runs += 1
            lim_acc_runs.append(run_number)
        if val == 4:
            n_lim_acc_no_rep_runs += 1
            lim_acc_no_rep_runs.append(run_number)
        if val == 6:
            n_bad_pid_runs += 1
            bad_pid_runs.append(run_number)
        if val == 3:
            n_no_det_data_runs += 1
            no_det_data_runs.append(run_number)
        if val == 14:
            n_unknown_runs += 1
            unknown_runs.append(run_number)

    for i, r in enumerate(runs):
        f.write(str(r).rstrip('.0') + (", " if i != len(runs) - 1 else ".\n"))

    sameQuality = 'The quality was the same in the previous pass.'

    if n_runs == 0:
        f.write("No runs to report.\n\n")
        return

    if n_good_runs == n_runs:
        f.write("All the runs are GOOD.\n\n")
        return
    if n_bad_tracking == n_runs:
        f.write("All the runs have been flagged as Bad tracking. " + (sameQuality if noDiff else "") + "\n\n")
        return
    if n_lim_acc_runs == n_runs:
        f.write("All the runs have been flagged as Limited acceptance (MC reproducible). " + (sameQuality if noDiff else "") + "\n\n")
        return
    if n_lim_acc_no_rep_runs == n_runs:
        f.write("All the runs have been flagged as Limited acceptance (MC Not reproducible). " + (sameQuality if noDiff else "") + "\n\n")
        return
    if n_bad_pid_runs == n_runs:
        f.write("All the runs have been flagged as Bad PID. " + (sameQuality if noDiff else "") + "\n\n")
        return
    if n_no_det_data_runs == n_runs:
        f.write("All the runs have been flagged as No Detector Data.\n\n")
        return
    if n_unknown_runs == n_runs:
        f.write("All the runs have been flagged as Unknown.\n\n")
        return

    def write_list(title, lst, add_same_quality=False):
        if not lst:
            return
        f.write(title)
        for k, rr in enumerate(lst):
            sep = ', ' if k != len(lst) - 1 else '.\n'
            f.write(str(rr) + sep)
        if add_same_quality and noDiff:
            # Append same-quality statement on the same line if desired
            # The original code appends it to the last run line for some categories.
            pass

    if good_runs:
        f.write('GOOD runs: ' + ', '.join(map(str, good_runs)) + '.\n')

    if bad_tracking:
        line = 'Runs flagged as Bad tracking: ' + ', '.join(map(str, bad_tracking)) + '.'
        if noDiff:
            line += ' ' + sameQuality
        f.write(line + '\n')

    if lim_acc_runs:
        line = 'Runs flagged as Limited acceptance (MC reproducible): ' + ', '.join(map(str, lim_acc_runs)) + '.'
        if noDiff:
            line += ' ' + sameQuality
        f.write(line + '\n')

    if lim_acc_no_rep_runs:
        line = 'Runs flagged as Limited acceptance (MC Not reproducible): ' + ', '.join(map(str, lim_acc_no_rep_runs)) + '.'
        if noDiff:
            line += ' ' + sameQuality
        f.write(line + '\n')

    if bad_pid_runs:
        line = 'Runs flagged as Bad PID: ' + ', '.join(map(str, bad_pid_runs)) + '.'
        if noDiff:
            line += ' ' + sameQuality
        f.write(line + '\n')

    if unknown_runs:
        f.write('Runs flagged as Unknown: ' + ', '.join(map(str, unknown_runs)) + '.\n')

    if no_det_data_runs:
        f.write('Runs flagged as No Detector Data: ' + ', '.join(map(str, no_det_data_runs)) + '.\n')

    f.write('\n')


# -----------------------
# CLI
# -----------------------
parser = argparse.ArgumentParser(description="Post a quality control flag.")
parser.add_argument('config', type=str, help='Path to the configuration file')
parser.add_argument('--data_pass', type=str, required=True, help='Data pass name to use')
parser.add_argument('--detector', type=str, required=True, help='Detector name to use')
parser.add_argument('--flagTypeId', type=int, help='Flag type ID to use (non-batch and interval modes)')
parser.add_argument('--comment', type=str, default=None, help='Optional comment (only in non-batch and interval modes)')
parser.add_argument('--min_run', type=int, help='Minimum run number')
parser.add_argument('--max_run', type=int, help='Maximum run number')
parser.add_argument('--excluded_runs', type=int, nargs='*', default=[], help='List of run numbers to exclude')
parser.add_argument('-b', '--batch', type=str, help='Path to CSV file for batch mode')
parser.add_argument('--minutes', type=str, help='Name of the output file containing the minutes')
parser.add_argument('--no_diff', action="store_true",
                    help='Use this option if the non GOOD runs show no difference wrt the previous pass (batch mode only)')
parser.add_argument('--interval_file', type=str,
                    help='Path to interval file (run tmin tmax [# comment]) to post time-resolved flags')

# Throttling options
parser.add_argument('--throttle_n', type=int, default=200,
                    help='Sleep after every N successful posted flags (default: 200). Set to 0 to disable.')
parser.add_argument('--throttle_sleep', type=float, default=5.0,
                    help='Sleep duration in seconds between batches (default: 5.0)')

# Dry-run options
parser.add_argument('--dry_run', action='store_true',
                    help='Do not POST flags. Still loop and apply all filters and produce logging summary.')
parser.add_argument('--dry_run_verbose', action='store_true',
                    help='In dry-run, print one line per flag that would be posted (can be very verbose).')

args = parser.parse_args()

# -----------------------
# Argument compatibility checks
# -----------------------
if args.batch and args.interval_file:
    parser.error('--interval_file cannot be used together with -b/--batch')

if args.batch:
    if args.min_run or args.max_run or args.excluded_runs or args.comment or args.flagTypeId:
        parser.error("--min_run, --max_run, --excluded_runs, --comment, and --flagTypeId cannot be used with -b/--batch")

if args.interval_file:
    if args.minutes:
        parser.error('--minutes can be used only in batch mode, not with --interval_file')
    if args.no_diff:
        parser.error('--no_diff can be used only in batch mode, not with --interval_file')
    if args.flagTypeId is None:
        parser.error('--flagTypeId is required when using --interval_file')

if not args.batch and not args.interval_file:
    if args.minutes:
        parser.error('--minutes can be used only in batch mode')
    if args.no_diff:
        parser.error('--no_diff can be used only in batch mode')

if not args.minutes and args.no_diff:
    parser.error('--no_diff can be used only if --minutes is used')

if args.dry_run and args.throttle_n and args.throttle_n > 0:
    # By default, do not sleep in dry-run. We will log when a sleep would have happened.
    pass


# -----------------------
# Initialization from config and APIs
# -----------------------
config = load_config(args.config)

TOKEN = config['token']
API_BASE_URL = config['run_api_url']
FLAG_API_URL = config['flag_api_url']

data_pass_ids = fetch_data_pass_ids(API_BASE_URL, TOKEN)
data_pass_id = data_pass_ids.get(args.data_pass)
if not data_pass_id:
    print(f"No data pass ID found for {args.data_pass}. Check if your token is still valid; the token validity is 1 week only.")
    raise SystemExit(1)

runs = fetch_runs(API_BASE_URL, data_pass_id, TOKEN)
run_numbers = {run['runNumber'] for run in runs}
run_by_number = {run['runNumber']: run for run in runs}

detector_id = config['detector_ids'].get(args.detector)
if not detector_id:
    print(f"No detector ID found for {args.detector}")
    raise SystemExit(1)


# -----------------------
# Throttling and logging counters
# -----------------------
posted_flags_total = 0
posted_per_run = defaultdict(int)
failed_per_run = defaultdict(int)


def print_run_summary():
    if not posted_per_run and not failed_per_run:
        print("INFO: No flags were processed.")
        return

    print("\n=== Flag processing summary (per run) ===")
    all_runs = sorted(set(list(posted_per_run.keys()) + list(failed_per_run.keys())))
    for r in all_runs:
        ok_n = posted_per_run.get(r, 0)
        fail_n = failed_per_run.get(r, 0)
        print(f"Run {r}: processed={ok_n}, failed={fail_n}, total_attempted={ok_n + fail_n}")
    print(f"TOTAL processed flags: {posted_flags_total}\n")


# -----------------------
# Core POST function (dry-run aware)
# -----------------------
def post_flag(run_number, flagTypeId, comment, from_ts=None, to_ts=None):
    """
    Returns True if:
      - dry-run is enabled (no network POST), or
      - POST succeeded (HTTP 2xx).
    Returns False on POST failures.
    """
    if args.dry_run:
        if args.dry_run_verbose:
            print(f"DRY-RUN would POST: run={run_number} flagTypeId={flagTypeId} from={from_ts} to={to_ts} comment={comment}")
        return True

    data = {
        "from": from_ts,
        "to": to_ts,
        "comment": comment,
        "flagTypeId": flagTypeId,
        "runNumber": run_number,
        "dplDetectorId": detector_id,
        "dataPassId": data_pass_id
    }

    try:
        response = requests.post(
            FLAG_API_URL,
            params={"token": TOKEN},
            json=data,
            verify=False,
            timeout=30
        )
    except requests.RequestException as e:
        print(f"ERROR: POST failed for run {run_number} (from={from_ts}, to={to_ts}), exception: {e}")
        return False

    if not response.ok:
        body_snippet = ""
        try:
            body_snippet = str(response.json())[:300]
        except Exception:
            body_snippet = (response.text or "")[:300]
        print(f"ERROR: POST failed for run {run_number} (from={from_ts}, to={to_ts}), HTTP {response.status_code}: {body_snippet}")
        return False

    return True


def post_flag_throttled(run_number, flagTypeId, comment, from_ts=None, to_ts=None):
    global posted_flags_total

    ok = post_flag(run_number, flagTypeId, comment, from_ts=from_ts, to_ts=to_ts)

    if ok:
        posted_flags_total += 1
        posted_per_run[run_number] += 1

        if args.throttle_n and args.throttle_n > 0 and (posted_flags_total % args.throttle_n == 0):
            if args.dry_run:
                print(f"INFO: Processed {posted_flags_total} flags total (dry-run). Would sleep {args.throttle_sleep} s here.")
            else:
                print(f"INFO: Posted {posted_flags_total} flags total. Sleeping {args.throttle_sleep} s...")
                time.sleep(args.throttle_sleep)
    else:
        failed_per_run[run_number] += 1

    return ok


# -----------------------
# Interval mode
# -----------------------
if args.interval_file:
    intervals = read_interval_file(args.interval_file)

    for entry in intervals:
        run_number = entry["run"]
        tmin = entry["tmin"]
        tmax = entry["tmax"]
        comment_from_file = entry["comment"]

        if run_number not in run_numbers:
            print(f"Warning: Run {run_number} from interval file is not in data pass {args.data_pass}, skipping.")
            continue

        if args.min_run is not None and run_number < args.min_run:
            continue
        if args.max_run is not None and run_number > args.max_run:
            continue
        if is_run_excluded(run_number, args.excluded_runs):
            continue

        run_info = run_by_number[run_number]
        if args.detector not in run_info['detectors_involved']:
            print(f"Warning: Detector {args.detector} not in run {run_number}, skipping.")
            continue

        if tmin == tmax:
            tmax = tmin + 1

        if args.comment and comment_from_file:
            comment = f"{args.comment} | {comment_from_file}"
        elif args.comment:
            comment = args.comment
        elif comment_from_file:
            comment = comment_from_file
        else:
            comment = "Interval flag"

        post_flag_throttled(run_number, args.flagTypeId, comment, from_ts=tmin, to_ts=tmax)

    print_run_summary()
    raise SystemExit(0)


# -----------------------
# Batch mode
# -----------------------
if args.batch:
    csv_data = read_csv_file(args.batch)
    for row in csv_data:
        if row.get("post") != 'ok':
            continue

        run_number = row.get("run_number")
        if run_number not in run_numbers:
            print(f"Error: Run number {run_number} not found in data pass {args.data_pass}.")
            continue

        if 'comment' not in row or pd.isna(row['comment']):
            comment = " "
        else:
            comment = row['comment']

        post_flag_throttled(run_number, row[args.data_pass], comment)

    if args.minutes:
        outputFile = args.minutes
        with open(outputFile, "a" if os.path.isfile(outputFile) else "w") as f:
            f.write(args.data_pass)
        produce_minutes(csv_data, outputFile, args.data_pass, args.no_diff)

    print_run_summary()
    raise SystemExit(0)


# -----------------------
# Non-batch mode (run-global flags)
# -----------------------
for run in runs:
    run_number = run['runNumber']

    if is_run_excluded(run_number, args.excluded_runs):
        continue

    if (args.min_run is not None and run_number < args.min_run) or \
       (args.max_run is not None and run_number > args.max_run):
        continue

    if args.detector not in run['detectors_involved']:
        continue

    post_flag_throttled(run_number, args.flagTypeId, args.comment)

print_run_summary()
