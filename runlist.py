import json
import gspread
import datetime
import argparse

from oauth2client.service_account import ServiceAccountCredentials

parser = argparse.ArgumentParser(description='Process ALICE Run3 runlist.')
parser.add_argument('config_file', type=str, help='Configuration file path')
args = parser.parse_args()

def read_config(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def get_detector_column_index(detector, header_row, pass_id):
    return header_row.index(detector) + pass_id + 1

def check_run_quality_2022(run_row, detector_indices, qualities, current_period, period_column_index, allowed_periods, separate_22o_test):
    period_raw = run_row[period_column_index].strip()
    period = ''
    if period_raw == '' and (current_period == "LHC22o_test" or current_period == "LHC22o") and separate_22o_test == "True":
        current_period = "LHC22o"
        period = current_period
    if period_raw.startswith('LHC22'):
        period = period_raw
        current_period = period
    elif period_raw.startswith('22o_test'):
        period = "LHC22o_test"
        current_period = period
    else:
        period = current_period
    #period = run_row[period_column_index].strip() or current_period
    #if allowed_periods and period not in allowed_periods:
    if "LHC22o" in allowed_periods and separate_22o_test == "False":  
        if allowed_periods and current_period not in allowed_periods and current_period != "LHC22o_test" or period_raw == 'BAD':
            return False, period
    else: 
        if allowed_periods and current_period not in allowed_periods or period_raw == 'BAD':
            return False, period
    for detector, quality_flags in qualities.items():
        if run_row[detector_indices[detector]-1].strip() not in quality_flags:
            #print(detector,run_row[detector_indices[detector]-1].strip())
            return False, period
        #print(detector,run_row[detector_indices[detector]-1].strip())
    return True, period

def check_run_quality(run_row, detector_indices, qualities, current_period, period_column_index, allowed_periods):
    period = run_row[period_column_index].strip() or current_period
    if allowed_periods and period not in allowed_periods:
        return False, period

    for detector, quality_flags in qualities.items():
        if run_row[detector_indices[detector]-1].strip() not in quality_flags:
            #print(detector,run_row[detector_indices[detector]-1].strip())
            return False, period
        #print(detector,run_row[detector_indices[detector]-1].strip())
    return True, period

config = read_config(args.config_file)
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name('runlist-5dfcf12a816d.json', scope)
client = gspread.authorize(credentials)

current_date = datetime.datetime.now().strftime("%Y-%m-%d")
for sheet_config in config['sheets']:
    spreadsheet = client.open(sheet_config.get('sheet_name'))
    #print(f'sheet name: {spreadsheet}')
    allowed_periods = sheet_config.get('periods', None)
    worksheet = spreadsheet.worksheet(sheet_config['tab_name'])
    pass_id = int(sheet_config.get('pass_shift', 1))
    pass_name = sheet_config.get('pass_name', None)
    separate_22o_test  =  sheet_config.get('separate_22o_test', False)
 
    for runlist_config in sheet_config['runlists']:
        header_row_period = worksheet.row_values(1)
        header_row = worksheet.row_values(2)
        #print(header_row)
        unique_periods = set()
        default_periods = set()

        period_column_index = header_row_period.index('Period')
        detector_indices = {detector: get_detector_column_index(detector, header_row, pass_id) for detector in runlist_config['detectors'].keys()}
        runlist = []
        current_period = ''
        rows = worksheet.get_all_values()

        for row in rows[3:]:
            if sheet_config.get('sheet_name') == "QC_summary_data_2022":
                is_good_run, current_period = check_run_quality_2022(row, detector_indices, runlist_config['detectors'], current_period, period_column_index, allowed_periods, separate_22o_test)
            else: is_good_run, current_period = check_run_quality(row, detector_indices, runlist_config['detectors'], current_period, period_column_index, allowed_periods)
            default_periods.add(current_period)
            if is_good_run:
                if sheet_config.get('sheet_name') == "QC_summary_data_2022":
                    runlist.append(row[0])
                else: runlist.append(row[3])

        if allowed_periods:
            unique_periods = allowed_periods
            file_name_suffix = '_'.join(allowed_periods)
        else:
            unique_periods = default_periods
            file_name_suffix = sheet_config['tab_name'].replace('/', '_')
            
        with open(f'Runlist_{file_name_suffix}_{pass_name}_{runlist_config["name"]}_{current_date}.txt', 'w') as file:
            file.write(f'# Creation Date: {current_date}, Pass: {pass_name}, Periods: {", ".join(unique_periods)}\n')
            file.write(','.join(runlist))

        print(f'Runlist {runlist_config["name"]} has been generated with {len(runlist)} runs.')

