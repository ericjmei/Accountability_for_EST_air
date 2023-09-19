# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 08:46:16 2023

stitches Simple Dispatch subsetted output data from multiple (or singular) regions to necessary groups

@author: emei3
"""

import pandas as pd
import os

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

## Inputs
# raw Simple Dispatch subsetted folder
rel_path_input = "../../Data/Simple Dispatch Outputs/2023-06-23 act/Actual CEMS"
# output folder
rel_path_output = "../../Data/Simple Dispatch Outputs/2023-06-23 act/Actual CEMS"
# beginning of file name for new file
fn_beg = "actual_CEMS"
# groups of regions to sum together
groups_of_regions = [['PJM', 'NYIS', 'ISNE'], ['TVA', 'SOCO', 'AEC']]
# output region name
output_region = ['NYC', 'SE']
# years to run; must be iterable
years = range(2006, 2020)

for i, group_of_regions in enumerate(groups_of_regions):
    output = dict.fromkeys(years) # dictionary to hold all dataframes
    
    # loop through states in group, adding each CEMS output to the dictionary
    os.chdir(base_dname) # change to code directory
    os.chdir(rel_path_input) # change to raw output 
    all_files = os.listdir() # grab all files in the directory
    # loop through and add each subset's data to dictionary
    for region in group_of_regions:
        # loop through each year needed, appending to dictionary
        for year in years:
            fn_end = region+'_'+str(year)+'.csv' # unique file name for particular state year combo
            fn = next((file for file in all_files if file.endswith(fn_end)), None) # retrieve file name
            data_year_raw = pd.read_csv(fn) # obtain data for specific year
            if 'date' in data_year_raw.columns: # unfortunate workaround due to inconsistent naming convention
                data_year_raw.rename(columns={'date': 'datetime'}, inplace=True)
            data_year_raw["datetime"] = pd.to_datetime(data_year_raw["datetime"]) # ensure this row is datetime
            data_year_raw = data_year_raw[data_year_raw["datetime"].dt.year == year] # ensure only 1 year data 
            output[year] = pd.concat([output[year], data_year_raw], axis=0) # add to dictionary
    
    # sum data in all dataframes and write to new file
    os.chdir(base_dname) # change to code directory
    os.chdir(rel_path_output) # change to output path
    for year in years:
        fn = fn_beg+'_'+output_region[i]+'_'+str(year)+'.csv' # new file name
        output[year] = output[year].resample('24H', origin='start_day', on="datetime").sum(numeric_only=True)
        output[year].reset_index(inplace=True) # move date back to column instead of index
        output[year].rename(columns={'datetime': 'date'}, inplace=True) # rename datetime column
        output[year].to_csv(fn, index=False) # write to file