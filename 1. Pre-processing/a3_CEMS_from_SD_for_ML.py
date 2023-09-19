# -*- coding: utf-8 -*-
"""
Created on Fri May  5 19:49:50 2023

stitches CEMS historical dispatch files from simple dispatch that come from multiple years for ML model inputs

@author: emei3
"""

import pandas as pd
import os

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

def retrieve_emissions_and_stitch(group_of_states, years, rel_path_input):
    """
    retrieves emissions files for file names with "group_of_states_year.csv" from 'rel_path_input' folder
    and stitches them all together for all years specified

    Parameters
    ----------
    group_of_states : (array-like)
        unique end of file name prior to year.
    years : (tuple)
        all years to stitch together.
    rel_path_input : (string)
        folder that contains all files.

    Returns
    -------
    output_emissions : (DataFrame)
        stitched emissions

    """
    output_emissions = pd.DataFrame() # instantiate emisisons
    os.chdir(base_dname) # change to code directory
    os.chdir(rel_path_input) # change to input folder
    all_files = os.listdir() # grab all files in the directory
    
    # loop through and add each subset's data to dataframe
    for year in years:
        fn_end = '_'.join(group_of_states)+'_'+str(year)+'.csv' # unique file name for particular state year combo
        fn = next((file for file in all_files if file.endswith(fn_end)), None) # retrieve file name
        emissions_raw = pd.read_csv(fn) # obtain data for specific year
        if any("datetime" in s for s in emissions_raw.columns): # rename date column if it's not called "date
            emissions_raw["datetime"] = pd.to_datetime(emissions_raw["datetime"]) # ensure this row is datetime
            emissions_raw = emissions_raw.resample('24H', origin='start_day', on="datetime").sum(numeric_only=True) # sum to 24h
            emissions_raw.reset_index(inplace=True) # move date back to column instead of index
            emissions_raw.rename(columns={'datetime': 'date'}, inplace=True)
        emissions_raw["date"] = pd.to_datetime(emissions_raw["date"]) # ensure this row is datetime
        emissions_raw = emissions_raw[emissions_raw["date"].dt.year == year] # ensure only 1 year data 
        output_emissions = pd.concat([output_emissions, emissions_raw])
        
    return output_emissions

if __name__ == '__main__':
    # relative file paths
    rel_path_CEMS = "../../Data/Simple Dispatch Outputs/2023-06-23 act/Actual CEMS" # folder with observed emissions
    rel_path_ML = "../../Data/FromZiqi" # folder with ML pre-processed data
    rel_path_output = rel_path_CEMS
    
    # years to run; must be iterable
    years = range(2006, 2020)
    # filename endings to run the analysis for
    fn_ends = [['SOCO'], # ATL regional
               ['NYC']] # NYC regional
    # ML file names
    fn_ML = ['Atlanta.xlsx', 'NYC.xlsx']
    
    ## begin fn_end loop
    for i, fn_end in enumerate(fn_ends):
    
        ## assemble CEMS datasets
        data_CEMS = retrieve_emissions_and_stitch(fn_end, years, rel_path_CEMS)
        ## read in data from ML
        os.chdir(base_dname) # change to code directory
        os.chdir(rel_path_ML) # change to ML directory
        data_ML = pd.read_excel(fn_ML[i], index_col=None)
        ## filter CEMS data for dates that match ML dates
        mask = data_CEMS['date'].isin(data_ML['Date'])
        data_CEMS = data_CEMS[mask]
        
        ## save data
        os.chdir(base_dname) # change to code directory
        os.chdir(rel_path_output) # change to output directory
        data_CEMS.to_csv('actual_CEMS_'+'_'.join(fn_end)+'_'+str(years[0])+'-'+str(years[-1])+'.csv', index=False)