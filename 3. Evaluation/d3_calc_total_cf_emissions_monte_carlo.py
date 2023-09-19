# -*- coding: utf-8 -*-
"""
Created on Tue May 16 20:02:06 2023

uses the split ERs to create timeseries of counterfactual emissions for the time periods of interest

@author: emei3
"""

import pandas as pd
import numpy as np
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

def monte_carlo_prediction(x, ER_means, cov, n_simulations=5000):
    """
    Uses a Monte Carlo method to predict a series of y values for a given set of x values
    x here is demand and y is the emissions mass

    Parameters:
    x (array-like): The x values for which to predict y values.
    ER_means (array-like): Mean ERs matched to day of year parallel to demands in x.
    cov (float): Mean CoV of all averaged ERs from base years.
    n_simulations (int): The number of simulations to run. Default is 5000.

    Returns:
    y_mc (ndarray): An array of shape (n_simulations, len(x)) containing the predicted y values for each simulation and each x value.
    """
    y_mc = np.zeros((n_simulations, len(x)))

    for i in range(n_simulations):
        ER_mc = np.random.normal(loc=ER_means, scale=ER_means*cov)
        y_mc[i] = np.multiply(x, ER_mc)
        # ensure emissions can't go below 0
        y_mc[i][y_mc[i] < 0] = 0
    
    output = pd.DataFrame(y_mc.T) # change to dataframe
    column_names = ['column_' + str(i) for i in range(0, n_simulations)] # rename columns for easier saving
    output.rename(columns=dict(zip(output.columns, column_names)), inplace=True)
    return output

if __name__ == '__main__':
    
    ## define relative file paths
    rel_path_CEMS = "../../Data/Simple Dispatch Outputs/2023-06-23 act/Actual CEMS" # folder with observed emissions
    rel_path_ERs = "../../Data/Counterfactual Emissions/8. total cf edited" # folder with observed emissions
    rel_path_output = "../../Data/Counterfactual Emissions/8. total cf edited" # relative path of ER outputs
    
    ## define input years, regions, states, and whether ozone season should be separated
    years_for_ER = range(2006, 2008) # specify years to use for ER
    years_for_cf = range(2006, 2020) # specify years to calculate counterfactual
    # specify species
    species = 'nox' # nox or so2
    # filename endings to search for when stitching CEMS data
    fn_ends = [['SOCO'], # ATL regional
                ['NYC']] # NYC regional
    
    
    for fn_end in fn_ends:
        
        ## import ERs and calc avg CoV
        os.chdir(base_dname) 
        os.chdir(rel_path_ERs) 
        fn = species+'_ER_averaged_'+'_'.join(fn_end)+'_'+str(years_for_ER[0])+'-'+str(years_for_ER[-1])+'.parquet'
        ER = pd.read_parquet(fn)
        CoV = np.mean(ER[species+'_cov'])
        CoV_std = np.std(ER[species+'_cov'])
        
        ## add leap day to ERs by duplicating last day in Feb
        mask = (ER['month'] == 2) & (ER['day'] == 28)
        ER_leapday = ER.loc[mask]
        ER_leapday.loc[ER_leapday.index, 'day'] = 29
        ER = pd.concat([ER, ER_leapday], axis=0)
        
        ## assemble CEMS dataframe
        data_CEMS = retrieve_emissions_and_stitch(fn_end, years_for_cf, rel_path_CEMS)
        # retrieve demand and date data
        data_CEMS = data_CEMS[['date', 'demand']].copy()
        
        ## match average ERs to emissions dataframe
        # Extract the month and day from the 'date' column
        data_CEMS['month'] = data_CEMS['date'].dt.month
        data_CEMS['day'] = data_CEMS['date'].dt.day
        data_CEMS = pd.merge(data_CEMS, ER[['month', 'day', species+'_avg']], on=['month', 'day'], how='left')
        
        ## assmble monte carlo timeseries 
        output = monte_carlo_prediction(data_CEMS['demand'], data_CEMS[species+'_avg'], CoV)
        # concat datetime to left of output
        output = pd.concat([data_CEMS['date'].reset_index(drop=True), output], axis=1)
        
        ## save output
        output = output.rename(columns={'date':'Date'}) # for ML compatibility
        output.set_index('Date', inplace=True)
        os.chdir(base_dname) # change to code directory
        os.chdir(rel_path_output) # change to input directory
        fn = species+'_'+'_'.join(fn_end)+'_'+str(years_for_cf[0])+'-'+str(years_for_cf[-1])+'.parquet'
        output.to_parquet(fn)
