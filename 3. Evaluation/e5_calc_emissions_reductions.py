# -*- coding: utf-8 -*-
"""
Created on Wed May 24 08:16:11 2023

calculate emissions reductions between:
    1. short-run counterfactual and observations
    2. total counterfactual and short-run counterfactual

@author: emei3
"""

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from datetime import datetime

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

def plot_impact(df, ylabel):
    # Plotting the impact time-series with upper and lower bounds
    fig, ax = plt.subplots()
    
    # Plot the median line
    ax.plot(df.index, df['median'], color='blue', label='Median')
    # plot line at 0
    plt.axhline(y=0, color='r', linestyle='-')
    
    # Fill the area between lower and upper bounds
    ax.fill_between(df.index, df['lower_bound'], df['upper_bound'], color='gray', alpha=0.5, label='Range')
    
    # Set labels and title
    ax.set_xlabel('Date')
    ax.set_ylabel(ylabel)
    
    # Add legend
    ax.legend()
    
    # Rotate x-axis labels for better visibility (optional)
    plt.xticks(rotation=45)
    
    # Display the plot
    plt.show()
    
    return fig

if __name__ == '__main__':
    
    ## define relative file paths
    rel_path_emissions_obs = "../../Data/Simple Dispatch Outputs/2023-06-23 act/Actual CEMS" # folder with observed emissions
    rel_path_emissions_total_cf = "../../Data/Counterfactual Emissions/8. total cf edited" # relative path of total counterfactual emissions
    rel_path_emissions_short_cf = "../../Data/Counterfactual Emissions/7. ba regions edited" # relative path of short-run counterfactual emissions
    rel_path_output = "../../Data/Emissions Reductions/2. edited" # relative path of outputs
    
    ## define input years, regions, states, and whether ozone season should be separated
    years_for_emissions = range(2006, 2020) # specify years to use for ER
    # specify species
    species = 'so2' # nox or so2
    # filename endings to search for when stitching CEMS data
    fn_ends = [['SOCO'], # ATL regional
                ['NYC']] # NYC regional
    
    i = 0
    fn_end = fn_ends[i]
    
    ## retrieve counterfactual emissions
    fn = species+'_'+'_'.join(fn_end)+'_'+str(years_for_emissions[0])+'-'+str(years_for_emissions[-1])+'_bin_daily.parquet'
    # total cf
    os.chdir(base_dname)
    os.chdir(rel_path_emissions_total_cf)
    emissions_total_cf = pd.read_parquet(fn)
    # short-run cf
    os.chdir(base_dname)
    os.chdir(rel_path_emissions_short_cf)
    emissions_short_cf = pd.read_parquet(fn)
    ## make standard deviation column based on 95% bounds (upper - lower)/(2*1.96)
    emissions_total_cf['std'] = (emissions_total_cf['upper_bound'] - emissions_total_cf['lower_bound'])/(2*1.96)
    emissions_short_cf['std'] = (emissions_short_cf['upper_bound'] - emissions_short_cf['lower_bound'])/(2*1.96)
    # also calculate relative standard deviation
    emissions_short_cf['std_norm'] = np.divide(emissions_short_cf['std'], emissions_short_cf['median'])
    emissions_total_cf['std_norm'] = np.divide(emissions_total_cf['std'], emissions_total_cf['median'])
    
    ## retrieve oberved emissions
    emissions_obs = retrieve_emissions_and_stitch(fn_end, years_for_emissions, rel_path_emissions_obs)
    # processing to ensure observed emissions dataframe is compatible with cf
    emissions_obs = emissions_obs.rename(columns={'date':'Date'})
    emissions_obs.set_index('Date', inplace=True)
    
    ## 1. get short-run impact (short-run - observed)
    impact_short = emissions_short_cf.copy()
    impact_short = impact_short.sub(emissions_obs[species+'_tot'], axis=0)
    
    ## 2. get medium- and long-run impacts (total - short-run)
    impact_medium_long = pd.DataFrame()
    impact_medium_long['median'] = emissions_total_cf['median'] - emissions_short_cf['median']
    # uncertainty bounds are added in quadrature because uncertainty is gaussian around median
    impact_medium_long['std'] = np.sqrt(np.square(emissions_total_cf['std']) + np.square(emissions_short_cf['std']))
    impact_medium_long['std_norm'] = abs(np.divide(impact_medium_long['std'], impact_medium_long['median']))
    impact_medium_long['lower_bound'] = impact_medium_long['median'] - 1.96*impact_medium_long['std'] 
    impact_medium_long['upper_bound'] = impact_medium_long['median'] + 1.96*impact_medium_long['std']
    
    ## also calculate normalized E_short-run impacts (Eshort/Etotal) - 1
    emissions_short_cf_norm = pd.DataFrame()
    emissions_short_cf_norm['median'] = np.divide(emissions_short_cf['median'], emissions_total_cf['median']) - 1
    # uncertainty is normalized uncertainties of medium- and long-impact and E_total added in quadrature
    # we use medium-long impact bc (Eshort/Etotal) - 1 = (Eshort-Etotal/Etotal) = Impact_med-long/Etotal
    emissions_short_cf_norm['std_norm'] = np.sqrt(np.square(emissions_total_cf['std_norm']) + np.square(impact_medium_long['std_norm']))
    emissions_short_cf_norm['std'] = abs(np.multiply(emissions_short_cf_norm['std_norm'], emissions_short_cf_norm['median']))
    emissions_short_cf_norm['lower_bound'] = emissions_short_cf_norm['median'] - 1.96*emissions_short_cf_norm['std'] 
    emissions_short_cf_norm['upper_bound'] = emissions_short_cf_norm['median'] + 1.96*emissions_short_cf_norm['std']
    
    ## plot
    ylabel = 'Emissions (kg/day)'
    fig_short = plot_impact(impact_short, ylabel)
    fig_medium_long = plot_impact(impact_medium_long, ylabel)
    fig_emissions_short_cf_norm = plot_impact(emissions_short_cf_norm, r"$\dfrac{E_{short-run}}{E_{total}} - 1$")
    
    ## save outputs
    os.chdir(base_dname)
    os.chdir(rel_path_output)
    # short-run
    fn = species+'_'+'_'.join(fn_end)+'_'+str(years_for_emissions[0])+'-'+str(years_for_emissions[-1])+'_short-run'
    # fig_short.savefig('fig_'+fn+'.png', dpi=fig_short.dpi*10, bbox_inches='tight')
    impact_short.to_parquet(fn+'.parquet')
    # medium- and long-run
    fn = species+'_'+'_'.join(fn_end)+'_'+str(years_for_emissions[0])+'-'+str(years_for_emissions[-1])+'_medium-long-run'
    # fig_medium_long.savefig('fig_'+fn+'.png', dpi=fig_medium_long.dpi*10, bbox_inches='tight')
    impact_medium_long.to_parquet(fn+'.parquet')
    # normalized E_short
    fn = species+'_'+'_'.join(fn_end)+'_'+str(years_for_emissions[0])+'-'+str(years_for_emissions[-1])+'_short-run_normalized'
    # fig_emissions_short_cf_norm.savefig('fig_'+fn+'.png', dpi=fig_emissions_short_cf_norm.dpi*10, bbox_inches='tight')
    emissions_short_cf_norm.to_parquet(fn+'.parquet')