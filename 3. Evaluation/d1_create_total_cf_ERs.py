# -*- coding: utf-8 -*-
"""
Created on Tue May 16 20:02:06 2023

creates total counterfactual emissions ratios in a timeseries for 
    the specified regions over the specified time periods
can separate ozone season and non-ozone season as well

@author: emei3
"""

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime

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

def plot_ERs(data, col_to_plot):
    """
    plots histogram and timeseries for inputted data and column to plot from that data
    make sure data has a corresponding 'Date' column

    Parameters
    ----------
    data : dataframe
        dataframe containing 'Date' and col_to_plot.
    col_to_plot : str
        name of column to plot.

    Returns
    -------
    figure handle of plots.

    """
    
    fig, ax = plt.subplots(2, 1)
    fig.tight_layout(h_pad=2)
    # histogram
    plt.subplot(2, 1, 1)
    data[col_to_plot].plot(kind='hist', edgecolor='black')
    plt.xlabel(col_to_plot+' ER (kg/MWh)')
    
    # time-series
    plt.subplot(2, 1, 2)
    # script to insert nans for large gaps
    time_diff = data.date.diff()
    gaps_mask = time_diff > pd.Timedelta(days=2)
    gaps_mask = gaps_mask.drop(gaps_mask[gaps_mask == False].index).copy()
    for gap in gaps_mask.items():
        nan_date = data.date[gap[0]] - pd.Timedelta(days=1)
        data = pd.concat([data, 
                          pd.DataFrame({'date': nan_date, col_to_plot: np.nan}, index=[gap[0]])],
                         axis = 0)
    data = data.sort_values('date').copy()
    plt.plot(data['date'], data[col_to_plot])
    plt.xlabel('Date')
    plt.ylabel(col_to_plot+' ER (kg/MWh)')
    # Set the x-axis tick frequency and format
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 7]))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%Y'))
    plt.gca().set_xlim(datetime.datetime(years_for_ER[0], 1, 1), None)
    
    return fig

if __name__ == '__main__':
    
    ## define relative file paths
    rel_path_CEMS = "../../Data/Simple Dispatch Outputs/2023-06-23 act/Actual CEMS" # folder with observed emissions
    rel_path_output = "../../Data/Counterfactual Emissions/8. total cf edited" # relative path of ER outputs
    
    ## define input years, regions, states, and whether ozone season should be separated
    years_for_ER = range(2006, 2008) # specify years to use for ER
    # specify species
    species = 'nox' # nox or so2
    # specify whether ozone season (May 1 to Sept 30) should be separated from timeseries (for NOx)
    separate_ozone_season = True
    # filename endings to search for when stitching CEMS data
    fn_ends = [['SOCO'], # ATL regional
                ['NYC']] # NYC regional
               
    
    for i, fn_end in enumerate(fn_ends):
        
        ## assemble CEMS datasets
        data_CEMS = retrieve_emissions_and_stitch(fn_end, years_for_ER, rel_path_CEMS)
        
        ## assemble new dataframe and export
        output = data_CEMS[['date', 'demand']].copy()
        # assemble ERs
        output[species] = np.divide(data_CEMS[species+'_tot'], data_CEMS['demand'])
        # reset index
        output.reset_index(drop=True, inplace=True)
        # export
        os.chdir(base_dname) # change to code directory
        os.chdir(rel_path_output) # change to input folder
        fn = species+'_ER_'+'_'.join(fn_end)+'_'+str(years_for_ER[0])+'-'+str(years_for_ER[-1])+'.parquet'
        output.to_parquet(fn, index=False)
        
        # if ozone season, split up output and export
        if separate_ozone_season:
            mask = (output['date'].dt.month >= 5) & (output['date'].dt.month <= 9)
            output_ozone_season = output.loc[mask].copy()
            output_non_ozone_season = output.loc[~mask].copy()
            # reset index
            output_ozone_season.reset_index(drop=True, inplace=True)
            output_non_ozone_season.reset_index(drop=True, inplace=True)
            # export
            fn = species+'_ER_'+'_'.join(fn_end)+'_'+str(years_for_ER[0])+'-'+str(years_for_ER[-1])+'_ozone_season.parquet'
            output_ozone_season.to_parquet(fn, index=False)
            fn = species+'_ER_'+'_'.join(fn_end)+'_'+str(years_for_ER[0])+'-'+str(years_for_ER[-1])+'_non_ozone_season.parquet'
            output_non_ozone_season.to_parquet(fn, index=False)
        
        ## plot and export
        output_plot = plot_ERs(output, species)
        # export
        os.chdir('./figures')
        fn = 'fig_'+species+'_ER_'+'_'.join(fn_end)+'_'+str(years_for_ER[0])+'-'+str(years_for_ER[-1])+'.png'
        output_plot.savefig(fn, dpi=output_plot.dpi*10)
        
        # if ozone season, plot separate seasons and export
        if separate_ozone_season:
            output_ozone_season_plot = plot_ERs(output_ozone_season, species)
            output_non_ozone_season_plot = plot_ERs(output_non_ozone_season, species)
            # export
            fn = 'fig_'+species+'_ER_'+'_'.join(fn_end)+'_'+str(years_for_ER[0])+'-'+str(years_for_ER[-1])+'_ozone_season.png'
            output_ozone_season_plot.savefig(fn, dpi=output_ozone_season_plot.dpi*10)
            fn = 'fig_'+species+'_ER_'+'_'.join(fn_end)+'_'+str(years_for_ER[0])+'-'+str(years_for_ER[-1])+'_non_ozone_season.png'
            output_non_ozone_season_plot.savefig(fn, dpi=output_non_ozone_season_plot.dpi*10)