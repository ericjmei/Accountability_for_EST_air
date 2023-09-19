# -*- coding: utf-8 -*-
"""
Created on Sun May  7 19:59:27 2023

bin the results of the monte carlo method on emissions and pollutants to daily resolution in which only the median and lower
and upper bounds of the 95 CI remain
I'm working on binning to a weekly resolution as well, but I can't figure that out yet

@author: emei3
"""

import pandas as pd
import os
import numpy as np

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

def bin_daily(df):
    # Calculate the median for each date
    median = df.median(axis=1)
    # Calculate the lower and upper bounds of the 95% interval for each date
    lower_bound = df.quantile(0.025, axis=1)
    upper_bound = df.quantile(0.975, axis=1)
    # Create a new dataframe with the median, lower bound, and upper bound
    new_df = pd.DataFrame({'median': median, 'lower_bound': lower_bound, 'upper_bound': upper_bound})
    return new_df

def func(x): # finds median and lower and upper bounds of 95 CI
    median = np.nanmedian(x.to_numpy().flatten())
    lower_bound = np.nanpercentile(x.to_numpy().flatten(), 2.5)
    upper_bound = np.nanpercentile(x.to_numpy().flatten(), 97.5)
    return pd.Series((median, lower_bound, upper_bound), index=['median', 'lower_bound', 'upper_bound'])

def bin_weekly(df):
    weekly_df = df.resample('W').apply(func)
    return weekly_df

if __name__ == '__main__':
    # directory with counterfactual emissions
    rel_path_input_emissions = "../../Data/Counterfactual Emissions/8. total cf edited"
    # directory with ipnut air pollutants
    rel_path_input_pollutants = "../../Data/Counterfactual Air Pollutants/4b. small uncertainty"
    
    # sites to run for
    sites = ["SDK", # all sites in Atlanta
        "Bronx", "Manhattan", "Queens"] # all sites in NYC
    # all target names to run for
    targetNames = ["pm25", "ozone"] # just PM and ozone for now
    # groups of states to run for; must be parallel to emissions used for "sites"
    fn_ends = [['SOCO'], # ATL regional
                ['NYC']] # NYC regional
    # years to run; must be iterable
    years = range(2006, 2020)
    
    ### emissions
    os.chdir(base_dname) # change to code directory
    os.chdir(rel_path_input_emissions) # change to emissions directory
    for fn_end in fn_ends:
        ## retrieve counterfactual monte carlo emissions
        so2_cf = pd.read_parquet('so2_'+'_'.join(fn_end)+'_'+str(years[0])+'-'+str(years[-1])+'.parquet')
        nox_cf = pd.read_parquet('nox_'+'_'.join(fn_end)+'_'+str(years[0])+'-'+str(years[-1])+'.parquet')
        
        ## bin daily
        so2_cf_daily = bin_daily(so2_cf) # so2
        nox_cf_daily = bin_daily(nox_cf) # nox
        
        ## bin weekly
        so2_cf_weekly = bin_weekly(so2_cf) # so2
        nox_cf_weekly = bin_weekly(nox_cf) # nox
        
        ## write to table
        so2_cf_daily.to_parquet('so2_'+'_'.join(fn_end)+'_'+str(years[0])+'-'+str(years[-1])+'_bin_daily.parquet')
        nox_cf_daily.to_parquet('nox_'+'_'.join(fn_end)+'_'+str(years[0])+'-'+str(years[-1])+'_bin_daily.parquet')
        so2_cf_weekly.to_parquet('so2_'+'_'.join(fn_end)+'_'+str(years[0])+'-'+str(years[-1])+'_bin_weekly.parquet')
        nox_cf_weekly.to_parquet('nox_'+'_'.join(fn_end)+'_'+str(years[0])+'-'+str(years[-1])+'_bin_weekly.parquet')
    
    # ### pollutant concentrations
    # os.chdir(base_dname) # change to code directory
    # os.chdir(rel_path_input_pollutants) # change to pollutant directory
    # # loop through each pollutant at each site
    # for site in sites:
    #     for target in targetNames:
    #         # read data
    #         pollutant = pd.read_parquet(site+'_'+target+'_'+str(years[0])+'-'+str(years[-1])+'.parquet')
    #         # bin daily
    #         pollutant_daily = bin_daily(pollutant)
    #         # bin weekly
    #         pollutant_weekly = bin_weekly(pollutant)
    #         # write to table
    #         pollutant_daily.to_parquet(site+'_'+target+'_'+str(years[0])+'-'+str(years[-1])+'_bin_daily.parquet')
    #         pollutant_weekly.to_parquet(site+'_'+target+'_'+str(years[0])+'-'+str(years[-1])+'_bin_weekly.parquet')
