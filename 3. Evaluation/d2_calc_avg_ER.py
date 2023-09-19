# -*- coding: utf-8 -*-
"""
Created on Tue May 23 09:11:53 2023

reports average and standard deviation of ERs when averaged by day of year
NOTE: assumes no leap day in any of the years

@author: emei3
"""

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

if __name__ == '__main__':
    
    ## define relative file paths
    rel_path_ERs = "../../Data/Counterfactual Emissions/8. total cf edited" # relative path of ER outputs
    rel_path_output = "../../Data/Counterfactual Emissions/8. total cf edited" # relative path of ER outputs
    
    ## define input years, regions, states, and whether ozone season should be separated
    years_for_ER = range(2006, 2008) # specify years to use for ER
    # specify species
    species = 'so2' # nox or so2
    # filename endings to search for when stitching CEMS data
    fn_ends = [['SOCO'], # ATL regional
                ['NYC']] # NYC regional
    
    ## for loop
    for i, fn_end in enumerate(fn_ends):
    
        ## retrieve ERs
        os.chdir(base_dname)
        os.chdir(rel_path_ERs)
        fn = species+'_ER_'+'_'.join(fn_end)+'_'+str(years_for_ER[0])+'-'+str(years_for_ER[-1])+'.parquet'
        ER = pd.read_parquet(fn)
        
        ## bin species by day of year and find average and standard deviation
        # Extract the month and day from the 'date' column
        ER['month'] = ER['date'].dt.month
        ER['day'] = ER['date'].dt.day
        
        # Group the data by month and day, and calculate the mean and standard deviation for each group
        # mean
        avg_ER = ER.groupby(['month', 'day']).mean(numeric_only=True)
        avg_ER = avg_ER.rename(columns={'demand':'demand_avg', species:(species+'_avg')})
        # standard deviation
        std_ER = ER.groupby(['month', 'day']).std(numeric_only=True)
        std_ER = std_ER.rename(columns={'demand':'demand_std', species:(species+'_std')})
        # concatenate mean and standard deviation
        output = pd.concat([avg_ER, std_ER[['demand_std', species+'_std']]], axis=1)
        # calculate coefficient of variation for each day
        output[species+'_cov'] = np.divide(output[species+'_std'], output[species+'_avg'])
        
        # Reset the index to obtain a DataFrame with the 'month' and 'day' as columns
        output = output.reset_index().copy()
        
        # export ER
        fn = species+'_ER_averaged_'+'_'.join(fn_end)+'_'+str(years_for_ER[0])+'-'+str(years_for_ER[-1])+'.parquet'
        output.to_parquet(fn, index=False)
        
        ## plot average and standard deviation
        fig, ax = plt.subplots(2, 1)
        fig.tight_layout(h_pad=2)
        # mean
        plt.subplot(2, 1, 1)
        dayofyear = range(1, 366)
        plt.plot(dayofyear, output[species+'_avg'])
        plt.xlabel('day of year')
        plt.ylabel('ER (kg/MWh)')
        
        # cov
        plt.subplot(2, 1, 2)
        plt.plot(dayofyear, output[species+'_cov'])
        plt.xlabel('day of year')
        plt.ylabel('CoV')
        
        # export
        os.chdir('./figures')
        fn = 'fig_'+species+'_ER_averaged_'+'_'.join(fn_end)+'_'+str(years_for_ER[0])+'-'+str(years_for_ER[-1])+'.png'
        fig.savefig(fn, dpi=fig.dpi*10)
        