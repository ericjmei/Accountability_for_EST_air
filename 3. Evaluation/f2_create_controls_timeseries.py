# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 14:27:50 2023

use cleaned AMPD and eGRID data to create annual time-series that show controls installation 

@author: emei3
"""

import pandas as pd
import os
import numpy as np

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

def sum_over_years(df, col_to_calculate):
    # mask for column to calculate (where it equals 1)
    mask = df[col_to_calculate] == 1
    df_mask = df.loc[mask, :]
    # instantiate dataframe
    output = pd.DataFrame()
    
    ## map maximum values mw of the df_mask dataframe
    # Get the maximum 'mw' values for each unique 'orispl_unit' in the original dataframe
    max_mw_values = df.groupby('orispl_unit')['mw'].max()
    
    # Assign the maximum 'mw' values to the corresponding 'orispl_unit' values in the separate series
    df_mask['mw'] = df_mask['orispl_unit'].map(max_mw_values).copy()
    
    for i, year in enumerate(years_all):
        # mask for year in df
        mask = df_mask['year'] == year
        # sum number of units and their mw
        sum_number = sum(mask)
        sum_mw = np.nansum(df_mask.loc[mask, 'mw'])
        
        # assemble row
        row = pd.DataFrame({'year':year, 'number':sum_number, 'mw':sum_mw}, index=[i])
        # concatenate to output
        output = pd.concat([output, row], axis=0)
        
    return df_mask, output

if __name__ == '__main__':
    
    ## define relative file paths
    # directory with machine learning model input features
    rel_path_input_plant_characteristics = "../../Data/plant characteristics/" # input features have base EGU emissions
    # directory for output merged and cleaned dataframe
    rel_path_output_plant_characteristics = rel_path_input_plant_characteristics
    
    # define groups of balancing authority regions to run for
    ba_region_groups = [['SOCO'], ['PJM', 'ISNE', 'NYIS']]
    region_names = ['ATL', 'NYC']
    # define years to run for
    years_all = range(2006, 2020)
    
    ## import AMPD
    os.chdir(base_dname)
    os.chdir(rel_path_input_plant_characteristics)
    df_AMPD = pd.read_excel('merged_AMPD_eGRID.xlsx')
    
    #%%
    ## mask for group
    i = 1
    ba_region_group = ba_region_groups[i]
    
    df_AMPD_subset = df_AMPD.loc[df_AMPD['ba'].isin(ba_region_group), :]
    
    ## sum up number and mwh of FGDs installed
    FGD_units_installed, FGD_installed_trend = sum_over_years(df_AMPD_subset, 'so2_FGD_install')
    
    ## sum up number and mwh of SCRs installed
    SCR_units_installed, SCR_installed_trend = sum_over_years(df_AMPD_subset, 'nox_SCR_install')
    
    ## write to file
    os.chdir(base_dname)
    os.chdir(rel_path_output_plant_characteristics)
    with pd.ExcelWriter('FGD_'+ region_names[i] +'.xlsx') as writer:
        FGD_units_installed.to_excel(writer, sheet_name='units installed', index=False)
        FGD_installed_trend.to_excel(writer, sheet_name='trend', index=False)
    with pd.ExcelWriter('SCR_'+ region_names[i] +'.xlsx') as writer:
        SCR_units_installed.to_excel(writer, sheet_name='units installed', index=False)
        SCR_installed_trend.to_excel(writer, sheet_name='trend', index=False)