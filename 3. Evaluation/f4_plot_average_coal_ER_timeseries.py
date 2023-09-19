# -*- coding: utf-8 -*-
"""
Created on Mon Jun 12 12:08:14 

plot average ER from available coal plants from the generator data objects from simple dispatch
use df_AMPD to ensure correct plants are operating

@author: emei3
"""

import pandas as pd
import os
import numpy as np
import pickle
import matplotlib.pyplot as plt

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

# calculate median, weighted average, and weighted standard deviation of generators fed in
def calc_weighted_metrics(gen_data_operating, run_year, col):
    ### calculate average and standard deviation weighted by mw
    output = pd.DataFrame({'year': run_year,
                           'week': range(1, 53)})
    for week in range(1, 53):
        ## retrieve data from EGUs for each week of the year
        data_to_calc = gen_data_operating[[col+str(week), 'mw'+str(week)]].copy()
        # rename columns
        data_to_calc.columns = [col, 'mw']
        ## calculate weighted median and lower and upper bounds (95%)
        output.loc[week-1, 'median'] = np.median(data_to_calc[col])
        ## calculate weighted average ER
        weighted_average = np.average(data_to_calc[col], weights=data_to_calc['mw'])
        output.loc[week-1, 'average'] = weighted_average
        ## calculate weighted standard deviation ER
        weighted_std = np.sqrt(np.average((data_to_calc[col] - weighted_average)**2, weights=data_to_calc['mw']))
        output.loc[week-1, 'std'] = weighted_std
    
    return output

#%%
if __name__ == '__main__':
    
    ## define relative file paths
    # directory with plant characteristics
    rel_path_input_plant_characteristics = "../../Data/plant characteristics/" # input features have base EGU emissions
    # directory for generator data
    rel_path_input_generators = "../../Data/Simple Dispatch Outputs/2023-05-10 act ba coal propagated/Generator Data"
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
    i = 1 # choose which site to run for
    ba_region_group = ba_region_groups[i]
    
    ## filter AMPD data to only include the ba regions
    df_AMPD_subset = df_AMPD.loc[df_AMPD['ba'].isin(ba_region_group), :]
    
    ## instantiate output dataframe
    ER_so2_all = pd.DataFrame()
    ER_so2_end = pd.DataFrame()
    ER_nox_all = pd.DataFrame()
    ER_nox_end = pd.DataFrame()
    
    ## import generator data for 2019
    os.chdir(base_dname)
    os.chdir(rel_path_input_generators)
    gen_data_end = pd.DataFrame()
    for ba_region in ba_region_group:
        gd_short = pickle.load(open('generator_data_short_%s_%s.obj'%(ba_region, str(2019)), 'rb')) # load generatordata object
        gen_data_end = pd.concat([gen_data_end, gd_short['df']], axis=0) # load generator data dataframe and concat to existing gen data (if multiple)
    
    for run_year in years_all:
        ## import generator data
        os.chdir(base_dname)
        os.chdir(rel_path_input_generators)
        gen_data = pd.DataFrame()
        for ba_region in ba_region_group:
            gd_short = pickle.load(open('generator_data_short_%s_%s.obj'%(ba_region, str(run_year)), 'rb')) # load generatordata object
            gen_data = pd.concat([gen_data, gd_short['df']], axis=0) # load generator data dataframe and concat to existing gen data (if multiple)
        
        ### calculate metrics for all EGUs in any particular year
        ## retrieve operating EGUs
        mask = (gen_data['fuel_type'] == 'coal') # 3
        gen_data_operating = gen_data.loc[mask, :].copy()
        ## calculate average and standard deviation weighted by mw
        ER_so2_year = calc_weighted_metrics(gen_data_operating, run_year, 'so2')
        ER_nox_year = calc_weighted_metrics(gen_data_operating, run_year, 'nox')
        ## concatenate to final dataframe
        ER_so2_all = pd.concat([ER_so2_all, ER_so2_year], axis=0)
        ER_nox_all = pd.concat([ER_nox_all, ER_nox_year], axis=0)
        
        ### calculate metrics for only EGUs that are active in end of time period
        ## retrieve operating EGUs
        # index for coal EGUs that aren't retired by the end
        mask = (gen_data_end['fuel_type'] == 'coal') & (gen_data_end['mw52'] != 0)
        # obtain operating EGU orispl_unit
        EGU_operating = gen_data_end.loc[mask, 'orispl_unit']
        # index for those EGUs from gen_data
        mask = gen_data['orispl_unit'].isin(EGU_operating)
        gen_data_operating = gen_data.loc[mask, :].copy()
        ## calculate average and standard deviation weighted by mw
        ER_so2_year = calc_weighted_metrics(gen_data_operating, run_year, 'so2')
        ER_nox_year = calc_weighted_metrics(gen_data_operating, run_year, 'nox')
        ## concatenate to final dataframe
        ER_so2_end = pd.concat([ER_so2_end, ER_so2_year], axis=0)
        ER_nox_end = pd.concat([ER_nox_end, ER_nox_year], axis=0)

    ### plot average and std from both dataframes
    fig_so2, ax = plt.subplots()
    dates = pd.to_datetime(ER_so2_all['year'].astype(str) + ER_so2_all['week'].astype(str) + '1', format='%Y%W%w')
    
    # Plot the average lines
    ax.plot(dates, ER_so2_all['average'], color='blue', label='all')
    ax.plot(dates, ER_so2_end['average'], color='green', label='EGUs that remain by 2019')
    
    # Plot the std shading
    ax.fill_between(dates, ER_so2_all['average'] - ER_so2_all['std'],
                    ER_so2_all['average'] + ER_so2_all['std'], color='lightblue', alpha=0.5)
    ax.fill_between(dates, ER_so2_end['average'] - ER_so2_end['std'],
                    ER_so2_end['average'] + ER_so2_end['std'], color='lightgreen', alpha=0.5)

    plt.xlabel('Date')
    plt.ylabel('SO2 (kg/MWh)') 
    plt.legend()
    plt.show()
    
    ## NOx
    fig_nox, ax = plt.subplots()
    dates = pd.to_datetime(ER_nox_all['year'].astype(str) + ER_nox_all['week'].astype(str) + '1', format='%Y%W%w')
    
    # Plot the average lines
    ax.plot(dates, ER_nox_all['average'], color='blue', label='all')
    ax.plot(dates, ER_nox_end['average'], color='green', label='EGUs that remain by 2019')
    
    # Plot the std shading
    ax.fill_between(dates, ER_nox_all['average'] - ER_nox_all['std'],
                    ER_nox_all['average'] + ER_nox_all['std'], color='lightblue', alpha=0.5)
    ax.fill_between(dates, ER_nox_end['average'] - ER_nox_end['std'],
                    ER_nox_end['average'] + ER_nox_end['std'], color='lightgreen', alpha=0.5)

    plt.xlabel('Date')
    plt.ylabel('NOx (kg/MWh)') 
    plt.legend()
    plt.show()
    
    ### save data
    os.chdir(base_dname)
    os.chdir(rel_path_output_plant_characteristics)
    with pd.ExcelWriter('ER_trends_coal_'+ region_names[i] +'.xlsx') as writer:
        # so2
        ER_so2_all.to_excel(writer, sheet_name='so2 all', index=False)
        ER_so2_end.to_excel(writer, sheet_name='so2 end', index=False)
        # nox
        ER_nox_all.to_excel(writer, sheet_name='nox all', index=False)
        ER_nox_end.to_excel(writer, sheet_name='nox end', index=False)