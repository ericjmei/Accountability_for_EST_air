# -*- coding: utf-8 -*-
"""
Created on Sun Apr  2 18:41:19 2023

stitches fuel price metrics files from different years together

@author: emei3
"""

import pandas as pd
import os

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

## input
# rel_path_input = "../../Data/Simple Dispatch Outputs/2023-05-04 cf ba regions without hist downtime FIXED/Fuel Price Metrics" # folder with fuel prices
rel_path_input = "../../Data/Simple Dispatch Outputs/Fuel Price Metrics/7. ba regions edited"
rel_path_output = "../../Data/Simple Dispatch Outputs/Fuel Price Metrics/7. ba regions edited"
fn_beginning = "actual_fuel_price_metrics_" # beginning of file name (actual and counterfactual)
regions_all = ['SOCO', 'TVA', 'AEC', 'PJM', 'ISNE', 'NYIS', 'SE']
# columns that are repeated 'column1', 'column2', ..., 'column12'
# cols_to_repeat = ['average', 'average_percent_change', 'standard_deviation', 'min', 'max', 'upper_threshold_outliers', 
#                   'excluded_units', 'excluded_units_fraction', 'average_no_outliers'] # counterfactual
cols_to_repeat = ['average', 'standard_deviation', 'min', 'max', 'upper_threshold_outliers', 
                  'excluded_units', 'excluded_units_fraction', 'average_no_outliers', 'standard_deviation_no_outliers'] # actual
# years to run; must be iterable
years = range(2006, 2020)

for region in regions_all:
    # dictionary to hold all dataframes
    output = dict.fromkeys(cols_to_repeat) 
    output['number_of_units'] = None
    
    for year in years:
        # read in the file
        os.chdir(base_dname)
        os.chdir(rel_path_input) # change to data folder
        fn = fn_beginning+region+"_"+str(year)+".csv"
        metrics = pd.read_csv(fn)
        
        # concat purchase type to the end of fuel type (only for natural gas)
        metrics['fuel'] = metrics['fuel'] + '_' + metrics['purchase_type'].fillna('')
        metrics['fuel'] = metrics['fuel'].replace('_nan', '').str.strip('_') # remove '_nan' ending from units
        
        # melt dataframe down by fuel type
        metrics_long = pd.melt(metrics, id_vars=['fuel'], var_name='old_column', value_name='value')
        months = [str(i) for i in range(1, 13)] # repeat range (months 1 through 12)
        
        # add repeated metrics
        for col_name in cols_to_repeat:
            # keep only col_name metric
            column_names_to_match = [col_name + f"{month}" for month in months]
            mask = metrics_long['old_column'].str.contains('|'.join(column_names_to_match))
            metric_to_concat = metrics_long.loc[mask]
            
            # extract number from column name
            metric_to_concat.loc[:, 'old_column'] = metric_to_concat['old_column'].str.extract('(\d+)').astype(int)
            
            # pivot dataframe to wide format
            metric_to_concat = metric_to_concat.pivot(index='old_column', columns='fuel', values='value')
            metric_to_concat.reset_index(inplace=True)
            metric_to_concat.rename(columns={'old_column':'month'}, inplace=True)
            
            # add year column to left of dataframe
            metric_to_concat['year'] = year
            metric_to_concat = pd.concat([metric_to_concat['year'], metric_to_concat.drop('year', axis=1)], axis=1)
            
            # add to output
            output[col_name] = pd.concat([output[col_name], metric_to_concat])
        
        # add number of units to output as well
        mask = metrics_long['old_column'].str.contains('number_of_units')
        metric_to_concat = metrics_long.loc[mask]
        
        # pivot dataframe to wide format
        metric_to_concat = metric_to_concat.pivot(index='old_column', columns='fuel', values='value')
        metric_to_concat.reset_index(inplace=True)
        
        # add year column to left of dataframe
        metric_to_concat['year'] = year
        metric_to_concat = pd.concat([metric_to_concat['year'], metric_to_concat.drop(['year', 'old_column'], axis=1)], axis=1)
        
        # add to output
        output['number_of_units'] = pd.concat([output['number_of_units'], metric_to_concat])
            
    # write to excel
    os.chdir(base_dname)
    os.chdir(rel_path_output) # change to data folder
    fn = fn_beginning+region+"_"+str(years[0])+"-"+str(years[-1])+".xlsx"
    writer = pd.ExcelWriter(fn, engine = 'xlsxwriter')
    for col_name in cols_to_repeat:
        output[col_name].to_excel(writer, sheet_name=col_name, index=False)
    output['number_of_units'].to_excel(writer, sheet_name='number_of_units', index=False)
    writer.close()