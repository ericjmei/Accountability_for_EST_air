# -*- coding: utf-8 -*-
"""
Created on Thu May  4 17:27:06 2023

performs least squares regression on the actual CEMS and modeled actual scenarios
then uses the coefficient estimates and standard estimates to generate 5000 time series of counterfactual emissions
saves both these outputs to the counterfactual folder

@author: emei3
"""

from scipy import stats
import numpy as np
import pandas as pd
import os
from datetime import datetime

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

def least_squares_regression(x, y):
    """
    Performs a linear regression on a set of x, y data and returns the slope, 
    intercept, standard errors of the slope and intercept, and R-squared.
    
    Parameters:
        x (array-like): The x values of the data.
        y (array-like): The y values of the data.
        
    Returns:
        tuple: A tuple containing the slope, intercept, standard error of the slope,
        standard error of the intercept, and R-squared.
    """
    
    result = stats.linregress(x, y)
    slope = result.slope
    se_slope = result.stderr
    intercept = result.intercept
    se_intercept = result.intercept_stderr
    r = result.rvalue
    
    # Calculate predicted values
    y_pred = intercept + slope * np.array(x)
    
    # Calculate residuals
    residuals = np.array(y) - y_pred
    
    # Calculate RMSE
    rmse = np.sqrt(np.mean(residuals**2))
    
    r_squared = r**2

    return slope, intercept, se_slope, se_intercept, r_squared, rmse

def monte_carlo_regression_prediction(x, slope, intercept, se_slope, n_simulations=5000):
    """
    Uses a Monte Carlo method to predict a series of y values for a given set of x values using an regression. 
    Assumes normal distribution around regression

    Parameters:
    x (array-like): The x values for which to predict y values.
    slope (float): The slope of the regression line.
    intercept (float): The intercept of the regression line.
    se_slope (float): The standard error of the slope of the regression line.
    se_intercept (float): The standard error of the intercept of the regression line.
    n_simulations (int): The number of simulations to run. Default is 5000.

    Returns:
    y_mc (ndarray): An array of shape (n_simulations, len(x)) containing the predicted y values for each simulation and each x value.
    """
    y_mc = np.zeros((n_simulations, len(x)))

    for i in range(n_simulations):
        noise = np.random.normal(slope, se_slope, size=x.shape)
        y_mc[i] = slope * x + intercept + noise
        # ensure emissions can't go below 0
        y_mc[i][y_mc[i] < 0] = 0
    
    output = pd.DataFrame(y_mc.T) # change to dataframe
    column_names = ['column_' + str(i) for i in range(0, n_simulations)] # rename columns for easier saving
    output.rename(columns=dict(zip(output.columns, column_names)), inplace=True)
    return output

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
    rel_path_SD_act = "../../Data/Simple Dispatch Outputs/2023-06-23 act" # directory with simple dispatch actual
    rel_path_SD_cf = "../../Data/Simple Dispatch Outputs/2023-06-23 cf" # directory with simple dispatch counterfactual
    rel_path_output = "../../Data/Counterfactual Emissions/7. ba regions edited"
    
    # years to run; must be iterable
    years = range(2006, 2020)
    # filename endings to run the analysis for
    fn_ends = [['SOCO'], ['NYC']]
    
    for fn_end in fn_ends:
    
        ## assemble CEMS and SD_actual datasets
        data_CEMS = retrieve_emissions_and_stitch(fn_end, years, rel_path_CEMS)
        data_SD_act = retrieve_emissions_and_stitch(fn_end, years, rel_path_SD_act)
        
        ## perform regression
        # so2
        slope_so2, intercept_so2, se_slope_so2, se_intercept_so2, r_squared_so2, rmse_so2 = least_squares_regression(
            data_SD_act['so2_tot'], data_CEMS['so2_tot'])
        # nox
        slope_nox, intercept_nox, se_slope_nox, se_intercept_nox, r_squared_nox, rmse_nox = least_squares_regression(
            data_SD_act['nox_tot'], data_CEMS['nox_tot'])
        
        ## save regression stats
        os.chdir(base_dname) # change to code directory
        os.chdir(rel_path_output) # change to output directory
        writer = pd.ExcelWriter('regression_stats_'+'_'.join(fn_end)+'_'+str(years[0])+'-'+str(years[-1])+'.xlsx')
        # save so2
        df = pd.DataFrame({'slope': [slope_so2],
                           'intercept': [intercept_so2],
                           'se_slope': [se_slope_so2],
                           'se_intercept': [se_intercept_so2],
                           'r_squared': [r_squared_so2],
                           'rmse': [rmse_so2]})
        df.to_excel(writer, sheet_name='so2', index=False)
        # save nox
        df = pd.DataFrame({'slope': [slope_nox],
                           'intercept': [intercept_nox],
                           'se_slope': [se_slope_nox],
                           'se_intercept': [se_intercept_nox],
                           'r_squared': [r_squared_nox],
                           'rmse': [rmse_nox]})
        df.to_excel(writer, sheet_name='nox', index=False)
        writer.close()
        
        ## assmble SD_counterfactual dataset
        data_SD_cf = retrieve_emissions_and_stitch(fn_end, years, rel_path_SD_cf)
        ## perform monte carlo method on emissions
        # so2
        data_cf_so2 = monte_carlo_regression_prediction(data_SD_cf['so2_tot'], slope_so2, intercept_so2, se_slope_so2)
        # nox
        data_cf_nox = monte_carlo_regression_prediction(data_SD_cf['nox_tot'], slope_nox, intercept_nox, se_slope_nox)
        ## append date column to end of dataframe
        # Generate a range of dates from 1/1/2006 to 12/31/2019
        start_date = datetime(2006, 1, 1)
        end_date = datetime(2019, 12, 31)
        date_range = pd.date_range(start_date, end_date)
        # Create a new dataframe with the date column
        new_dataframe = pd.DataFrame({'Date': date_range})
        # Concatenate the new dataframe with the existing dataframe
        data_cf_so2 = pd.concat([data_cf_so2, new_dataframe], axis=1)
        data_cf_nox = pd.concat([data_cf_nox, new_dataframe], axis=1)
        # set index
        data_cf_so2.set_index('Date', inplace=True)
        data_cf_nox.set_index('Date', inplace=True)
        ## saves monte carlo output as parquet file
        os.chdir(base_dname) # change to code directory
        os.chdir(rel_path_output) # change to output directory
        data_cf_so2.to_parquet('so2_'+'_'.join(fn_end)+'_'+str(years[0])+'-'+str(years[-1])+'.parquet')
        data_cf_nox.to_parquet('nox_'+'_'.join(fn_end)+'_'+str(years[0])+'-'+str(years[-1])+'.parquet')