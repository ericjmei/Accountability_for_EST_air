# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 11:37:05 2023

calculates modeled actual air quality with "constant" uncertainties in the same way that air quality is calculated
    for the counterfactuals to perform the modeled counterfactual vs. modeled actual

@author: emei3
"""

import pandas as pd
import numpy as np
import os
import joblib

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

# import functions of cf emissions to air pollutant and also bin monte carlo
os.chdir(base_dname)

def bin_daily(df):
    # Calculate the median for each date
    median = df.median(axis=1)
    # Calculate the lower and upper bounds of the 95% interval for each date
    lower_bound = df.quantile(0.025, axis=1)
    upper_bound = df.quantile(0.975, axis=1)
    # Create a new dataframe with the median, lower bound, and upper bound
    new_df = pd.DataFrame({'median': median, 'lower_bound': lower_bound, 'upper_bound': upper_bound})
    return new_df

def perturb_lognormal(df, columns_to_perturb, sigma):
    """
    randomly redistributes columns_to_perturb of dataframe df with a lognormal distribution of standard deviation sigma    
    """
        
    output = df.copy()
    
    for col in columns_to_perturb:
        X_to_perturb = output.loc[:, col].copy()
        # perturb the emissions with a lognormal distribution with sigma
        X_to_perturb = np.random.lognormal(mean=np.log(X_to_perturb),
                                           sigma=sigma)
        # file back into prediction dataframe
        output.loc[:, col] = X_to_perturb.copy()
        
    return output
    
if __name__ == '__main__':
    
    ## define relative file paths
    # directory with machine learning model input features
    rel_path_input_ML_features = "../../Data/ForModel/ML/" # input features have base EGU emissions
    # directory with fitted machine learning model
    rel_path_input_ML = "../../Data/Fitted Models/"
    # directory with output air pollutants
    rel_path_output_pollutants = "../../Data/Counterfactual Air Pollutants/0. modeled actual"
    
    # sites to run for
    sites = ["SDK", "Bronx", "Manhattan", "Queens"] 
    # all target names to run for
    targetNames = ["pm25", "ozone"] # just PM and ozone for now
    # years to run; must be iterable
    years = range(2006, 2020)
    # number of simulations to run
    n = 5000
    
        
    ## loop through each site and create counterfactual pollutants
    for site in sites:
        os.chdir(base_dname) # change to code directory
        os.chdir(rel_path_input_ML_features+site) # change to model data folder
        # read in all feature data
        X = pd.read_excel(site+'Base.xlsx', sheet_name="X") # read all X data including Date
        
        # loop through each target to create counterfactual pollutants
        for target in targetNames:
            fn = site + "_" + target # file names, except for extension
            
            # select particular X features and y targets needed
            os.chdir(base_dname) # change to code directory
            os.chdir(rel_path_input_ML_features+site) # change to model data folder
            featuresNeeded = pd.read_excel(site+'_features.xlsx', sheet_name=target) # X features needed
            X_forTarget = X.loc[:, np.append(['Date'], featuresNeeded.transpose().values[0])] # select X features needed + 'Date'
        
            # load model
            os.chdir(base_dname)
            os.chdir(rel_path_input_ML + site)
            regressor = joblib.load(fn+"_XGB.json")
            
            # push emissions through model predictions
            y_mc = np.zeros((len(X_forTarget.index), n)) # pre-allocate pollutant concentration
            for i in range(0, n): # loop through number of monte carlo runs (n_col - 1 times to avoid running 'Date' column)
                X_forTarget_temp = X_forTarget.copy()
                
                # perturb mobile and other emissions using +-50% uniform distribution
                # use log normal distributions with sigmas from Hanna et al. 2001
                mobile_columns = [col for col in X_forTarget_temp.columns if 'mobile' in col]
                X_forTarget_temp = perturb_lognormal(X_forTarget_temp, mobile_columns, 0.347)
                other_columns = [col for col in X_forTarget_temp.columns if 'other' in col]
                X_forTarget_temp = perturb_lognormal(X_forTarget_temp, other_columns, 0.203)
                
                # predict output
                y_mc[:, i] = regressor.predict(X_forTarget_temp[[value[0] for value in featuresNeeded.values]])

            output = pd.DataFrame(y_mc, index=X.loc[:, 'Date']) # change to dataframe
            # bin output to daily resolution
            output = bin_daily(output)
            
            # write to table
            os.chdir(base_dname)
            os.chdir(rel_path_output_pollutants)
            output.to_parquet(site+'_'+target+'_'+str(years[0])+'-'+str(years[-1])+'_bin_daily.parquet')