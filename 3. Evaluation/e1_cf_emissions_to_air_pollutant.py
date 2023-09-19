# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 11:53:54 2023

plugs in each Monte Carlo emissions output into ML XGB model; saves all outputs as parquet file

@author: emei3
"""

import numpy as np
import pandas as pd
import os
import joblib

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

def insert_emissions_into_features(X_forTarget_temp, cf_df, species_target_name, monte_carlo_column, featuresNeeded):
    """
    inserts monte carlo emissions column into the correct species name within the feature dataset for ML while preserving
    the order of features. Also makes sures datetime values align

    Parameters
    ----------
    X_forTarget_temp : dataframe
        feature dataframe.
    cf_df : dateframe
        counterfactual emissions dataframe.
    species_target_name : string
        name of feature to replace.
    monte_carlo_column : string
        name of counterfactual emissions column to insert.
    featuresNeeded : dataframe
        dataframe of features needed.

    Returns
    -------
    X_forTarget_temp : dataframe
        feature dataframe with new emissions.

    """
    cf_df_temp = cf_df.reset_index()
    X_forTarget_temp = pd.merge(X_forTarget_temp, cf_df_temp[['Date', monte_carlo_column]], on='Date', how='left') # make dataframes consistent
    X_forTarget_temp.drop(columns=[species_target_name], inplace=True) # drop old column
    X_forTarget_temp.rename(columns={monte_carlo_column: species_target_name}, inplace=True) # rename new column as old column
    X_forTarget_temp = X_forTarget_temp[np.append(['Date'], [value[0] for value in featuresNeeded.values])] # rearrange columns to original order
    return X_forTarget_temp

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
    # directory with counterfactual emissions
    rel_path_input_emissions = "../../Data/Counterfactual Emissions/7. ba regions edited"
    # directory with machine learning model input features
    rel_path_input_ML_features = "../../Data/ForModel/ML/"
    # directory with fitted machine learning model
    rel_path_input_ML = "../../Data/Fitted Models/" 
    # directory with output air pollutants
    rel_path_output_pollutants = "../../Data/Counterfactual Air Pollutants/7. ba regions edited"
    
    # sites to run for
    groups_of_sites = [["SDK"], # all sites in Atlanta
        ["Bronx", "Manhattan", "Queens"]] # all sites in NYC
    # all target names to run for
    targetNames = ["pm25", "ozone"] # just PM and ozone for now
    # groups of states to run for; must be parallel to emissions used for "sites"
    fn_ends = [['SOCO'], ['NYC']]
    # years to run; must be iterable
    years = range(2006, 2020)
    
    # repeat for each site
    for i, sites in enumerate(groups_of_sites):
        fn_end = fn_ends[i]
        
        ## retrieve counterfactual monte carlo emissions
        os.chdir(base_dname) # change to code directory
        os.chdir(rel_path_input_emissions) # change to emissions directory
        so2_cf = pd.read_parquet('so2_'+'_'.join(fn_end)+'_'+str(years[0])+'-'+str(years[-1])+'.parquet')
        nox_cf = pd.read_parquet('nox_'+'_'.join(fn_end)+'_'+str(years[0])+'-'+str(years[-1])+'.parquet')
        
        ## loop through each site and create counterfactual pollutants
        for site in sites:
            ## create output pollutant concentrations
            os.chdir(base_dname) # change to code directory
            os.chdir(rel_path_input_ML_features+site) # change to model data folder
            # read in all feature data
            X = pd.read_excel(site+'Base.xlsx', sheet_name="X") # read all X data including Date
            # retrieve so2 and nox feature names used in ML models
            if site in ['Bronx', 'Manhattan', 'Queens']:
                species_features_all = ['SO2EGUtot', 'NOxEGUtot']
            else:
                species_features_all = ['SO2EGU', 'NOxEGU']
            
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
                if any(featuresNeeded.isin(species_features_all).values): # only run if so2 or nox in model
                    y_mc = np.zeros((len(X_forTarget.index), len(so2_cf.columns))) # pre-allocate pollutant concentration
                    for i in range(0, len(so2_cf.columns)): # loop through number of monte carlo runs (n_col - 1 times to avoid running 'Date' column)
                        X_forTarget_temp = X_forTarget.copy()
                        
                        # if so2 EGU is in features needed, replace the observed with the monte carlo simulated
                        if species_features_all[0] in featuresNeeded.values:
                            X_forTarget_temp = insert_emissions_into_features(X_forTarget_temp, so2_cf, species_features_all[0], 'column_'+str(i), featuresNeeded)
                            
                        # if nox EGU is in features needed, replace the observed with the monte carlo simulated
                        if species_features_all[1] in featuresNeeded.values:
                            X_forTarget_temp = insert_emissions_into_features(X_forTarget_temp, nox_cf, species_features_all[1], 'column_'+str(i), featuresNeeded)
                        
                        # perturb mobile and other emissions using +-50% uniform distribution
                        # use log normal distributions with sigmas from Hanna et al. 2001
                        mobile_columns = [col for col in X_forTarget_temp.columns if 'mobile' in col]
                        X_forTarget_temp = perturb_lognormal(X_forTarget_temp, mobile_columns, 0.347)
                        other_columns = [col for col in X_forTarget_temp.columns if 'other' in col]
                        X_forTarget_temp = perturb_lognormal(X_forTarget_temp, other_columns, 0.203)
                        
                        # predict output
                        y_mc[:, i] = regressor.predict(X_forTarget_temp[[value[0] for value in featuresNeeded.values]])
                
                    output = pd.DataFrame(y_mc, index=X.loc[:, 'Date']) # change to dataframe
                    column_names = ['column_' + str(i) for i in range(0, len(so2_cf.columns))] # rename columns for easier saving
                    output.rename(columns=dict(zip(output.columns, column_names)), inplace=True)
                    
                    # write to table
                    os.chdir(base_dname)
                    os.chdir(rel_path_output_pollutants)
                    output.to_parquet(site+'_'+target+'_'+str(years[0])+'-'+str(years[-1])+'.parquet')