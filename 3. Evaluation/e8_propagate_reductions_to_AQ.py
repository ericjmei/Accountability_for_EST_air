# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 11:22:49 2023

propagates emissions reductions and uncertainties calculated apportioned to medium- and long-run impacts (CAIR and CSAPR, MATS, and economic)
    in e7 to air quality (total magnitude) and also bins them by day
yes, I know this is a gross combination of three prior scripts. If I have time, I'll combine everything.

@author: emei3
"""

import pandas as pd
import numpy as np
import os
import joblib
from datetime import timedelta

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

# import functions of cf emissions to air pollutant and also bin monte carlo
os.chdir(base_dname)
def add_impact_to_base_emissions(X_forTarget_temp, impact_df, species_target_name, featuresNeeded):
    """
    adds emissions reductions magnitudes with noise into the correct species name within the feature dataset for ML while preserving
    the order of features. Also makes sures datetime values align

    Parameters
    ----------
    X_forTarget_temp : dataframe
        feature dataframe.
    cf_df : dateframe
        counterfactual emissions dataframe.
    species_target_name : string
        name of feature to replace.
    featuresNeeded : dataframe
        dataframe of features needed.

    Returns
    -------
    X_forTarget_temp : dataframe
        feature dataframe with new emissions.

    """
    # create gaussian uncertainty about the impact median using the standard deviation
    impact_mc = np.random.normal(loc=impact_df['median'], scale=impact_df['std'])
    # mask for days that are in the dataframe
    mask = impact_df['Date'].isin(X_forTarget_temp['Date'])
    impact_mc = impact_mc[mask]
    # add impact to target
    X_forTarget_temp.loc[:, species_target_name] = X_forTarget_temp.loc[:, species_target_name] + impact_mc
    return X_forTarget_temp

# pads dataframe with leading 0s and dates from first_date to the first date of the original dataframe
def pad_impact_dataframe(df_original, first_date):
    # create leading datetime column
    first_date_original = pd.to_datetime(df_original['Date'].min())
    dates = pd.date_range(start=first_date, end=(first_date_original - timedelta(days=1)), freq='D')
    new_rows = pd.DataFrame({'Date':dates})
    # pad rest of columns with 0
    for col in df_original.select_dtypes(include=['number']).columns:
        new_rows[col] = 0
    new_df = pd.concat([new_rows, df_original])
    new_df = new_df.sort_values(by='Date').reset_index(drop=True)  # Sort by the datetime column and reset index
    return new_df

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
        
def create_mc_AQ(X, X_forTarget, so2_impact, nox_impact, species_features_all, regressor):
    """
    poorly written function that pushes CAIR or other impact through ML model

    Parameters
    ----------
    X : dataframe
        all feature data include date
    X_forTarget : dataframe
        dataframe consistent with features used for ML model.
    so2_impact : dataframe
        factor impact dataframe with median, upper bound, and lower bound.
    nox_impact : dataframe
        factor impact dataframe with median, upper bound, and lower bound.
    species_features_all : list
        poorly designed list with name of so2 [0] and nox [1] column in X_forTarget.
    regressor : XGBoost model

    Returns
    -------
    None.

    """
    
    y_mc = np.zeros((len(X_forTarget.index), n)) # pre-allocate pollutant concentration
    for i in range(0, n): # loop through number of monte carlo runs (n_col - 1 times to avoid running 'Date' column)
        X_forTarget_temp = X_forTarget.copy()
        
        # if so2 EGU is in features needed, replace the observed with the monte carlo simulated
        if species_features_all[0] in featuresNeeded.values:
            X_forTarget_temp = add_impact_to_base_emissions(X_forTarget_temp, so2_impact, species_features_all[0], featuresNeeded)
            
        # if nox EGU is in features needed, replace the observed with the monte carlo simulated
        if species_features_all[1] in featuresNeeded.values:
            X_forTarget_temp = add_impact_to_base_emissions(X_forTarget_temp, nox_impact, species_features_all[1], featuresNeeded)
        
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
    
    return output

    
if __name__ == '__main__':
    
    ## define relative file paths
    # directory with emissions impacts
    rel_path_emissions_reductions = "../../Data/Emissions Reductions/2. edited" # relative path of emissions reductions
    # directory with machine learning model input features
    rel_path_input_ML_features = "../../Data/ForModel/ML/" # input features have base EGU emissions
    # directory with fitted machine learning model
    rel_path_input_ML = "../../Data/Fitted Models/"
    # directory with output air pollutants
    rel_path_output_pollutants = "../../Data/Counterfactual Air Pollutants/7. ba regions edited"
    fn_save_end = ''
    
    # sites to run for
    groups_of_sites = [["SDK"], # all sites in Atlanta
        ["Bronx", "Manhattan", "Queens"]] # all sites in NYC
    # all target names to run for
    targetNames = ["pm25", "ozone"] # just PM and ozone for now
    # groups of states to run for; must be parallel to emissions used for "sites"
    fn_ends = [['SOCO'], ['NYC']]
    # years to run; must be iterable
    years = range(2006, 2020)
    # number of simulations to run
    n = 5000
    
    ## push emissions reductions into counterfactul air pollutants
    for i, sites in enumerate(groups_of_sites):
        fn_end = fn_ends[i]
        
        ## retrieve emssions reductions
        os.chdir(base_dname) # change to code directory
        os.chdir(rel_path_emissions_reductions) # change to emissions directory
        fn = '_'.join(fn_end)+'_'+str(years[0])+'-'+str(years[-1])
        # CAIR impacts
        so2_CAIR_impact = pd.read_parquet('so2_'+fn+'_CAIR_reductions'+fn_save_end+'.parquet')
        nox_CAIR_impact = pd.read_parquet('nox_'+fn+'_CAIR_reductions'+fn_save_end+'.parquet')
        # other impacts
        so2_other_impact = pd.read_parquet('so2_'+fn+'_other_reductions'+fn_save_end+'.parquet')
        nox_other_impact = pd.read_parquet('nox_'+fn+'_other_reductions'+fn_save_end+'.parquet')
        
        ## pad impacts with leading 0s to make consistent with machine learning model
        first_date = str(years[0])+'-01-01'
        so2_CAIR_impact = pad_impact_dataframe(so2_CAIR_impact, first_date)
        nox_CAIR_impact = pad_impact_dataframe(nox_CAIR_impact, first_date)
        so2_other_impact = pad_impact_dataframe(so2_other_impact, first_date)
        nox_other_impact = pad_impact_dataframe(nox_other_impact, first_date)
        
        ## loop through each site and create counterfactual pollutants
        for site in sites:
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
                    # CAIR
                    AQ_CAIR = create_mc_AQ(X, X_forTarget, so2_CAIR_impact, nox_CAIR_impact, species_features_all, regressor)
                    # other
                    AQ_other = create_mc_AQ(X, X_forTarget, so2_other_impact, nox_other_impact, species_features_all, regressor)
                    
                    # write to table
                    os.chdir(base_dname)
                    os.chdir(rel_path_output_pollutants)
                    AQ_CAIR.to_parquet(site+'_'+target+'_'+str(years[0])+'-'+str(years[-1])+'_CAIR_bin_daily.parquet')
                    AQ_other.to_parquet(site+'_'+target+'_'+str(years[0])+'-'+str(years[-1])+'_other_bin_daily.parquet')