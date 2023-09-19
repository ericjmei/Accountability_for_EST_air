# -*- coding: utf-8 -*-
"""
Created on Fri May 26 12:22:11 2023

calculate pattern in emissions reductions using ratio of short-run and total counterfactuals ([E_short - E_total]/E_total - 1). Calculates this for:
    1. total counterfactual and short-run counterfactual between 2009 and (INSERT PERIOD) (CAIR impacts on medium run)
    2. total counterfactual and (short-run cf + CAIR) (MATS, CSAPR, and economic impact on medium and long-run)

@author: emei3
"""

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from scipy.interpolate import splev, splrep
from datetime import datetime

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

def plot_fit(df_data, df_fit, ylabel):
    """
    plots original dataset and result of fit

    Parameters
    ----------
    df_data : DataFrame
        all the original data. Contains median and lower and upper bounds.
    df_fit : DataFrame
        data fitted to polynomial.

    """
    # Plot the median line
    fig, ax = plt.subplots()
    ax.plot(df_data.index, df_data['median'], color='blue', label='Original Data')
    # Fill the area between lower and upper bounds
    ax.fill_between(df_data.index, df_data['lower_bound'], df_data['upper_bound'], color='gray', 
                    alpha=0.5, label='Range')
    # plot line at 0
    plt.axhline(y=0, color='r', linestyle='-')
    # plot line for fitted polynomial
    ax.plot(df_fit.index, df_fit['median'], label='Fitted Polynomial', color='m', linewidth=5)
    plt.xlabel('Date')
    plt.ylabel(ylabel) 
    plt.legend()
    plt.show()
    
    return fig

def calc_magnitude(df_impact, df_ratio):
    output = pd.DataFrame(index=df_impact.index)
    output['median'] = np.multiply(df_impact['median'], df_ratio['median'])*-1
    output['lower_bound'] = np.multiply(df_impact['lower_bound'], df_ratio['median'])*-1
    output['upper_bound'] = np.multiply(df_impact['upper_bound'], df_ratio['median'])*-1
    return output

def fit_emissions_reduction(df, smoothing_factor):
    """
    fits a 2nd order polynomial to the dataframe entered

    Parameters
    ----------
    df : DataFrame
        slice of larger dataframe with datetime index and a 'median' column.

    Returns
    -------
    coefficients : array
        coefficients of 2nd order fit.
    rmse : float
        rmse of fit.
    emissions_reduction : DataFrame
        dataframe with datetime index and a 'median' column that's parallel to the input df.

    """
    # convert the datetime index to numeric values
    x = pd.to_numeric(df.index)
    y = df['median'].values
    weights = df['std'].values**-1
    # Fit a polynomial of degree 2 (adjust the degree as needed)
    spl = splrep(x, y, k=3, s=smoothing_factor) # k is degree (cubic), and s is smoothing factor
    
    # calculate uncertainties
    y_fit = splev(x, spl)
    residuals = df['median'] - y_fit
    # Calculate the root mean square error (RMSE)
    rmse = np.sqrt(np.mean(residuals ** 2))
    
    ## put into table
    emissions_reduction = pd.DataFrame(index=df.index)
    emissions_reduction['x'] = pd.to_numeric(df.index)
    emissions_reduction['median'] = y_fit = splev(x, spl)
    emissions_reduction['lower_bound'] = emissions_reduction['median'] - 1.96*rmse
    emissions_reduction['upper_bound'] = emissions_reduction['median'] + 1.96*rmse
    
    return spl, rmse, emissions_reduction

if __name__ == '__main__':
    
    ## define relative file paths
    rel_path_emissions_reductions = "../../Data/Emissions Reductions/2. edited" # relative path of emissions reductions
    rel_path_emissions_total_cf = "../../Data/Counterfactual Emissions/8. total cf edited" # relative path of total counterfactual emissions
    rel_path_output = "../../Data/Emissions Reductions/2. edited" # relative path of outputs
    rel_path_output_figures = "../../Data/Emissions Reductions/2. edited/figures" # relative path of output figures
    fn_save_end = ''
    
    ## define input years, regions, states, and whether ozone season should be separated
    years_for_emissions = range(2006, 2020) # specify years to use for ER
    # filename endings to search for when stitching CEMS data
    fn_ends = [['SOCO'], ['NYC']]
    fn_figures = ['ATL', 'NYC']
    # choose start and end years for CAIR impact
    start_CAIR = datetime(2007, 6, 1)
    end_CAIR = datetime(2011, 10, 1)
    # choose start and end date of other impact years
    start_other = datetime(2014, 7, 1)
    end_other = datetime(2020, 1, 1)
    # smoothing factor of splines
    smoothing_factor = 6
    # specify species
    species = 'so2' # nox or so2
    # decide if ozone season should be separated
    separate_ozone_season = False
    
    i = 1
    fn_end = fn_ends[i]
    
    ## import medium- and long-run impacts
    os.chdir(base_dname)
    os.chdir(rel_path_emissions_reductions)
    # import normalized short-run (to total counterfactual)
    fn = species+'_'+'_'.join(fn_end)+'_'+str(years_for_emissions[0])+'-'+str(years_for_emissions[-1])+'_short-run_normalized'
    emissions_short_cf_norm = pd.read_parquet(fn+'.parquet')
    # medium long run impacts
    fn = species+'_'+'_'.join(fn_end)+'_'+str(years_for_emissions[0])+'-'+str(years_for_emissions[-1])+'_medium-long-run'
    impact_medium_long = pd.read_parquet(fn+'.parquet')
    ## retrieve counterfactual emissions
    fn = species+'_'+'_'.join(fn_end)+'_'+str(years_for_emissions[0])+'-'+str(years_for_emissions[-1])+'_bin_daily.parquet'
    # total cf
    os.chdir(base_dname)
    os.chdir(rel_path_emissions_total_cf)
    emissions_total_cf = pd.read_parquet(fn)
    
    ## 1. fit CAIR impact (total - short-run in period of medium-run impact)
    # fit quadratic to medium- and long-run impacts in period identified
    # grab time slice of impact
    emissions_norm_CAIR_slice = emissions_short_cf_norm.loc[start_CAIR:end_CAIR].copy()
    emissions_total_cf_CAIR_slice = emissions_total_cf.loc[start_CAIR:end_CAIR].copy()
    ylabel = r"$\dfrac{E_{short-run}}{E_{total}} - 1$"
    if not separate_ozone_season:
        # fit data and retrieve fit parameters
        coeffs_CAIR, rmse_CAIR, fit_CAIR = fit_emissions_reduction(emissions_norm_CAIR_slice, smoothing_factor)
        ## plot
        fig_CAIR = plot_fit(emissions_short_cf_norm, fit_CAIR, ylabel)
    else: # if ozone season, separate it and fit a different polynomial
        mask = (emissions_norm_CAIR_slice.index.month >= 5) & (emissions_norm_CAIR_slice.index.month <= 9)
        emissions_total_cf_CAIR_slice_ozone_season = emissions_total_cf_CAIR_slice.loc[mask].copy()
        emissions_total_cf_CAIR_slice_non_ozone_season = emissions_total_cf_CAIR_slice.loc[~mask].copy()
        emissions_norm_CAIR_slice_ozone_season = emissions_norm_CAIR_slice.loc[mask].copy()
        emissions_norm_CAIR_slice_non_ozone_season = emissions_norm_CAIR_slice.loc[~mask].copy()
        # fit data and retrieve fit parameters
        coeffs_CAIR_ozone_season, rmse_CAIR_ozone_season, fit_CAIR_ozone_season = fit_emissions_reduction(emissions_norm_CAIR_slice_ozone_season, smoothing_factor)
        coeffs_CAIR_non_ozone_season, rmse_CAIR_non_ozone_season, fit_CAIR_non_ozone_season = fit_emissions_reduction(emissions_norm_CAIR_slice_non_ozone_season, 
                                                                                                                      smoothing_factor)
        ## plot
        fig_CAIR_ozone_season = plot_fit(emissions_short_cf_norm, fit_CAIR_ozone_season, ylabel)
        fig_CAIR_non_ozone_season = plot_fit(emissions_short_cf_norm, fit_CAIR_non_ozone_season, ylabel)
        
    ## 2. fit "other" impact
    # fit quadratic to medium- and long-run impacts in period identified
    # grab time slice of impact
    emissions_norm_other_slice = emissions_short_cf_norm.loc[start_other:end_other].copy()
    emissions_total_cf_other_slice = emissions_total_cf.loc[start_other:end_other].copy()
    if not separate_ozone_season:
        # fit data and retrieve fit parameters
        coeffs_other, rmse_other, fit_other = fit_emissions_reduction(emissions_norm_other_slice, smoothing_factor)
        ## plot
        fig_other = plot_fit(emissions_short_cf_norm, fit_other, ylabel)
    else: # if ozone season, separate it and fit a different polynomial
        mask = (emissions_norm_other_slice.index.month >= 5) & (emissions_norm_other_slice.index.month <= 9)
        emissions_total_cf_other_slice_ozone_season = emissions_total_cf_other_slice.loc[mask].copy()
        emissions_total_cf_other_slice_non_ozone_season = emissions_total_cf_other_slice.loc[~mask].copy()
        emissions_norm_other_slice_ozone_season = emissions_norm_other_slice.loc[mask].copy()
        emissions_norm_other_slice_non_ozone_season = emissions_norm_other_slice.loc[~mask].copy()
        # fit data and retrieve fit parameters
        coeffs_other_ozone_season, rmse_other_ozone_season, fit_other_ozone_season = fit_emissions_reduction(emissions_norm_other_slice_ozone_season, smoothing_factor)
        coeffs_other_non_ozone_season, rmse_other_non_ozone_season, fit_other_non_ozone_season = fit_emissions_reduction(emissions_norm_other_slice_non_ozone_season, 
                                                                                                                         smoothing_factor)
        ## plot
        fig_other_ozone_season = plot_fit(emissions_short_cf_norm, fit_other_ozone_season, ylabel)
        fig_other_non_ozone_season = plot_fit(emissions_short_cf_norm, fit_other_non_ozone_season, ylabel)
        
    
    ## save just the polynomial fits and their figures
    fn = species+'_'+'_'.join(fn_end)+'_'+str(years_for_emissions[0])+'-'+str(years_for_emissions[-1])
    os.chdir(base_dname)
    os.chdir(rel_path_output)
    if not separate_ozone_season:
        fit_CAIR.to_parquet(fn+'_CAIR_reductions_ratios'+fn_save_end+'.parquet')
        fit_other.to_parquet(fn+'_other_reductions_ratios'+fn_save_end+'.parquet')
    else:
        fit_CAIR_ozone_season.to_parquet(fn+'_CAIR_ozone_season_reductions_ratios'+fn_save_end+'.parquet')
        fit_other_ozone_season.to_parquet(fn+'_other_ozone_season_reductions_ratios'+fn_save_end+'.parquet')
        fit_CAIR_non_ozone_season.to_parquet(fn+'_CAIR_non_ozone_season_reductions_ratios'+fn_save_end+'.parquet')
        fit_other_non_ozone_season.to_parquet(fn+'_other_non_ozone_season_reductions_ratios'+fn_save_end+'.parquet')
    # figures
    fn = fn_figures[i]
    os.chdir(base_dname)
    os.chdir(rel_path_output_figures)
    if not separate_ozone_season:
        fig_CAIR.savefig('fig_'+fn+'_CAIR'+fn_save_end+'.png', dpi=fig_CAIR.dpi*10, bbox_inches='tight')
        fig_other.savefig('fig_'+fn+'_other'+fn_save_end+'.png', dpi=fig_other.dpi*10, bbox_inches='tight')
    else:
        fig_CAIR_ozone_season.savefig('fig_'+fn+'_CAIR_ozone_season'+fn_save_end+'.png', dpi=fig_CAIR_ozone_season.dpi*10, bbox_inches='tight')
        fig_other_ozone_season.savefig('fig_'+fn+'_other_ozone_season'+fn_save_end+'.png', dpi=fig_other_ozone_season.dpi*10, bbox_inches='tight')
        fig_CAIR_non_ozone_season.savefig('fig_'+fn+'_CAIR_non_ozone_season'+fn_save_end+'.png', dpi=fig_CAIR_non_ozone_season.dpi*10, bbox_inches='tight')
        fig_other_non_ozone_season.savefig('fig_'+fn+'_other_non_ozone_season'+fn_save_end+'.png', dpi=fig_other_non_ozone_season.dpi*10, bbox_inches='tight')