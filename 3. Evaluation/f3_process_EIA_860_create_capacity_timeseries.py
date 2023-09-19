# -*- coding: utf-8 -*-
"""
Created on Sun Jun 11 13:06:21 2023

processes EIA 860m form to find capacity installed and removed

@author: emei3
"""

import pandas as pd
import os
import numpy as np

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

def sum_installments(df, years):
    """
    sums all generator installments in df over the years sepcified
    """
    
    # mask for df installments within years
    mask = (df['operating_year'] >= years[0]) & (df['operating_year'] <= years[-1])
    output_df = df.loc[mask, :].copy()
    
    # sort installments by year and month
    output_df.sort_values(['operating_year', 'operating_month'], inplace=True)
    
    ## sum by month and year
    # create output dataframe
    years = np.arange(years[0], years[-1] + 1)
    months = np.arange(1, 13)
    year_mesh, month_mesh = np.meshgrid(years, months)
    output = pd.DataFrame({
        'year': year_mesh.ravel(),
        'month': month_mesh.ravel(),
    })
    output.sort_values(['year', 'month'], inplace=True)
    # sum data
    summed_data = output_df.groupby(['operating_year', 'operating_month'])['mw'].sum().reset_index()
    summed_data.rename(columns={'operating_year':'year', 'operating_month':'month'}, inplace=True)
    # merge with output dataframe
    output = pd.merge(output, summed_data, on=['year', 'month'], how='left')
    output['mw'].fillna(0, inplace=True)
    
    return output_df, output

def sum_retirements(df, years):
    """
    sums all generator retirements in df over the years sepcified
    """
    
    # mask for df installments within years
    mask = (df['retirement_year'] >= years[0]) & (df['retirement_year'] <= years[-1])
    output_df = df.loc[mask, :].copy()
    
    # sort installments by year and month
    output_df.sort_values(['retirement_year', 'retirement_month'], inplace=True)
    
    # sum by month and year
    output = output_df.groupby(['retirement_year', 'retirement_month'])['mw'].sum()
    
    ## sum by month and year
    # create output dataframe
    years = np.arange(years[0], years[-1] + 1)
    months = np.arange(1, 13)
    year_mesh, month_mesh = np.meshgrid(years, months)
    output = pd.DataFrame({
        'year': year_mesh.ravel(),
        'month': month_mesh.ravel(),
    })
    output.sort_values(['year', 'month'], inplace=True)
    # sum data
    summed_data = output_df.groupby(['retirement_year', 'retirement_month'])['mw'].sum().reset_index()
    summed_data.rename(columns={'retirement_year':'year', 'retirement_month':'month'}, inplace=True)
    # merge with output dataframe
    output = pd.merge(output, summed_data, on=['year', 'month'], how='left')
    output['mw'].fillna(0, inplace=True)
    
    return output_df, output
    
if __name__ == '__main__':
    
    ## define relative file paths
    # directory with machine learning model input features
    rel_path_input_EIA = "../../Data/EIA/860" # input features have base EGU emissions
    # directory for output merged and cleaned dataframe
    rel_path_output_plant_characteristics = "../../Data/plant characteristics"
    
    # define groups of balancing authority regions to run for
    ba_region_groups = [['SOCO'], ['PJM', 'ISNE', 'NYIS']]
    region_names = ['ATL', 'NYC']
    # define years to run for
    years_all = range(2006, 2020)
    
    ## import EIA 860 data
    os.chdir(base_dname)
    os.chdir(rel_path_input_EIA)
    df_EIA_operating_raw = pd.read_excel('january_generator2020.xlsx', sheet_name="Operating", skiprows=1)
    df_EIA_retired_raw = pd.read_excel('january_generator2020.xlsx', sheet_name="Retired", skiprows=1)
    
    #%% clean data by dropping unneeded columns and rows
    i = 0 # define group to run for
    
    ## drop un-needed columns
    df_EIA_operating = df_EIA_operating_raw[['Plant ID', 'Generator ID', 'Balancing Authority Code', 'Energy Source Code', 'Prime Mover Code', 
                                             'Nameplate Capacity (MW)', 'Operating Month', 'Operating Year', 'Plant State', 'Plant Name']]
    df_EIA_retired = df_EIA_retired_raw[['Plant ID', 'Generator ID', 'Balancing Authority Code', 'Energy Source Code', 'Prime Mover Code', 
                                        'Nameplate Capacity (MW)', 'Operating Month', 'Operating Year', 'Retirement Month', 'Retirement Year', 
                                        'Plant State', 'Plant Name']]
    
    ## re-name columns
    df_EIA_operating = df_EIA_operating.rename(columns={'Plant ID':'orispl', 'Generator ID':'unit', 'Balancing Authority Code':'ba', 'Energy Source Code':'fuel_type',
                                     'Prime Mover Code':'prime_mover', 'Nameplate Capacity (MW)':'mw', 'Operating Month':'operating_month', 
                                     'Operating Year':'operating_year', 'Plant State':'state', 'Plant Name':'name'}).copy()
    df_EIA_retired = df_EIA_retired.rename(columns={'Plant ID':'orispl', 'Generator ID':'unit', 'Balancing Authority Code':'ba', 'Energy Source Code':'fuel_type',
                                     'Prime Mover Code':'prime_mover', 'Nameplate Capacity (MW)':'mw', 'Operating Month':'operating_month', 
                                     'Operating Year':'operating_year', 'Retirement Month':'retirement_month', 'Retirement Year':'retirement_year', 
                                     'Plant State':'state', 'Plant Name':'name'}).copy()
    
    ## filter by balancing authority
    mask = df_EIA_operating['ba'].isin(ba_region_groups[i])
    df_EIA_operating = df_EIA_operating.loc[mask, :].copy()
    mask = df_EIA_retired['ba'].isin(ba_region_groups[i])
    df_EIA_retired = df_EIA_retired.loc[mask, :].copy()
    
    ## combine dataframes
    # add retirement month and year to df_EIA_operating
    missing_columns = [col for col in df_EIA_retired.columns if col not in df_EIA_operating.columns]
    for col in missing_columns:
        df_EIA_operating[col] = pd.Series(dtype='float64')
    df_EIA_operating = df_EIA_operating[df_EIA_retired.columns]
    # combine dataframes
    df_EIA = pd.concat([df_EIA_operating, df_EIA_retired], axis=0)
    
    ### identify capacity trends (from both operating and retired sheets)
    ## do for net NG CC and CT
    # mask for ng generators with CT and CC
    mask = (df_EIA['fuel_type'] == 'NG') & (df_EIA['prime_mover'].isin(['CC', 'CT']))
    df_EIA_subset_ng = df_EIA.loc[mask, :]
    # identify installments
    ng_units_installed, ng_installed_trend = sum_installments(df_EIA_subset_ng, years_all)
    # identify retirements
    ng_units_retired, ng_retired_trend = sum_retirements(df_EIA_subset_ng, years_all)
    # net installments trend is installed - retired
    ng_net_installed_trend = ng_installed_trend.copy()
    ng_net_installed_trend.loc[:, 'mw'] = ng_installed_trend.loc[:, 'mw'] - ng_retired_trend.loc[:, 'mw']
    ## identify net trend
    # identify total capacity at beginning
    mask = (df_EIA_subset_ng['operating_year'] < years_all[0]) & ~(df_EIA_subset_ng['retirement_year'] < years_all[0])
    beginning_capacity = df_EIA_subset_ng.loc[mask, 'mw'].sum()
    # copy over net installed trend
    ng_trend = ng_net_installed_trend.copy()
    # do cumulative sum to calculate true trend over time
    ng_trend['mw'] = ng_trend['mw'].cumsum() + beginning_capacity
    
    ## do for net coal
    # mask for ng generators with CT and CC
    mask = df_EIA['fuel_type'].isin(['LIG', 'BIT', 'SUB', 'RC', 'WC', 'ANT'])
    df_EIA_subset_coal = df_EIA.loc[mask, :]
    # identify installments
    coal_units_installed, coal_installed_trend = sum_installments(df_EIA_subset_coal, years_all)
    # identify retirements
    coal_units_retired, coal_retired_trend = sum_retirements(df_EIA_subset_coal, years_all)
    # net trend is installed - retired
    coal_net_installed_trend = coal_installed_trend.copy()
    coal_net_installed_trend.loc[:, 'mw'] = coal_installed_trend.loc[:, 'mw'] - coal_retired_trend.loc[:, 'mw']
    ## identify net trend
    # identify total capacity at beginning
    mask = (df_EIA_subset_coal['operating_year'] < years_all[0]) & ~(df_EIA_subset_coal['retirement_year'] < years_all[0])
    beginning_capacity = df_EIA_subset_coal.loc[mask, 'mw'].sum()
    # copy over net installed trend
    coal_trend = coal_net_installed_trend.copy()
    # do cumulative sum to calculate true trend over time
    coal_trend['mw'] = coal_trend['mw'].cumsum() + beginning_capacity
    
    ## just for fun, divide the two trends
    ng_coal_ratio = coal_trend[['year', 'month']].copy()
    ng_coal_ratio['ratio'] = np.divide(ng_trend['mw'], coal_trend['mw'])
    
    ## calculate trend as ratio of total fossil
    # mask for total fossil fuels
    mask = df_EIA['fuel_type'].isin(['LIG', 'BIT', 'SUB', 'RC', 'WC', 'ANT', 'DFO', 'JF', 'KER', 
                                     'PC', 'PG', 'RFO', 'SGP', 'WO', 'SGC', 'BFG', 'NG', 'H2', 'OG'])
    df_EIA_subset_fossil = df_EIA.loc[mask, :]
    # identify installments
    temp, fossil_installed_trend = sum_installments(df_EIA_subset_fossil, years_all)
    # identify retirements
    temp, fossil_retired_trend = sum_retirements(df_EIA_subset_fossil, years_all)
    # net trend is installed - retired
    fossil_net_installed_trend = fossil_installed_trend.copy()
    fossil_net_installed_trend.loc[:, 'mw'] = fossil_installed_trend.loc[:, 'mw'] - fossil_retired_trend.loc[:, 'mw']
    ## identify net trend
    # identify total capacity at beginning
    mask = (df_EIA_subset_fossil['operating_year'] < years_all[0]) & ~(df_EIA_subset_fossil['retirement_year'] < years_all[0])
    beginning_capacity = df_EIA_subset_fossil.loc[mask, 'mw'].sum()
    # copy over net installed trend
    fossil_trend = fossil_net_installed_trend.copy()
    # do cumulative sum to calculate true trend over time
    fossil_trend['mw'] = fossil_trend['mw'].cumsum() + beginning_capacity
    
    ## find portion of coal and NG as portion of total fossil
    ng_fossil_ratio = fossil_trend[['year', 'month']].copy()
    ng_fossil_ratio['ratio'] = np.divide(ng_trend['mw'], fossil_trend['mw'])
    coal_fossil_ratio = fossil_trend[['year', 'month']].copy()
    coal_fossil_ratio['ratio'] = np.divide(coal_trend['mw'], fossil_trend['mw'])
    
    ## write to file
    os.chdir(base_dname)
    os.chdir(rel_path_output_plant_characteristics)
    with pd.ExcelWriter('capacity_trends_'+ region_names[i] +'.xlsx') as writer:
        # ng
        ng_trend.to_excel(writer, sheet_name='ng trend', index=False)
        ng_net_installed_trend.to_excel(writer, sheet_name='ng net installed trend', index=False)
        ng_fossil_ratio.to_excel(writer, sheet_name='ng to fossil ratio', index=False)
        # coal
        coal_trend.to_excel(writer, sheet_name='coal trend', index=False)
        coal_net_installed_trend.to_excel(writer, sheet_name='coal net installed trend', index=False)
        coal_fossil_ratio.to_excel(writer, sheet_name='coal to fossil ratio', index=False)
        # total fossil trend
        fossil_trend.to_excel(writer, sheet_name='fossil trend', index=False)
        fossil_net_installed_trend.to_excel(writer, sheet_name='fossil net installed trend', index=False)
        # ng to coal ratio
        ng_coal_ratio.to_excel(writer, sheet_name='ng to coal ratio', index=False)