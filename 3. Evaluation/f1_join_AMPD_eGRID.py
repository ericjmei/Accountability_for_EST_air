# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 13:39:24 2023

joins AMPD with eGRID and generator data objects to understand generator characteristics

@author: emei3
"""

import pandas as pd
import re
import os
import pickle

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

def controls_install_year(orispl_units, years, controls, first_year, series_name):
    """
    identifies the first year a control is installed. series should be sorted by orispl_unit and year prior to method use

    Parameters
    ----------
    orispl_units : series
        orispl and unit names.
    years : series
        year in the AMPD.
    controls : series
        1 if controls exist for orispl_unit for year.
    first_year : int
        first year of the AMPD data (as a boundary condition).
    series_name : string
        name of the column to be outputted.

    Returns
    -------
    None.

    """
    
    # instantiate output series; nan rows go unfilled unless it's the installation year
    output = pd.Series(dtype='float64', index=orispl_units.index, name=series_name)
    
    # set installation year to 1 if it fills the following conidtions
    output.loc[(controls.shift() != 1) &  # Controls in prior row does not have 1
    (controls == 1) &  # Current row has 1
    (orispl_units.shift() == orispl_units) &  # Prior row has the same orispl_unit
    (years != first_year),  # Current row's 'year' is not first_year
    ] = 1
    
    return output

if __name__ == '__main__':
    
    ## define relative file paths
    # directory with machine learning model input features
    rel_path_input_AMPD = "../../Data/air markets program data/" # input features have base EGU emissions
    # directory with eGRID data
    rel_path_input_eGRID = "../../Data/Simple Dispatch Inputs"
    # directory with generator data
    rel_path_gen_data = "../../Data/Simple Dispatch Outputs/2023-05-10 act ba coal propagated/Generator Data"
    # directory for output merged and cleaned dataframe
    rel_path_output_pollutants = rel_path_input_AMPD
    
    # define balancing authority regions to run for
    ba_regions = ['SOCO', 'PJM', 'ISNE', 'NYIS']
    
    
    ## import and clean AMPD
    os.chdir(base_dname)
    os.chdir(rel_path_input_AMPD)
    df_AMPD_raw = pd.read_csv('facility_attributes_raw.csv')
    # retain only needed columns
    df_AMPD_raw.loc[:,'orispl_unit'] = df_AMPD_raw.loc[:,'Facility ID'].astype(str) + '_' + df_AMPD_raw.loc[:,'Unit ID']
    df_AMPD = df_AMPD_raw[['orispl_unit', 'Year', 'Facility Name', 'Primary Fuel Type', 'SO2 Controls', 'NOx Controls', 'NERC Region', 
                           'State', 'Commercial Operation Date', 'Operating Status']]
    df_AMPD = df_AMPD.rename(columns={'Year':'year', 'Facility Name':'name', 'Primary Fuel Type':'fuel_type', 'SO2 Controls':'so2_controls', 'NOx Controls':'nox_controls',
                            'State':'state', 'Commercial Operation Date':'date_online', 'Operating Status':'status'}).copy()
    
    ### import, clean, and merge eGRID data with AMPD
    os.chdir(base_dname)
    os.chdir(rel_path_input_eGRID)
    # year groups for egrid runs
    year_eGRID = [2006, 2007, 2009, 2010, 2012, 2014, 2016, 2018, 2019]
    fns_eGRID = ['egrid2005_data', 'egrid2007_data', 'egrid2009_data', 'egrid2010_data', 'egrid2012_data',
                'egrid2014_data', 'egrid2016_data', 'egrid2018_data', 'egrid2019_data']
    # years for generator data
    year_gendata = range(2006, 2020)
    
    # loop through each eGRID and append necessary data
    for i, fn_eGRID in enumerate(fns_eGRID):
            
        ## cleaning copied from Simple Dispatch
        egrid_unt = pd.read_parquet(fn_eGRID+'_UNT.parquet')
        egrid_gen = pd.read_parquet(fn_eGRID+'_GEN.parquet')
        egrid_plnt = pd.read_parquet(fn_eGRID+'_PLNT.parquet')
        
        ##unit-level data: prime mover type, fuel type, heat input, NOx, SO2, CO2, and hours online
        df = egrid_unt.copy(deep=True)
        #rename columns
        df = df[['PNAME', 'ORISPL', 'UNITID', 'PRMVR', 'FUELU1', 'HTIAN', 'NOXAN', 'SO2AN', 'CO2AN', 'HRSOP']]
        df.columns = ['gen', 'orispl', 'unit', 'prime_mover', 'fuel', 'mmbtu_ann', 'nox_ann', 'so2_ann', 'co2_ann', 'hours_on']
        df['orispl_unit'] = df.orispl.map(str) + '_' + df.unit.map(str) #orispl_unit is a unique tag for each generator unit
        #drop nan fuel
        df = df[~df.fuel.isna()]
        
        ##gen-level data: contains MW capacity and MWh annual generation data, generator fuel, generator online year
        df_gen = egrid_gen.copy(deep=True) 
        df_gen['orispl_unit'] = df_gen['ORISPL'].map(str) + '_' + df_gen['GENID'].map(str) #orispl_unit is a unique tag for each generator unit
        # create two different dataframes for generator: one short and one long
        df_gen_long = df_gen[['ORISPL', 'GENID', 'NAMEPCAP', 'GENNTAN', 'GENYRONL', 'orispl_unit', 'PRMVR', 'FUELG1']].copy()
        df_gen_long.columns = ['orispl', 'unit', 'mw', 'mwh_ann', 'year_online', 'orispl_unit', 'prime_mover', 'fuel_type']
        df_gen = df_gen[['NAMEPCAP', 'GENNTAN', 'GENYRONL', 'orispl_unit']] # short
        df_gen.columns = ['mw', 'mwh_ann', 'year_online', 'orispl_unit']
        
        ##plant-level data: contains fuel, fuel_type, balancing authority, nerc region, and egrid subregion data
        df_plnt = egrid_plnt.copy(deep=True) 
        # grab unique fuel types
        df_plnt_fuel = df_plnt[['PLPRMFL', 'PLFUELCT']] # plant primary fuel and plant primary fuel category
        df_plnt_fuel = df_plnt_fuel.drop_duplicates('PLPRMFL')
        df_plnt_fuel.PLFUELCT = df_plnt_fuel.PLFUELCT.str.lower()
        df_plnt_fuel.columns = ['fuel', 'fuel_type']
        # grab all geography
        # NOTE: 2005, no BACODE (balancing authority code)
        df_plnt = df_plnt[['ORISPL', 'PSTATABB', 'BACODE', 'NERC', 'SUBRGN']]
        df_plnt.columns = ['orispl', 'state', 'ba', 'nerc', 'egrid']
       
        ## merge these egrid data together at the unit-level
        # change here from simple dispatch - sometimes unit and generator level orispl_unit identifiers are not 1 to 1. We ensure as many as possible are paired
        df = df.merge(df_gen, how='left', on='orispl_unit')
        ## merge rest of dataframe
        df = df.merge(df_plnt, how='left', on='orispl')  
        df = df.merge(df_plnt_fuel, how='left', on='fuel')  
        
        ## merge egrid into the AMPD data
        # since egrid data don't occur every year, subset all years including the current eGRID year and future years from AMPD
        # this is to ensure no blank data occur
        df_merge_subset = df_AMPD.loc[df_AMPD['year'] >= year_eGRID[i], :].copy()
        df_merge_subset = df_merge_subset.reset_index() # retain original AMPD index for df_merge
        # merge eGRID data to AMPD data
        df_merge_subset = df_merge_subset.merge(df.drop_duplicates(subset=['orispl_unit']), how='left', on='orispl_unit')
        df_merge_subset = df_merge_subset.set_index('index') # retain orginal AMPD index
        if i == 0:
            df_merge = df_merge_subset # if first loop through, df_merge is the entire subsetted dataframe
        else:
            df_merge.loc[df_merge_subset.index] = df_merge_subset # for later merges, file the data into df_merge based on index
    
    # mask for ba regions needed
    mask = df_merge['ba'].isin(ba_regions)
    df_AMPD = df_merge.loc[mask, :].copy()
    
    ### clean merged data
    ## remove un-needed columns
    df_AMPD = df_AMPD.drop(columns = ['gen', 'orispl', 'unit', 'mmbtu_ann', 'co2_ann', 'hours_on', 'year_online'])
    # remove duplicate fuel type and state
    # we keep eGRID to be consistent with Simple Dispatch
    df_AMPD = df_AMPD.drop(columns=['fuel_type_x', 'state_x'])
    df_AMPD = df_AMPD.rename(columns={'fuel_type_y':'fuel_type', 'state_y':'state'})
    # remove rows with 'Retired' status
    mask = df_AMPD['status'].str.contains('Retired', case=False)
    df_AMPD.drop(df_AMPD.loc[mask].index, inplace=True)
    ## turn controls columns into different columns
    # all SO2 controls listed are a form of Flue Gas Desulfurization (for SOCO, PJM, NYIS, and ISNE)
    # controls column doesn't list changes to fuel type, such as lower amounts of sulfur
    mask = df_AMPD['so2_controls'].isna()
    df_AMPD.loc[~mask, 'so2_FGD'] = 1
    # split NOx controls into Selective Catalytic Reduction (most effective) and "any" (any NOx controls at all)
    # do "any" first 
    mask = df_AMPD['nox_controls'].isna()
    df_AMPD.loc[~mask, 'nox_any'] = 1
    # find SCRs
    mask = df_AMPD['nox_controls'].fillna('nan').str.contains('Selective Catalytic Reduction', case=False)
    mask = mask | df_AMPD['nox_controls'].fillna('nan').str.contains('Selective Non-catalytic Reduction', case=False)
    df_AMPD.loc[mask, 'nox_SCR'] = 1
    
    ### merge with generator data to get observed max mw capacity
    os.chdir(base_dname)
    os.chdir(rel_path_gen_data)
    
    for run_year in year_gendata:
        # import and concatenate all generator data objects
        generators_year = pd.DataFrame() # instantiate generator data
        for region in ba_regions:
            gd_short = pickle.load(open('generator_data_short_%s_%s.obj'%(region, str(run_year)), 'rb')) # load generatordata object
            generators_year = pd.concat([generators_year, gd_short['df']], axis=0) # load generator data dataframe
        generators_year = generators_year.rename(columns={'mw':'mw_obs'})
        
        ## merge egrid into the AMPD data
        # since egrid data don't occur every year, subset all years including the current eGRID year and future years from AMPD
        # this is to ensure no blank data occur
        df_merge_subset = df_AMPD.loc[df_AMPD['year'] >= run_year, :].copy()
        df_merge_subset = df_merge_subset.reset_index() # retain original AMPD index for df_merge
        # merge generator data to AMPD data
        df_merge_subset = df_merge_subset.merge(generators_year[['orispl_unit', 'mw_obs']], how='left', on='orispl_unit')
        df_merge_subset = df_merge_subset.set_index('index') # retain orginal AMPD index
        if run_year == 2006:
            df_merge = df_merge_subset # if first loop through, df_merge is the entire subsetted dataframe
        else:
            df_merge.loc[df_merge_subset.index] = df_merge_subset # for later merges, file the data into df_merge based on index
    
    df_AMPD = df_merge.copy()
    # # use observed unless there is no data available
    # df_AMPD['mw'] = df_AMPD['mw_obs'].fillna(df['mw'])
    # use gendata mw instead of egrid mw
    df_AMPD['mw_egrid'] = df_AMPD['mw'] # file away egrid mw
    df_AMPD['mw'] = df_AMPD['mw_obs']
    
    ## create columns indicating first year of FGD, SCR, or operation. Also list year of retirement
    # sort dataframe by orispl_unit and year
    df_AMPD.sort_values(['orispl_unit', 'year'], inplace=True)
    df_AMPD.reset_index(inplace=True, drop=True)
    # first year of FGD
    df_AMPD.loc[:, 'so2_FGD_install'] = controls_install_year(df_AMPD['orispl_unit'], df_AMPD['year'], df_AMPD['so2_FGD'], 2005, 'so2_FGD_install')
    # first year of any NOx controls
    df_AMPD.loc[:, 'nox_any_install'] = controls_install_year(df_AMPD['orispl_unit'], df_AMPD['year'], df_AMPD['nox_any'], 2005, 'nox_any_install')
    # first year of SCR
    df_AMPD.loc[:, 'nox_SCR_install'] = controls_install_year(df_AMPD['orispl_unit'], df_AMPD['year'], df_AMPD['nox_SCR'], 2005, 'nox_SCR_install')
    # first year of operation; only 100 rows have nan date online and they don't matter for our purposes
    mask = df_AMPD['year'] == df_AMPD['date_online'].fillna('1').str.split('-', n=1).str[0].astype(int)
    df_AMPD.loc[mask, 'year_online'] = 1
    # year of retirement
    # NOTE: different requirement for oil and gas is that EGUs will change between the two. Sometimes, CMPD and eGRID will disagree 
    df_AMPD.loc[
    (df_AMPD['orispl_unit'].shift(-1) != df_AMPD['orispl_unit']) &  # Next row does not have the same 'orispl_unit'
    (df_AMPD['year'] != 2020)  # Current row's 'year' is not 2019
    , 'year_retired'
    ] = 1
    # sort columns to put most needed info first
    needed_cols = ['orispl_unit', 'year', 'fuel_type', 'prime_mover', 'year_online', 'year_retired', 
                   'so2_FGD', 'nox_any', 'nox_SCR', 'so2_FGD_install', 'nox_any_install', 'nox_SCR_install', 'mw']
    sorted_cols = needed_cols + [col for col in df_AMPD.columns if col not in needed_cols]
    df_AMPD = df_AMPD[sorted_cols]
    # remove un-needed 2005 and 2020 data
    mask = (df_AMPD['year'] > 2005) & (df_AMPD['year'] < 2020)
    df_AMPD = df_AMPD.loc[mask, :]
    
    
    ### write data to table
    os.chdir(base_dname)
    os.chdir(rel_path_output_pollutants)
    writer = pd.ExcelWriter('merged_AMPD_eGRID.xlsx')
    df_AMPD.to_excel(writer, index=False)
    writer.close()