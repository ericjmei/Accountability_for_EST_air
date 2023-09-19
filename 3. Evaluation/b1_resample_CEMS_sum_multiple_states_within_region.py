# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 07:33:36 2023

script to sum EGU emissions and demand from different states - intended for obtaining actual CEMS outputs for subsetted regions
uses simple dispatch code and eGRID to retain generators within the balancing authority or nerc region specified
there will be a slight 1hr time shift in the data, but this does not significantly impact results

@author: emei3
"""

import pandas as pd
import os

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

## Inputs
# input folder
rel_path_eGRID = "../../Data/Simple Dispatch Inputs"
rel_path_CEMS = "../../Data/CAMD/PUDL retrieved hourly"
CEMS_prefix = "CEMS_hourly_local_" # prefix to all CEMS files
# output folder
rel_path_output = "../../Data/Simple Dispatch Outputs/2023-04-18 act ba regions/Actual CEMS"
# group of states to sum together
# group_of_states=['CT', 'NY', 'PA', 'NJ', 'DE', 'MD', 'VA', 'WV', 'OH', 'KY', 'MI', 'IL', 'NC', 'IN'] # NY regional for BA
group_of_states = ['NY', 'CT', 'MI','IN','OH','WV','MD','PA','NJ', 'IL', 'KY', 'WI', 'DE', 'VA'] # NY regional for NERC
# nerc region or ba region to subset for; both can be used, but if regions don't intersect, then nothing will be returned
# multiple regions can be entered as well
# nerc_regions_to_retain = [] # leave blank if not parsing
nerc_regions_to_retain = ['RFC', 'NPCC']
# ba_regions_to_retain = ['PJM', 'ISNE', 'NYIS'] # leave blank if not parsing
ba_regions_to_retain = []
# years to run; must be iterable
years = range(2006, 2020)

# loop through states in group, adding each CEMS output 
for year in years:
    output = pd.DataFrame() # instantiate output dataframe
    for state in group_of_states:
        os.chdir(base_dname) # change to code directory
        os.chdir(rel_path_CEMS) # change to CEMS path
        os.chdir("./"+state) # specific state's folder
        # loop through each year needed, appending to dictionary
        fn = CEMS_prefix+state+"_"+str(year)+'.parquet' # unique file name for particular state year combo
        CEMS_hourly = pd.read_parquet(fn) # obtain data for specific year
        CEMS_hourly.drop(columns=['plant_id_eia', 'emissions_unit_id_epa',
               'year', 'state', 'operating_time_hours',
               'heat_content_mmbtu', 'steam_load_1000_lbs',
               'so2_mass_measurement_code',
               'nox_mass_measurement_code', 'co2_mass_tons',
               'co2_mass_measurement_code'], axis=1, inplace=True)
        CEMS_hourly.rename(columns={'plant_id_epa':'ORISPL', 'operating_datetime':'date', 'gross_load_mw':'demand',
                            'so2_mass_lbs':'so2_tot', 'nox_mass_lbs': 'nox_tot'}, inplace=True)
        CEMS_hourly["date"] = pd.to_datetime(CEMS_hourly["date"])
        CEMS_hourly = CEMS_hourly[CEMS_hourly["date"].dt.year == year] # ensure only 1 year data 
        
        ## remove powerplants not in eGRID, unfortunately need to do that here or the output is too large
        # different run years will have different eGRIDs
        os.chdir(base_dname) # change to code directory
        os.chdir(rel_path_eGRID) # change to CEMS path
        if year == 2006:
            egrid_data_xlsx = 'egrid2005_data.xlsx'
        elif year == 2007 or year == 2008:
            egrid_data_xlsx = 'egrid2007_data.xlsx'
        elif year == 2009:
            egrid_data_xlsx = 'egrid2009_data.xlsx'
        elif year == 2010 or year == 2011:
            egrid_data_xlsx = 'egrid2010_data.xlsx'
        elif year == 2012 or year == 2013:
            egrid_data_xlsx = 'egrid2012_data.xlsx'
        elif year == 2014 or year == 2015:
            egrid_data_xlsx = 'egrid2014_data.xlsx'
        elif year == 2016 or year == 2017:
            egrid_data_xlsx = 'egrid2016_data.xlsx'
        elif year == 2018:
            egrid_data_xlsx = 'egrid2018_data.xlsx'
        elif year == 2019:
            egrid_data_xlsx = 'egrid2019_data.xlsx'
        egrid_plnt = pd.read_parquet(egrid_data_xlsx.split('.')[0]+'_PLNT.parquet')
        CEMS_hourly = CEMS_hourly.merge(egrid_plnt, how='left', on='ORISPL') # add data such as egrid subregion and balancing authority
        # remove rows that aren't in the nerc or ba regions wanted
        if nerc_regions_to_retain:
            CEMS_hourly = CEMS_hourly[CEMS_hourly['NERC'].isin(nerc_regions_to_retain)]
        if ba_regions_to_retain:
            CEMS_hourly = CEMS_hourly[CEMS_hourly['BACODE'].isin(ba_regions_to_retain)]
        CEMS_hourly.drop(columns=CEMS_hourly.columns.difference(['date', 'demand', 'so2_tot', 'nox_tot']),
                          inplace=True) # remove unneeded columns
        
        ## process columns (resample, change units)
        CEMS_daily = CEMS_hourly.resample('24H', origin='start_day', on="date").sum()
        CEMS_daily.reset_index(inplace=True) # move date back to column instead of index
        CEMS_daily.so2_tot = CEMS_daily.so2_tot * 0.454 #lbs to kg
        CEMS_daily.nox_tot = CEMS_daily.nox_tot * 0.454 #lbs to kg
        
        output = pd.concat([output, CEMS_daily], axis=0) # outputs
    
    # sum outputs from different states
    output = output.resample('24H', origin='start_day', on="date").sum()
    output.reset_index(inplace=True) # move date back to column instead of index
    # write to file
    os.chdir(base_dname) # change to code directory
    os.chdir(rel_path_output) # change to output path
    output.to_csv("actual_CEMS_"+'_'.join(group_of_states)+'_stitched'+'_'+str(year)+'.csv', index=False) # write to file