# -*- coding: utf-8 -*-
"""
Created on Tue Feb 14 07:54:36 2023

bins hourly data to daily and shifts times from UTC to local standard time
only retains gross load, SO2, and NOx data

@author: emei3
"""

## imports
import os
import pandas as pd

import datetime

# Dictionary of US state time zones
state_time_zones = {
    "AL": "-06:00",
    "AK": "-09:00",
    "AZ": "-07:00",
    "AR": "-06:00",
    "CA": "-08:00",
    "CO": "-07:00",
    "CT": "-05:00",
    "DE": "-05:00",
    "FL": "-05:00",
    "GA": "-05:00",
    "HI": "-10:00",
    "ID": "-07:00",
    "IL": "-06:00",
    "IN": "-05:00",
    "IA": "-06:00",
    "KS": "-06:00",
    "KY": "-05:00",
    "LA": "-06:00",
    "ME": "-05:00",
    "MD": "-05:00",
    "MA": "-05:00",
    "MI": "-05:00",
    "MN": "-06:00",
    "MS": "-06:00",
    "MO": "-06:00",
    "MT": "-07:00",
    "NE": "-06:00",
    "NV": "-08:00",
    "NH": "-05:00",
    "NJ": "-05:00",
    "NM": "-07:00",
    "NY": "-05:00",
    "NC": "-05:00",
    "ND": "-06:00",
    "OH": "-05:00",
    "OK": "-06:00",
    "OR": "-08:00",
    "PA": "-05:00",
    "RI": "-05:00",
    "SC": "-05:00",
    "SD": "-06:00",
    "TN": "-06:00",
    "TX": "-06:00",
    "UT": "-07:00",
    "VT": "-05:00",
    "VA": "-05:00",
    "WA": "-08:00",
    "WV": "-05:00",
    "WI": "-06:00",
    "WY": "-07:00"
}

# Function to convert UTC time to US state standard time
def convert_utc_to_state_time(utc_time, state_abbr):
    # Get the time zone offset for the state
    offset = datetime.timedelta(hours=int(state_time_zones[state_abbr][0:3]), minutes=int(state_time_zones[state_abbr][4:6]))
    
    # Apply the offset to the UTC time
    state_time = utc_time + offset
    
    return state_time

#%% Shift timeseries to local standard time 
# change path to data folder
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
os.chdir("../../Data")
os.chdir("./CAMD/")

## Inputs
years = range(2006,2020) # years needed
# states needed (based on which NERC regions are modeled)
NERCneeded = ['SERC', 'WECC', 'NPCC', 'RFC'] # NERC regions to model, can comment out if want to select alternative set of states
NERCdict = {'FRCC': ['fl'], 
            'WECC': ['ca','or','wa', 'nv','mt','id','wy','ut','co','az','nm','tx'],
            'SPP' : ['nm','ks','tx','ok','la','ar','mo'],
            'RFC' : ['wi','mi','il','in','oh','ky','wv','va','md','pa','nj'],
            'NPCC' : ['ny','ct','de','ri','ma','vt','nh','me'],
            'SERC' : ['mo','ar','tx','la','ms','tn','ky','il','va','al','ga','sc','nc', 'fl'], 
            'MRO': ['ia','il','mi','mn','mo','mt','nd','ne','sd','wi','wy'], 
            'TRE': ['ok','tx']} # NERC regions from Simple Dispatch
states = [NERCdict[x] for x in NERCneeded] # grab all states needed
states = [item for sublist in states for item in sublist] # flatten 2-D list to 1-D
states = [x.upper() for x in states] # make uppercase


## begin function
# loop through all states and years needed
for state in states:
    print("converting hourly CEMS for " + state + " to hourly local standard time for years " 
          + ", ".join([str(x) for x in years]))
    
    for year in years: 
        fn = state+"_"+str(year) # unique file name for particular state year combo
        
        ## read in raw file
        os.chdir("./PUDL retrieved hourly/"+state) # change to particular state's raw hourly folder
        CEMS_hourly = pd.read_parquet("CEMS_hourly_"+fn+".parquet")
        
        # break loop if the dataframe is empty
        if CEMS_hourly.shape[0] == 0:
            print("hourly CEMS for " + state + " in " + str(year) + " is empty. The data may not exist.")
            os.chdir("../..")
            break
        
        ## change time zone
        CEMS_hourly = CEMS_hourly.rename(columns={"operating_datetime_utc":"operating_datetime"}) # rename time column
        CEMS_hourly["operating_datetime"] = CEMS_hourly["operating_datetime"].apply(
            lambda x: convert_utc_to_state_time(x, state)) # convert to standard time
        CEMS_hourly['operating_datetime'] = CEMS_hourly['operating_datetime'].dt.tz_localize(None) # make time zone unaware
        
        ## write to file
        # write hourly data back into new file
        CEMS_hourly.to_parquet("CEMS_hourly_local_"+fn+".parquet", index=False)
        os.chdir("../..")
        
#%% bin data to daily resolution and store in different folder
# change path to data folder
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
os.chdir("../../Data")
os.chdir("./CAMD/")

## Inputs
years = range(2006,2020) # all years needed
# states needed (based on which NERC regions are modeled)
NERCneeded = ['SERC', 'NPCC', 'RFC'] # NERC regions to model, can comment out if want to select alternative set of states
NERCdict = {'FRCC': ['fl'], 
            'WECC': ['ca','or','wa', 'nv','mt','id','wy','ut','co','az','nm','tx'],
            'SPP' : ['nm','ks','tx','ok','la','ar','mo'],
            'RFC' : ['wi','mi','il','in','oh','ky','wv','va','md','pa','nj'],
            'NPCC' : ['ny','ct','de','ri','ma','vt','nh','me'],
            'SERC' : ['mo','ar','tx','la','ms','tn','ky','il','va','al','ga','sc','nc', 'fl'], 
            'MRO': ['ia','il','mi','mn','mo','mt','nd','ne','sd','wi','wy'], 
            'TRE': ['ok','tx']} # NERC regions from Simple Dispatch
states = [NERCdict[x] for x in NERCneeded] # grab all states needed
states = [item for sublist in states for item in sublist] # flatten 2-D list to 1-D
states = [x.upper() for x in states] # make uppercase

## begin function
# loop through all states and years needed
for state in states:
    print("binning hourly CEMS for " + state + " to daily resolution for years " 
          + ", ".join([str(x) for x in years]))
    
    for year in years: 
        fn = state+"_"+str(year) # unique file name for particular state year combo
        
        ## read in raw file
        os.chdir("./PUDL retrieved hourly/"+state) # change to particular state's raw hourly folder
        CEMS_hourly = pd.read_parquet("CEMS_hourly_local_"+fn+".parquet") # data shifted to local time
        
        # break loop if the dataframe is empty
        if CEMS_hourly.shape[0] == 0:
            print("hourly CEMS for " + state + " in " + str(year) + " is empty. The data may not exist.")
            os.chdir("../..")
            break
        
        ## bin to daily resolution and sum up to whole state
        CEMS_daily = CEMS_hourly.rename(columns={"operating_datetime":"date"}) # rename time column
        CEMS_daily = CEMS_daily[["date", "gross_load_mw", "so2_mass_lbs", "nox_mass_lbs"]] # retrieve relevant columns
        CEMS_daily = CEMS_daily.resample('24H', origin='start_day', on="date").sum() # bin to 24 h
        CEMS_daily.reset_index(inplace=True) # move date back to column instead of index
        
        ## write to file
        isExist = os.path.exists("../../Hourly to daily CEMS/"+state) # check if folder exists
        if not isExist:
           # Create a new directory because it does not exist
           os.makedirs("../../Hourly to daily CEMS/"+state)
        os.chdir("../../Hourly to daily CEMS/"+state)
        CEMS_daily.to_csv("CEMS_daily_local_"+fn+".csv", index=False) # write csv (can be changed to parquet)
        os.chdir("../..")