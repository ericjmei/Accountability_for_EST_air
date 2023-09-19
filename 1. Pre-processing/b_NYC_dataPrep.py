# -*- coding: utf-8 -*-
"""
Created on Sun Jan  8 17:42:25 2023

prepares data for machine learning models and pre-selects features

@author: emei3
"""

##imports
import os
import pandas as pd

#change path to data folder
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
os.chdir("../../Data")

#%% import entire dataframe
os.chdir("./FromZiqi")
df = pd.read_excel("NYC.xlsx", sheet_name="Base")
os.chdir("..")

#%% split up data into features and response variables
## encode dayofweekf
ohe = pd.get_dummies(df[['dayofweekf']]) # apply one-hot encoding
# put items back in df
df = df.join(ohe)
# drop original categorical variable
df = df.drop(labels = "dayofweekf", axis = 1)
# rename variables 
df = df.rename(columns={"dayofweekf_Mon":"Mon", "dayofweekf_Tue":"Tue", "dayofweekf_Wed":"Wed", "dayofweekf_Thu":"Thu",
                   "dayofweekf_Fri":"Fri", "dayofweekf_Sat":"Sat", "dayofweekf_Sun":"Sun"})

## X
# keep all data that are non-redundant, Date will be removed later
# don't include non-NOx or SO2 EGU (e.g., NH3) bc they are annual and colinear with NOx and SO2 but have no counterfactual scenarios
# for the same reason, in some species, for EGU, we use NOx or SO2 as proxies for NEI-measured values
XNeededBase = ["Date", "dayofyear", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun",
               "TEMPLaGuardia", "WSLaGuardia", "TmaxLaGuardia", "PRCPLaGuardia", "RHLaGuardia"] # met and time variables
XNeededFeatures = {"pm25": ["NOxEGUtot", "SO2EGUtot", "NH3mobile", "NOxmobile", "SO2mobile", "PM25-PRImobile", 
                            "VOCmobile", "ECmobile", "NH3other", "NOxother", "PM25-PRIother", "SO2other", "VOCother"], # PM2.5
                  "ozone": ["NOxEGUtot", "NOxmobile", "VOCmobile", "NOxother", "VOCother"], # O3
                  "OC": ["NOxEGUtot", "NOxmobile", "VOCmobile","NOxother", "VOCother"], # OC
                  "EC": ["PM25-PRImobile", "ECmobile", "PM25-PRIother"], # EC
                  "OC_TOT": ["NOxEGUtot", "NOxmobile", "VOCmobile","NOxother", "VOCother"], # OC
                  "EC_TOT": ["PM25-PRImobile", "ECmobile", "PM25-PRIother"], # EC
                  "NH4": ["NOxEGUtot", "SO2EGUtot", "NH3mobile", "NOxmobile", "SO2mobile", "NH3other", "NOxother", 
                          "SO2other"], # NH4
                  "NO3": ["NOxEGUtot", "NH3mobile", "NOxmobile", "NH3other", "NOxother"], # NO3
                  "SO4": ["SO2EGUtot", "NH3mobile", "SO2mobile", "NH3other", "SO2other"], # SO4
                  "CO": ["COmobile", "COother"], # CO
                  "NO2": ["NOxEGUtot", "NOxmobile", "NOxother"], # NO2
                  "SO2": ["SO2EGUtot",  "SO2mobile", "SO2other"]} # SO2

# create dataframe with all X needed variables
temp = XNeededBase # for storing all variable names
for key in XNeededFeatures:
    temp = temp + XNeededFeatures[key] # add all needed variables to list
    XNeededFeatures[key] = XNeededBase + XNeededFeatures[key] # add base to all variables
temp = list(dict.fromkeys(temp)) # grab unique variable names
X = df[temp] # grab all unique variables

# Y
sites = ["Bronx", "Manhattan", "Queens"] # all sites in NYC
os.chdir("./FromZiqi")
y = {sites[0]: pd.read_excel("NYC.xlsx", sheet_name=sites[0]), # Bronx
     sites[1]: pd.read_excel("NYC.xlsx", sheet_name=sites[1]), # Manhattan
     sites[2]: pd.read_excel("NYC.xlsx", sheet_name=sites[2])} # Queens
os.chdir("..")

# remove rows with X nan (n=7)
inds = X.notna().all(axis=1) # find all rows with full data
X = X[inds] # remove nan rows from feature set
for site in sites:
    y[site] = y[site][inds] # remove corresponding target inds 

#%% write features and target variables to file; use featurewiz to select features for all yNeeded
# change to working folder to import function
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
from selectFeatures import selectFeatures # helper function to select features
os.chdir("../../Data")

targetNames = list(XNeededFeatures.keys()) # all target names, assumes all sites are standardized

for site in sites: # loop through all sites (for individual folders)
    # change to processed data folder
    os.chdir("./ForModel/ML/"+site)
    # Pandas excel writer
    writer = pd.ExcelWriter(site+'Base.xlsx', engine='xlsxwriter')
    ##write to table
    X.to_excel(writer, sheet_name='X', index=False)
    y[site].to_excel(writer, sheet_name='y', index=False)
    # save excel file
    writer.close()
    # change back to data folder
    os.chdir("../../..")
    
    ## select features and write them to file
    selectFeatures(site, X, y[site], targetNames, XNeededFeatures) # select features 