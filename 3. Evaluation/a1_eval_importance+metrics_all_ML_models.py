# -*- coding: utf-8 -*-
"""
Created on Tue Jan 10 15:42:33 

evaluating all models made from RF and XGBoost
plots and stores all model feature importances
does 5-fold cross-validation on all data (debugging currently)
calculates metrics (training, testing, and all) for all sites
calculates time series of modeled targets

all figures stored in the figures folder under "Importance"
all metrics stored in the data folder under "Analysis"
all timeseries data stored in data folder under "Analysis"

@author: emei3
"""

## imports
import os

# change path to fitted model folder
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

from XGBoost_eval import XGBeval # evaluates models for each target for each class (XGB)

#%% make dictionary mapping sensible strings to variable names

temp = {"Day of Year": ["dayofyear"],
        "Mon": ["Mon"],
        "Tue": ["Tue"],
        "Wed": ["Wed"],
        "Thu": ["Thu"],
        "Fri": ["Fri"],
        "Sat": ["Sat"],
        "Sun": ["Sun"],
        "Precipitation": ["PRCP", "PRCPLaGuardia"],
        "RH": ["RH", "RHontario", "RHLaGuardia"],
        "Wind Speed": ["WS", "AWNDLAX", "WSLaGuardia"],
        "Max Temp": ["TMAX", "TMAXbarstow", "TmaxLaGuardia"],
        "Temp": ["TEMP", "TEMPLaGuardia"],
        "ENSO Monthly": ["ENSOmonthly"],
        "Max Solar Rad": ["SRmaxC"],
        "Mean Solar Rad": ["SRmeanC"],
        "NOx EGU": ["NOxEGU", "NOxEGUtot"],
        "SO2 EGU": ["SO2EGU", "SO2EGUtot"],
        "Regional NOx EGU": ["NOxEGUout"],
        "Regional SO2 EGU": ["SO2EGUout"],
        "NH3 Mobile": ["NH3mobile"],
        "NOx Mobile": ["NOxmobile"],
        "SO2 Mobile": ["SO2mobile"],
        "PM2.5 Mobile": ["PM25-PRImobile"],
        "VOC Mobile": ["VOCmobile"],
        "EC Mobile": ["ECmobile"],
        "CO Mobile": ["COmobile"],
        "NH3 Other": ["NH3other"],
        "NOx Other": ["NOxother"],
        "SO2 Other": ["SO2other"],
        "PM2.5 Other": ["PM25-PRIother"],
        "VOC Other": ["VOCother"],
        "EC Other": ["ECother"],
        "CO Other": ["COother"],
        "(NOx Mobile)*(Max Temp)": ["NOxmobileTmax"],
        "(NOx Mobile)*(VOC Mobile)": ["NOxVOCmobile"],
        "(NOx EGU)*(Max Temp)": ["NOxEGUTmax", "NOxEGUtotTmax"],
        "(SO2 EGU)*(Max Temp)": ["SO2EGUTmax", "SO2EGUtotTmax"],
        "(Reg. NOx EGU)*(Max Temp)": ["NOxEGUoutTmax"],
        "(Reg. SO2 EGU)*(Max Temp)": ["SO2EGUoutTmax"],
        "(NH3 Mobile)*(SO2 Mobile)": ["NH3SO2mobile"],
        "(NH3 Mobile)*(NOx Mobile)": ["NH3NOxmobile"],
        "(VOC Mobile)*(Max Temp)": ["VOCmobileTmax"]}

# invert the dictionary (so that the values become the keys)
varNames = {}
for k,v in temp.items():
    for x in v:
        varNames.setdefault(x,[]).append(k) # for each value in list, make a new key entry

#%% run eval for each site
sites = ["SDK", # all sites in Atlanta
     "Bronx", "Manhattan", "Queens"] # all sites in NYC

for site in sites:
    
    # XGB
    evaler = XGBeval(site=site) # initiate fitter
    evaler.getImportance() # write importances to file
    evaler.plotImportance(varNames=varNames) # plot importances and save figures
    #evaler.crossVal() # do cross validation on train set
    evaler.finalMetrics() # final metrics on test, train, and total sets
    evaler.calcObservedTimeseries() # time series of obs and calc targets