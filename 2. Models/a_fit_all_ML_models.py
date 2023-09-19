# -*- coding: utf-8 -*-
"""
Created on Sun Jan  8 19:02:32 2023

tune hyperparameters and fit RF and XGB on all sites specified

@author: emei3
"""

## imports
import os

# change path to fitted model folder
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

from XGBoost_fit import XGBfitter # fits hyperparameters for each target for each class

#%% fit model for each site
sites = ["SDK", # all sites in Atlanta
    "Bronx", "Manhattan", "Queens"] # all sites in NYC

targetNames = ["pm25", "ozone",
               #"OC", "EC", "OC_TOT", "EC_TOT", "NH4", "NO3", "SO4", "CO", "NO2", "SO2"
               ]

for site in sites: # loop through each site
    
    ## XGB
    fitter = XGBfitter(site=site, targetNames=targetNames) # initiate fitter
    fitter.randomSearch() # random search
    fitter.gridSearchFromRandom() # grid search and save model