# -*- coding: utf-8 -*-
"""
Created on Fri Nov 18 22:34:43 2022

trying out XGBoost

@author: ericm
"""

## imports
import xgboost as xgb
import os
import pandas as pd

# change path to data folder
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
os.chdir("../../Data")

#%% grab necessary data
from sklearn.model_selection import train_test_split

os.chdir("./ForModel/ML")

# read excel file
fn = 'AtlantaBase.xlsx'
X = pd.read_excel(fn, sheet_name='X')
y = pd.read_excel(fn, sheet_name='y')

# grab necessary column
y = y.pm25sd

# change back to data folder
os.chdir("../..")


#%% pre-process training data
# drop nan y inds
inds = y.notnull()
X = X.loc[inds, :]
y = y.loc[inds]

# grab training and test data
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.1, random_state=13)

#%% preliminary fit
regressor=xgb.XGBRegressor(random_state=13)

regressor.fit(X_train, y_train)

#%% save model
# change path to fitted model folder
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
os.chdir("./Fitted")

# save model
regressor.save_model("SDK_xgb_pm25.json")