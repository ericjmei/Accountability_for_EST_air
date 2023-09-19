# -*- coding: utf-8 -*-
"""
Created on Fri Nov 18 09:32:54 2022

plot all data into folders to see if reasonable

@author: emei3
"""
##imports
import os
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

#change path to data folder
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
os.chdir("../../Data/FromZiqi")

#%% import dataframe
df = pd.read_excel("Atlanta.xlsx", sheet_name="base")

#%% plot a few things
fig, ax = plt.subplots()
ax.plot(df.Date, df.SO2EGU)