# -*- coding: utf-8 -*-
"""
Created on Sat Feb 11 20:48:48 2023

@author: emei3
"""

import pandas as pd
import os

state = "GA" # for data reading purposes
year = 2017
print("processing CEMS data from " + state + " for " + str(year))

## change file path for writing
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
os.chdir("../../Data/CAMD/PUDL retrieved hourly")

os.chdir("./" + state) # NEED TO EDIT THIS
df_cems_add = pd.read_parquet('CEMS_hourly_'+state+'_'+str(year)+'.parquet')
#%%
to_split = df_cems_add["operating_datetime_utc"]
to_split = to_split.tolist()

date = [x.strftime("%m/%d/%y") for x in to_split]
hour = [x.strftime("%H") for x in to_split]

timeData_toAdd = pd.DataFrame({'date': date,
                               'hour': hour})

# grab necessary data and rename
# because data was pre-cleaned by PUDL, made choice to use EIA id - should test with EPA id as well
### NEED TO SPLIT UTC TIME TO DATE AND HOUR - also, check to make sure that this program adjusts for time zone

#%%
df_cems_add = df_cems_add[['plant_id_eia', 'emissions_unit_id_epa', 'OP_DATE','OP_HOUR', 'gross_load_mw', 
                           'so2_mass_lbs', 'nox_mass_lbs', 'co2_mass_tons', 'heat_content_mmbtu']].dropna()
df_cems_add.columns=['orispl', 'unit', 'date','hour','mwh', 'so2_tot', 'nox_tot', 'co2_tot', 'mmbtu']