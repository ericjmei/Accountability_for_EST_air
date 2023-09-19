# -*- coding: utf-8 -*-
"""
Created on Sat Feb 11 17:40:48 2023

Use PUDL Catalyst Co-op to download EPA CEMS docs for both air quality ML and dispatch model
package (here)[https://pypi.org/project/catalystcoop.pudl-catalog/]

@author: emei3
"""

import intake
import os
from pudl_catalog.helpers import year_state_filter

pudl_cat = intake.cat.pudl_cat # intake PUDL catalog
list(pudl_cat)

pudl_cat.hourly_emissions_epacems

#%% show avaialble data for hourly CEMS
pudl_cat.hourly_emissions_epacems.discover() 

#%% loop through all states and years needed

## INPUTS
years = range(2006,2020) # years needed
NERCneeded = ['SERC', 'WECC', 'NPCC', 'RFC'] # NERC regions to model, can comment out if want to select alternative set of states
NERCdict = {'FRCC': ['fl'], 
          'WECC': ['ca','or','wa', 'nv','mt','id','wy','ut','co','az','nm','tx'],
          'SPP' : ['nm','ks','tx','ok','la','ar','mo'],
          'RFC' : ['wi','mi','il','in','oh','ky','wv','va','md','pa','nj'],
          'NPCC' : ['ny','ct','de','ri','ma','vt','nh','me'],
          'SERC' : ['mo','ar','tx','la','ms','tn','ky','il','va','al','ga','sc','nc', 'fl'],
          'MRO': ['ia','il','mi','mn','mo','mt','nd','ne','sd','wi','wy'], 
          'TRE': ['ok','tx'], # NERC regions from Simple Dispatch
          # Balancing authority regions
          'SOCO': ['GA','AL','FL','MS'],
          'PJM': ['PA', 'NJ', 'DE', 'MD', 'VA', 'WV', 'OH', 'KY', 'MI', 'IL', 'NC', 'IN'],
          'ISNE': ['ME', 'NH', 'VT', 'MA', 'RI', 'CT'],
          'NYIS': ['NY']} 
states = [NERCdict[x] for x in NERCneeded] # grab all states needed
states = [item for sublist in states for item in sublist] # flatten 2-D list to 1-D
states = [x.upper() for x in states] # make uppercase
print("retrieving hourly CEMS data for years " + ", ".join([str(x) for x in years]) + " and states "
      + ", ".join(states)) 

## change file path for writing
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
os.chdir("../../Data/CAMD/PUDL retrieved hourly")

## loop through states, then years to build CEMS outputs
# NOTE: PUDL can build query for many states and years easily, but states and years are separated for Simple Dispatch's purposes
for state in states:
    print("retrieving hourly CEMS for " + state + " from PUDL")
    
    ## change to particular state's folder
    isExist = os.path.exists(state) # check if folder exists
    if not isExist:
       os.makedirs(state) # Create a new directory because it does not exist
    os.chdir("./"+state)
    
    for year in years: 
        ## query data from PUDL
        filters = year_state_filter(
            years = [year],
            states = [state]) # function by PUDL for query
        hourlyCEMS = pudl_cat.hourly_emissions_epacems(filters=filters).to_dask().compute() # execute query
        
        ## write to parquet; NOTE: can also be CSV if you need to open it in Excel
        hourlyCEMS.to_parquet(
            'CEMS_hourly_'+state+'_'+str(year)+'.parquet', 
            index=False)
        
    os.chdir("..") # change back to larger folder

print("successfully retrieved hourly CEMS data for years " + ", ".join([str(x) for x in years]) + " and states "
      + ", ".join(states))