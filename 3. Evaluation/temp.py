# -*- coding: utf-8 -*-
"""
Created on Fri Mar 31 16:23:12 2023

renames and/or copies NERC region results such that the file names are consistent with
other scripts that use the data 
this script is mostly for legacy data directories that need to have their structures reformatted

@author: emei3
"""

import pandas as pd
import os

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)

## Inputs
# input folder (Simple Dispatch NERC results)
rel_path_input = "../../Data/Simple Dispatch Outputs/2023-05-04 cf ba regions without hist downtime FIXED"
# output folder (Simple Dispatch subsetted folder)
rel_path_output = "../../Data/Simple Dispatch Outputs/2023-05-04 cf ba regions without hist downtime FIXED/Actual CEMS"
# input file prefix (generally simple_dispatch_ or actual_dispatch_)
in_file_prefix = "simple_dispatch_"
# in_file_prefix = "actual_dispatch_"
# output file prefix (generally simple_dispatch_)
out_file_prefix = "simple_dispatch_"
# out_file_prefix = "actual_CEMS_"
# NERC regions to move over
nerc_regions = ['SOCO', 'PJM', 'ISNE', 'NYIS']
# states within NERC region - needed for renaming convention for consistency
nerc_to_state_names = [['GA', 'AL'],
                     ['PA', 'NJ', 'DE', 'WV', 'OH', 'IL', 'NC', 'IN'],
                     ['CT'],
                     ['NY']]
# years to run; must be iterable
years = range(2006, 2020)

# loop through all NERC regions, copying dispatch outputs into new folder
for i, nerc_region in enumerate(nerc_regions):
    output = dict.fromkeys(years) # dictionary to hold all dataframes
    
    # find all files that match specified file name structure
    os.chdir(base_dname) 
    os.chdir(rel_path_input) #change to input folder 
    for year in years:
        fn = in_file_prefix + nerc_region +'_'+str(year)+'_'.join(nerc_to_state_names)+'.parquet' # unique file name for particular NERC region
        # import files and store in output
        output[year] = pd.read_parquet(fn)
    
    # store files in output folder
    os.chdir(base_dname) 
    os.chdir(rel_path_output) #change to input folder 
    for year in years:
        fn = out_file_prefix+nerc_region+'_'+'_'.join(nerc_to_state_names[i])+'_'+str(year)+'.csv' # unique file name for particular NERC region
        # store output in new file
        output[year].to_csv(fn, index=False)