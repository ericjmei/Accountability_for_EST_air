# -*- coding: utf-8 -*-
"""
Created on Wed Apr 12 11:45:29 2023

@author: emei3
"""

import os
import pickle

# obtain code directory name for future folder changing
abspath = os.path.abspath(__file__)
base_dname = os.path.dirname(abspath)
os.chdir(base_dname)  # change to code directory
os.chdir("../../SimpleDispatch") # change to simple dispatch
from simple_dispatch import bidStack

if __name__ == '__main__':
    
    ## inputs
    run_year = 2015  # specify run year
    region = 'PJM'
    states_to_subset = []
    week = 31
    plot_legend = False
    coal_ng_only = False
    # input and output folders
    # rel_path_input = "../../Data/Simple Dispatch Outputs/2023-05-10 cf ba coal propagated/Generator Data"
    # fn_beginning_gd_short = 'counterfactual_'
    # fn_end = '_cf'
    fn_beginning_gd_short = ''
    fn_end = '_act'
    rel_path_input = "../../Data/Simple Dispatch Outputs/2023-05-10 act ba coal propagated/Generator Data"
    rel_path_output = "../../Figures/2023-05-10 cf ba coal propagated/merit orders"
    
    
    
    ## create bidstack object
    os.chdir(base_dname)
    os.chdir(rel_path_input)
    gd_short = pickle.load(open(fn_beginning_gd_short+'generator_data_short_%s_%s.obj'%(region, str(run_year)), 'rb')) # load generatordata object
    bs = bidStack(gd_short, time=week, dropNucHydroGeo=True, include_min_output=True, 
                  states_to_subset=states_to_subset, mdt_weight=0.5) # create bidstack
    
    ## plot  
    # bid stack
    fig_bidstack = bs.plotBidStackMultiColor('gen_cost', 'bar', fig_dim = (4,4), production_cost_only=True, show_legend=False, coal_ng_only=coal_ng_only)
    
    # NOx
    fig_NOx = bs.plotBidStackMultiColor('nox', 'bar', fig_dim = (4,4), production_cost_only=True, show_legend=False, coal_ng_only=coal_ng_only)
    
    # SO2
    fig_SO2 = bs.plotBidStackMultiColor('so2', 'bar', fig_dim = (4,4), production_cost_only=True, show_legend=False, coal_ng_only=coal_ng_only)
    
    ## export
    fn_beginning = region+'_'+'_'.join(states_to_subset)+'_'+str(run_year)+'_week_'+str(week)+'_' # beginning figure name
    
    os.chdir(base_dname)
    os.chdir(rel_path_output)
    if plot_legend: # plot legend if needed
        # legend
        fig_leg = bs.plotBidStackMultiColor('gen_cost', 'bar', fig_dim = (4,4), production_cost_only=True, show_legend=True, coal_ng_only=coal_ng_only)
        fig_leg.savefig("legend"+fn_end+".png", dpi=fig_leg.dpi*20, bbox_inches='tight') # save at plot dpi*10
    fig_bidstack.savefig(fn_beginning+'bidstack'+fn_end+'.png', dpi=fig_bidstack.dpi*10) # bid stack
    fig_NOx.savefig(fn_beginning+'NOx'+fn_end+'.png', dpi=fig_NOx.dpi*10) # NOx
    fig_SO2.savefig(fn_beginning+'SO2'+fn_end+'.png', dpi=fig_SO2.dpi*10) # SO2