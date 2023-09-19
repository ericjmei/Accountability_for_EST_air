# -*- coding: utf-8 -*-
"""
Created on Tue Jan 10 17:15:57 2023

class to fit XGB models. Performs 4-fold random search and 4-fold grid search

@author: emei3
"""

## imports
import os
import pandas as pd
import numpy as np
from sklearn.model_selection import RandomizedSearchCV
import xgboost as xgb
from sklearn.model_selection import GridSearchCV
import joblib

class XGBfitter(object):
    
    def __init__(self, site, targetNames):
        # change path to fitted data folder
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        os.chdir(dname)
        os.chdir("../../Data/") # change to data folder
        
        self.site = site # set site to be fitted for
        self.targetNames = targetNames # set targets to be fitted for
    
    def randomSearch(self):
        """
        gets best random search hyperparameters for each target feature

        Returns
        -------
        None.

        """
        
        ## create random grid
        # Number of estimators
        n_estimators = [int(x) for x in np.linspace(start = 200, stop = 2000, num = 6)] 
        # minimum sub of weights of all observations in a child
        min_child_weight = range(1,6,2)
        # Maximum number of levels in tree
        max_depth = range(2,5,1)
        # Minimum loss reduction required to split a node
        gamma = [i/10.0 for i in range(0,5)]
        # subsample of observations to build trees; lower = more conservative
        subsample = [i/10.0 for i in range(4,10)]
        # shrinks weights, making boosting more conservative
        learning_rate = [0.0001,0.001, 0.01, 0.1, 1]
        # perc of columns to be randomly sampled for each tree
        colsample_bytree = [i/10.0 for i in range(6,10)]
        # higher = more conservative
        #reg_alpha =  [1e-5, 1e-2, 0.1, 1, 10, 100]
        reg_alpha =  [1, 10, 100]
        # higher = more concervative
        #reg_lambda = [1e-5, 1e-2, 0.1, 1, 10, 100]
        reg_lambda = [1, 10, 100]
        
        # Create the random grid
        random_grid = {'n_estimators': n_estimators,
                       'min_child_weight': min_child_weight,
                       'max_depth': max_depth,
                       'gamma': gamma,
                       'subsample': subsample,
                       'learning_rate': learning_rate,
                       'colsample_bytree': colsample_bytree,
                       "reg_alpha": reg_alpha,
                       "reg_lambda": reg_lambda}
        
        ## random search training
        ## instantiate results dataframe for random search
        RS_results = pd.DataFrame()

        for target in self.targetNames: # loop through all targets
            fn = self.site + "_" + target # file names, except for extension
            print("random search XGB hyperparameters for " + target + " at " + self.site)
            
            ## read excel files
            os.chdir("./ForModel/ML/"+self.site)
            X_train = pd.read_excel(fn+".xlsx", sheet_name='X_train')
            y_train = pd.read_excel(fn+".xlsx", sheet_name="y_train")
            os.chdir("../../..") # change back to data folder
            
            ## Use the random grid to search for best hyperparameters
            # First create the base model to tune
            xgboost = xgb.XGBRegressor(random_state=13)
            # Random search of parameters, using 10 fold cross validation, 
            # search across 100 different combinations, and use all available cores
            xgb_random = RandomizedSearchCV(estimator = xgboost, param_distributions = random_grid, 
                                           n_iter = 100, cv = 4, verbose=1, random_state=13, 
                                           n_jobs = -1)# Fit the random search model
            xgb_random.fit(X_train, y_train.values.ravel())
            
            ## put random search results into dataframe
            current_results = pd.DataFrame.from_dict(xgb_random.best_params_,
                                                     orient='index', columns=[target])
            # append to dataframe
            RS_results = pd.concat([RS_results, current_results.T])
        
        self.RS_results = RS_results # save RS results to object
        
        ## write to excel file
        os.chdir("./Analysis/"+self.site)
        fn = self.site+"_randomSearch_XGB.xlsx"
        writer = pd.ExcelWriter(fn)
        RS_results.to_excel(writer, index=True)
        writer.close()
        os.chdir("../..") # change back to data folder
        
    def gridSearchFromRandom(self):
        """
        gets best grid search hyperparameters for each target feature
        randomSearch must be run first!
        saves model to file

        Returns
        -------
        None.

        """
        
        ## instantiate results dataframe for grid search
        GS_results = pd.DataFrame()

        for target in self.targetNames: # loop through all targets
            fn = self.site+ "_" + target # file names, except for extension
            print("grid search XGB hyperparameters for " + target + " at " + self.site)
            
            ## read excel files
            os.chdir("./ForModel/ML/"+self.site)
            X_train = pd.read_excel(fn+".xlsx", sheet_name='X_train')
            y_train = pd.read_excel(fn+".xlsx", sheet_name="y_train")
            os.chdir("../../..") # change back to data folder
            
            ## adjust hyperparameters from random search for targe
            # extract grid parameters from random
            param_grid = self.RS_results.loc[target]
            param_grid = param_grid.to_dict()
            # keep gamma, subsample, colsample, alpha, and lambda (need to be lists)
            param_grid["gamma"] = [param_grid["gamma"]]
            param_grid["subsample"] = [param_grid["subsample"]]
            param_grid["colsample_bytree"] = [param_grid["colsample_bytree"]]
            param_grid["reg_alpha"] = [param_grid["reg_alpha"]]
            param_grid["reg_lambda"] = [param_grid["reg_lambda"]]
            # search around for optimal n_est, min_child, max_depth, and learning_rate
            # n_estimators are -20%, -10%, 0%, +10%, +20% change
            n_estimators = [0.8, 0.9, 1, 1.1, 1.2]  
            n_estimators = [int(estimator*param_grid["n_estimators"])
                            for estimator in n_estimators]
            # min_child_weight is 0 or +1 change
            min_child_weight = [0, 1] 
            min_child_weight = [int(weight)+param_grid["min_child_weight"]
                         for weight in min_child_weight]
            # max_depth is -1, 0, +1 change
            max_depth = [0, 1] 
            max_depth = [int(int(depth)+param_grid["max_depth"])
                         for depth in max_depth]
            # learning_rate is -50%, 0%, +100% change
            learning_rate = [0.5, 1, 2]
            if param_grid["learning_rate"] == 1:
                learning_rate = [0.2, 0.5, 1] #learning rate cannot be >1
            learning_rate = [rate*param_grid["learning_rate"]
                             for rate in learning_rate]
            # set new hyperparameters
            param_grid["n_estimators"] = n_estimators
            param_grid["min_child_weight"] = min_child_weight
            param_grid["max_depth"] = max_depth
            param_grid["learning_rate"] = learning_rate
            
            ## Use the grid to search for best hyperparameters
            # First create the base model to tune
            xgboost = xgb.XGBRegressor(random_state=13)
            # Grid search of parameters, using 10 fold cross validation
            xgb_grid = GridSearchCV(estimator = xgboost, param_grid = param_grid,                         
                                    cv = 4, n_jobs = -1, verbose = 1)# Fit the random search model
            xgb_grid.fit(X_train, y_train.values.ravel())
            
            ## put grid search results into dataframe
            current_results = pd.DataFrame.from_dict(xgb_grid.best_params_,
                                                     orient='index', columns=[target])
            # append to dataframe
            GS_results = pd.concat([GS_results, current_results.T])
            
            ## save model
            os.chdir("./Fitted Models/"+self.site) # change to relevant model folder
            joblib.dump(xgb_grid.best_estimator_, fn+"_XGB.json") # save best model
            os.chdir("../..") # change back to data folder

        ## write to excel file
        os.chdir("./Analysis/"+self.site)
        fn = self.site+"_gridSearch_XGB.xlsx"
        writer = pd.ExcelWriter(fn)
        GS_results.to_excel(writer, index=True)
        writer.close()
        os.chdir("../..") # change back to data folder