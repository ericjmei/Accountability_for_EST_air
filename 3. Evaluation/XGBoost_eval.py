# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 10:57:43 2023

class to evaluate all models made from XGB for a particular site
needs to have run XGBoost_fit for all targets at the site

@author: emei3
"""

## imports
import os
import matplotlib.pyplot as plt
import numpy as np
import joblib
import pandas as pd
from sklearn.model_selection import KFold
from sklearn.model_selection import cross_validate
import xgboost as xgb
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_percentage_error

class XGBeval(object):
    
    def __init__(self, site):
        # change path to fitted data folder
        abspath = os.path.abspath(__file__)
        dname = os.path.dirname(abspath)
        os.chdir(dname)
        os.chdir("../../Data/ForModel/ML/"+site) # change to data folder
        print("evaluating XGB model for " + site)
        
        self.site = site # set site to be fitted for
        
        y = pd.read_excel(site+'Base.xlsx', sheet_name="y") # read y data for targets
        self.targetNames = y.columns
        
        os.chdir("../../..") # change back to data folder
        
    def getImportance(self):
        """
        gets importance of fitted models for each target in site
        models should be in the Fitted Models folder
        training data should be pre-processed and in the ForModel foler
        figure importances will be written to the Analysis folder

        Returns
        -------
        None.

        """
        # for writing selected features to file
        os.chdir("./Analysis/"+self.site)
        fn = self.site+"_sortedFeatures_XGB.xlsx"
        writer = pd.ExcelWriter(fn, engine='xlsxwriter')
        os.chdir("../..") # change back to data folder
        
        self.importance = dict() # to pass each target importance into
        
        for target in self.targetNames: # loop through all targets
            print("retrieving XGB importance for " + target + " at " + self.site)
            fn = self.site + "_" + target # file names, except for extension
            
            ## read excel files
            os.chdir("./ForModel/ML/"+self.site)
            X_train = pd.read_excel(fn+".xlsx", sheet_name='X_train')
            os.chdir("../../..") # change back to data folder
            
            ## load model
            os.chdir("./Fitted Models/"+self.site)
            regressor = joblib.load(fn+"_XGB.json")
            os.chdir("../..") # change back to data folder
            
            ## grab importances
            importances = regressor.feature_importances_
            # sort importances
            sorted_indices = np.argsort(importances)[::-1] # indices
            sorted_featureImportance = pd.DataFrame({ # put sorted into dataframe
                'features': X_train.columns[sorted_indices],
                'importances': importances[sorted_indices]})
            
            ## write importances to excel
            sorted_featureImportance.to_excel(writer, sheet_name=target,
                                              index=False)
            
            ## store importances in class
            self.importance[target] = sorted_featureImportance
        
        writer.close() # save sorted features
            
    def plotImportance(self, varNames):
        """
        plots importance of features after getImportance feature

        Returns
        -------
        None.

        """
        if not hasattr(self, 'importance'):
            print('ERROR: getImportance() must be run first')
            return
        
        fn = self.site+"_sortedFeatures_XGB.xlsx"
        
        for target in self.targetNames: # loop through all targets
            print("plotting XGB importance for " + target + " at " + self.site)
            fn = self.site + "_" + target # file names, except for extension
            sorted_featureImportance = self.importance[target] # get feature importances
            
            ## map column name to "prettier" names
            temp = [varNames[x] for x in sorted_featureImportance.features.tolist()] # map to new names
            temp = [item for sublist in temp for item in sublist] # flatten list to 1-D list (from 2-D)
            sorted_featureImportance.features = temp # put back into dataframe
        
            ## plot importance
            fig = plt.figure()
            plt.title(self.site + ' ' + target + ' Feature Importance XGB')
            plt.barh(range(sorted_featureImportance.importances.shape[0]), 
                     sorted_featureImportance.importances[::-1], # invert y axis
                     align='center')
            plt.yticks(range(sorted_featureImportance.importances.shape[0]), 
                       sorted_featureImportance.features[::-1]) # invert y axis
            plt.xlabel('Importance')
            plt.tight_layout()
            plt.show()
            
            # save figure
            os.chdir("../Figures/Importance/"+self.site)
            fig.savefig(fn+"_XGB.png", dpi=fig.dpi*10) # save at plot dpi*10
            os.chdir("../../../Data") #change back to data folder
            
    def crossVal(self):
        """
        performs 5-fold cross validation with tuned hyperparameters and entire dataset

        Returns
        -------
        None.

        """
        
        ## for writing CV results to file
        os.chdir("./Analysis/" + self.site) # change to data folder
        fn = self.site+"_CV_XGB.xlsx"
        writer = pd.ExcelWriter(fn, engine='xlsxwriter')
        
        ## read in hyperparameters from hyperparameter tuning
        fn = self.site+"_gridSearch_XGB.xlsx"
        all_hyperparameters = pd.read_excel(fn, index_col=0)
        all_hyperparameters = all_hyperparameters.replace(np.nan, None) # to replace nan from blank cells
        os.chdir("../..") # change back to data folder
        all_CV_results = pd.DataFrame() # overall average for each target
        
        ## grab all features and target data to do CV with entire dataset
        os.chdir("./ForModel/ML/"+self.site) # change to model data folder
        X = pd.read_excel(self.site+'Base.xlsx', sheet_name="X") # read all X data including Date
        y = pd.read_excel(self.site+'Base.xlsx', sheet_name="y") # read y data for targets, in chronological order
        os.chdir("../../..") # change back to data folder

        for target in self.targetNames: # loop through all targets
            fn = self.site + "_" + target # file names, except for extension
            
            ## select particular X features and y targets needed
            os.chdir("./ForModel/ML/"+self.site) # change to model data folder
            featuresNeeded = pd.read_excel(self.site+'_features.xlsx', sheet_name=target) # X features needed
            X_forTarget = X.loc[:, featuresNeeded.transpose().values[0]] # select X features needed
            y_forTarget = y[target] # select particular target y needed
            os.chdir("../../..") # change back to data folder
            
            ## remove nans
            inds = y_forTarget.notnull().to_numpy()
            X_forTarget = X_forTarget.loc[inds, :]
            y_forTarget = y_forTarget.loc[inds]
            
            ## CV model
            # get hyperparameters
            hyperparameters = all_hyperparameters.loc[target]
            hyperparameters = hyperparameters.to_dict()
            # max depth and n estimators need to be int
            hyperparameters["max_depth"] = int(hyperparameters["max_depth"])
            hyperparameters["n_estimators"] = int(hyperparameters["n_estimators"])
            # fit model
            model=xgb.XGBRegressor(random_state=13, **hyperparameters)
            kfold = KFold(n_splits=5) # 5 folds
            metrics = ['r2', 'rmse', 'mape'] #metrics wanted
            scoring = {metrics[0]: 'r2',
                       metrics[1]: 'neg_root_mean_squared_error',
                       metrics[2]: 'neg_mean_absolute_percentage_error'} #multiple scoring
            results = cross_validate(model, X_forTarget, y_forTarget.values.ravel(), cv=kfold, 
                                      scoring=scoring)
            
            ## put CV results in dataframe
            CV_results = pd.DataFrame(data=results, # initial 5 runs
                                      index=["1", "2", "3", "4", "5"])
            # fix negatives
            CV_results.test_rmse = CV_results.test_rmse*-1
            CV_results.test_mape = CV_results.test_mape*-1
            # calculate means
            CV_means = pd.DataFrame(data={'means':CV_results.mean(axis=0)}) # mean of columns
            CV_results = pd.concat([CV_results, CV_means.T], # combine dfs
                                   axis=0)
            
            ## write CV results to excel
            CV_results.to_excel(writer, sheet_name=target, index=True)
            
            ## store mean CV results in separate table
            CV_means = pd.DataFrame(data={target:CV_results.mean(axis=0)})
            all_CV_results = pd.concat([all_CV_results, CV_means], axis=1) # add means to table
            
        ## write mean CV results to separate sheet
        all_CV_results.to_excel(writer, sheet_name="means", index=True)
        writer.close() # save CV results
        
    def finalMetrics(self):
        """
        evaluates test, training, and total dataset with fitted model for each target

        Returns
        -------
        None.

        """
        def evalMetrics(y_true, y_pred, target): # helper function for fitting
            ## calculate metrics
            rsquare = r2_score(y_true, y_pred) #R^2
            RMSE = np.sqrt(mean_squared_error(y_true, y_pred)) #RMSE
            MAPE = mean_absolute_percentage_error(y_true, y_pred) #MAPE
            
            ## put into dictionary
            metrics = {"R2": rsquare,
                       "RMSE": RMSE,
                       "MAPE": MAPE}
            
            ## turn dictionary into dataframe
            metrics = pd.DataFrame(data=metrics, index=[target])
            
            return metrics
        
        ## instantiate datafromes for metrics
        all_train_metrics = pd.DataFrame()
        all_test_metrics = pd.DataFrame()
        all_total_metrics = pd.DataFrame()

        for target in self.targetNames: # loop through all targets
            print("evaluating XGB model performance for " + target + " at " + self.site)
            fn = self.site + "_" + target # file names, except for extension
            
            ## read excel files
            os.chdir("./ForModel/ML/" + self.site)
            # training data
            X_train = pd.read_excel(fn+".xlsx", sheet_name='X_train')
            y_train = pd.read_excel(fn+".xlsx", sheet_name='y_train')
            # testing data
            X_test = pd.read_excel(fn+".xlsx", sheet_name='X_test')
            y_test = pd.read_excel(fn+".xlsx", sheet_name='y_test')
            os.chdir("../../..") # change back to data folder
            
            ## load model
            os.chdir("./Fitted Models/" + self.site)
            regressor = joblib.load(fn+"_XGB.json")
            os.chdir("../..") # change back to data folder
            
            ## evaluate training data
            y_train_pred = regressor.predict(X_train) # predict from model
            train_metrics = evalMetrics(y_train, y_train_pred, target) # grab metrics
            
            ## evaluate testing data
            y_test_pred = regressor.predict(X_test) # predict from model
            test_metrics = evalMetrics(y_test, y_test_pred, target) # grab metrics
            # save predictions
            os.chdir("./ForModel/ML/"+self.site) # change to model data folder
            y_test_pred_df = pd.DataFrame(y_test_pred, columns=['modeled'])
            y_test_pred_df.to_csv(fn+'_test_mdl_XGB.csv', index=False)
            os.chdir("../../..") # change back to data folder
            
            ## evaluate all data
            total_true = pd.concat([y_train, y_test]) # concatenate training data
            total_pred = np.concatenate((y_train_pred, y_test_pred)) # concatenate testing data
            total_metrics = evalMetrics(total_true, total_pred, target) # grab metrics
            
            ## put data into tables
            all_train_metrics = pd.concat([all_train_metrics, train_metrics]) # training
            all_test_metrics = pd.concat([all_test_metrics, test_metrics]) # testing
            all_total_metrics = pd.concat([all_total_metrics, total_metrics]) # total
            
            
        ## writing metrics to file
        os.chdir("./Analysis/" + self.site)
        fn = self.site + "_performance_metrics_XGB.xlsx"
        writer = pd.ExcelWriter(fn, engine='xlsxwriter')
        all_test_metrics.to_excel(writer, sheet_name="test", index=True) # testing
        all_train_metrics.to_excel(writer, sheet_name="train", index=True) # training
        all_total_metrics.to_excel(writer, sheet_name="total", index=True) # total
        writer.close() #save
        os.chdir("../..") # change back to data folder
    
    def calcObservedTimeseries(self):
        """
        calculates modeled target in order of time (low->high) so plotting is easier
        writes time series of used features, observed targets, and modeled targets
        to analysis folder

        Returns
        -------
        None.

        """
        # prepare writer for writing data
        os.chdir("./Analysis/" + self.site)
        fn = self.site + "_observed_modeled_timeseries_XGB.xlsx"
        writer = pd.ExcelWriter(fn, engine='xlsxwriter')
        os.chdir("../..") # change back to data folder

        os.chdir("./ForModel/ML/"+self.site) # change to model data folder
        X = pd.read_excel(self.site+'Base.xlsx', sheet_name="X") # read all X data including Date
        y = pd.read_excel(self.site+'Base.xlsx', sheet_name="y") # read y data for targets, in chronological order
        os.chdir("../../..") # change back to data folder
        
        ## begin for loop
        for target in self.targetNames: # loop through all targets
            print("calculating predicted timeseries using XGB for " + target + " at " + self.site)
            fn = self.site + "_" + target # file names, except for extension
    
            ## select particular X features and y targets needed
            os.chdir("./ForModel/ML/"+self.site) # change to model data folder
            featuresNeeded = pd.read_excel(self.site+'_features.xlsx', sheet_name=target) # X features needed
            X_forTarget = X.loc[:, featuresNeeded.transpose().values[0]] # select X features needed
            y_forTarget = y[target] # select particular target y needed
            os.chdir("../../..") # change back to data folder
    
            ## load model
            os.chdir("./Fitted Models/" + self.site)
            regressor = joblib.load(fn+"_XGB.json")
            os.chdir("../..") # change back to data folder
    
            ## use model to calculate modeled y values
            y_pred = regressor.predict(X_forTarget)
    
            ## concatenate date, y obs, and y predicted
            y_concat = pd.DataFrame({'Date': np.array(X.loc[:, "Date"].values), # date
                                     'observed': np.array(y_forTarget.values), # y obs
                                     'modeled': y_pred})
    
            ## write data to file
            X_forTarget.to_excel(writer, sheet_name="X_"+target, index=False) # X data
            y_concat.to_excel(writer, sheet_name="y_"+target, index=False) # y data

        writer.close() # save