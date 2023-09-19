# -*- coding: utf-8 -*-
"""
Created on Sun Jan  8 18:46:25 2023

does feature pre-processing by dropping highly correlated featues and using recursive selection (with RF) to keep the most important features
splits data into 80% train/20% test set

@author: emei3
"""
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import RFECV
import os
import pandas as pd
import numpy as np

def selectFeatures(site, X, y, targetNames, XNeededFeaturesDict):
    ## select features using featurewiz package for each site
    ## site is a string of the site name
    ## X is a dataframe of the set of features to be selected from
    ## y is a dataframe of all the targets
    ## targetNames is a list of strings of all targets (columns of y) to be fit
    ## XNeededFeaturesDict is dictionary with target names as keys and list of pre-filtered features to fit the ML models
    
    # change to processed data folder
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    os.chdir("../../Data/ForModel/ML/"+site)
    
    # pre-allocation
    # for writing selected feature names to file
    fn = site+"_features.xlsx"
    writer2 = pd.ExcelWriter(fn, engine='xlsxwriter')  
    
    # loop through targets to select most relevant features
    for target in targetNames:
        print("selecting features for " + target + " at " + site)
        
        XTemp = X[XNeededFeaturesDict[target]] # retrieve pre-selected features
        XTemp = XTemp.drop("Date", axis=1) # remove date column
        yTemp = y.loc[:, target].to_frame() # retrieve each target 
        
        ## find and remove highly correlated variables
        drop_col_names = corrX(XTemp) # use function to find >0.95 corr variables to drop
        print("highly correlated features to be removed for " + target + ": " + ', '.join(drop_col_names))
        XTemp = XTemp.drop(drop_col_names, axis=1)
        
        # remove nan
        inds = yTemp.notnull().to_numpy()
        XTemp = XTemp.loc[inds, :]
        yTemp = yTemp.loc[inds]
        
        ## grab training and test data
        X_train, X_test, y_train, y_test = train_test_split(
        XTemp, yTemp, test_size=0.2, random_state=13) # 80% training 20% testing
        
        ## grab most relevant features
        estimator = RandomForestRegressor(n_estimators=200,
                                          min_samples_leaf=2,
                                          max_features='sqrt') # model used to do feature importance, features used to avoid overfit
        selector = RFECV(estimator, step=1, cv=4, scoring='neg_mean_absolute_error') # recursive cv feature selection
        selector.fit(X_train, y_train.values.ravel()) # select from training data
        selectedFeatures = selector.get_feature_names_out() # selected features
        
        # force day of year into the features if not already in there
        if "dayofyear" not in selectedFeatures:
            selectedFeatures = np.append(selectedFeatures, ["dayofyear"]) # add to list
        
        # log statement for features selected
        print(str(selector.n_features_) + " features for " + target + ": " + ', '.join(selectedFeatures))
            
        ## grab only relevant features from training and test sets
        X_train_selected = X_train[selectedFeatures]
        X_test_selected = X_test[selectedFeatures]
        
        ## write training and test data to file
        fn = site + "_" + target + ".xlsx"
        writer = pd.ExcelWriter(fn, engine='xlsxwriter') # Pandas excel writer
        
        # training data
        X_train_selected.to_excel(writer, sheet_name='X_train', index=False)
        y_train.to_excel(writer, sheet_name='y_train', index=False)
        # test data
        X_test_selected.to_excel(writer, sheet_name='X_test', index=False)
        y_test.to_excel(writer, sheet_name='y_test', index=False)
        
        writer.close() #close file
        
        ## write selected features to separate file
        selectedFeatures = pd.DataFrame(selectedFeatures) #turn into df for to_excel function
        selectedFeatures.to_excel(writer2, sheet_name=target, index=False)
        
    writer2.close() #save to file
    # change back to data folder
    os.chdir("../../..")
    
def corrX(df, cut = 0.95) :
    ## gives names of variables that have higher than 0.95 correlation that should be cut
    ## taken directly from https://towardsdatascience.com/are-you-dropping-too-many-correlated-features-d1c96654abe6    
       
    # Get correlation matrix and upper triagle
    corr_mtx = df.corr().abs()
    avg_corr = corr_mtx.mean(axis = 1)
    up = corr_mtx.where(np.triu(np.ones(corr_mtx.shape), k=1).astype(np.bool))
    
    dropcols = list()
    
    res = pd.DataFrame(columns=(['v1', 'v2', 'v1.target', 
                                 'v2.target','corr', 'drop' ]))
    
    for row in range(len(up)-1):
        col_idx = row + 1
        for col in range (col_idx, len(up)):
            if(corr_mtx.iloc[row, col] > cut):
                if(avg_corr.iloc[row] > avg_corr.iloc[col]): 
                    dropcols.append(row)
                    drop = corr_mtx.columns[row]
                else: 
                    dropcols.append(col)
                    drop = corr_mtx.columns[col]
                
                s = pd.Series([ corr_mtx.index[row],
                up.columns[col],
                avg_corr[row],
                avg_corr[col],
                up.iloc[row,col],
                drop],
                index = res.columns)
                
                # add data to drop to frame
                res = pd.concat([res, s.to_frame().transpose()], axis=0)
                #res = pd.concat([res, s], ignore_index = False, axis=0, join='outer')
                # res = res.append(s, ignore_index = True) # tried to replace with concat, but throwing weird error.
    
    dropcols_names = calcDrop(res)
    
    return(dropcols_names)

def calcDrop(res):
    # All variables with correlation > cutoff
    all_corr_vars = list(set(res['v1'].tolist() + res['v2'].tolist()))
    
    # All unique variables in drop column
    poss_drop = list(set(res['drop'].tolist()))

    # Keep any variable not in drop column
    keep = list(set(all_corr_vars).difference(set(poss_drop)))
     
    # Drop any variables in same row as a keep variable
    p = res[ res['v1'].isin(keep)  | res['v2'].isin(keep) ][['v1', 'v2']]
    q = list(set(p['v1'].tolist() + p['v2'].tolist()))
    drop = (list(set(q).difference(set(keep))))

    # Remove drop variables from possible drop 
    poss_drop = list(set(poss_drop).difference(set(drop)))
    
    # subset res dataframe to include possible drop pairs
    m = res[ res['v1'].isin(poss_drop)  | res['v2'].isin(poss_drop) ][['v1', 'v2','drop']]
        
    # remove rows that are decided (drop), take set and add to drops
    more_drop = set(list(m[~m['v1'].isin(drop) & ~m['v2'].isin(drop)]['drop']))
    for item in more_drop:
        drop.append(item)
         
    return drop