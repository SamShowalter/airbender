#####################################################################################
#
#
# 	Preprocessing for ML Airflow
#  
#	Author: Sam Showalter
#	Date: October 3, 2018
#
#####################################################################################


#####################################################################################
# External Library and Module Imports
#####################################################################################

import pandas as pd 
import numpy as np 
from scipy.stats.mstats import winsorize

#####################################################################################
# Class and Constructor
#####################################################################################

def mla_impute(data, method = "median"):

    nulls = data.isnull().sum()
    null_names = nulls[nulls > 0].index.tolist()

    for name in null_names:
        if method == "median":
            data[name].fillna(data[name].median(), inplace = True)

    return data

def mla_winsorize(data, col_names, limits = [0.05, 0.05]):
    for column in col_names:
        data[column] = winsorize(data[column], limits = limits)
    
    return data






