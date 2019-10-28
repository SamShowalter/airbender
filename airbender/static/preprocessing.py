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

def impute(data, method = "median", prefit = None):

	fill_na_vals = None
	if prefit:
		fill_na_vals = prefit['fill_na_vals']

	else:
		if method == 'median':
			fill_na_vals = data.median()
		elif method == 'mean':
			fill_na_vals = data.mean()

	data = data.fillna(fill_na_vals)

	if prefit:
		return data
	else:
		return data, {'fill_na_vals': fill_na_vals}












