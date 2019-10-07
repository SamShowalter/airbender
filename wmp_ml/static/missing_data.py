#####################################################################################
#
#
# 	Missing Data Preprocessing for Airflow
#  
#	Author: Sam Showalter
#	Date: October 3, 2018
#
#####################################################################################


#####################################################################################
# External Library and Module Imports
#####################################################################################


#####################################################################################
# Class and Constructor
#####################################################################################


def find_missing_data(self, data):
	nulls = data.isnull().sum()
	null_cols = nulls[nulls>0].index.tolist()
	return null_cols

def dynamic_impute(self, data, method = 'median'):
	return

def fixed_impute(self, data, value = 0):
	return