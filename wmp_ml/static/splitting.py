#####################################################################################
#
#
# 	Data splitting functions, WMP_ML
#  
#	Author: Sam Showalter
#	Date: October 6, 2018
#
#####################################################################################


#####################################################################################
# External Library and Module Imports
#####################################################################################

from sklearn.model_selection import train_test_split

#####################################################################################
# Class and Constructor
#####################################################################################

def auto_train_test_split(data, target, test_ratio, random_state = 42):

	train = data.loc[:, data.columns != target]
	test = data.loc[:, target]

	X_train, X_test, y_train, y_test = train_test_split(train, test, 
														test_ratio, 
														random_state = random_state)

	return X_train, X_test, y_train, y_test