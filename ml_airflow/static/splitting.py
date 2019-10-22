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
from sklearn.model_selection import KFold

#####################################################################################
# Class and Constructor
#####################################################################################

def mla_train_test_split(data, target, test_ratio, random_state = 42):

	train = data.loc[:, data.columns != target]
	test = data.loc[:, target]

	print(train)
	print(test)
	print(test_ratio)

	X_train, X_test, y_train, y_test = train_test_split(train, test, 
														test_size = test_ratio, 
														random_state = random_state)

	return X_train, X_test, y_train, y_test

def mla_k_fold(data, target, k, random_state = 42, shuffle = True):

	kf = KFold(n_splits = k, random_state = random_state, shuffle = shuffle)
	folds = {}

	i = 0
	for train_index, test_index in kf.split(data):
		folds['fold' + str(i)] = {}

		folds['fold' + str(i)]['X_train'] = data.loc[train_index:, data.columns != target]
		folds['fold' + str(i)]['y_train'] = data.loc[train_index:, target]

		folds['fold' + str(i)]['X_test'] = data.loc[test_index:, data.columns != target]
		folds['fold' + str(i)]['y_test'] = data.loc[test_index:, target]



	return folds