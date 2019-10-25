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

def train_test_split(data, target, test_ratio, random_state = 42):


	train, test = train_test_split(data, 
										test_size = test_ratio, 
										random_state = random_state)

	return train, test, target

def k_fold(data, target, k, random_state = 42, shuffle = True):

	kf = KFold(n_splits = k, random_state = random_state, shuffle = shuffle)
	folds = {}

	i = 0
	for train_index, test_index in kf.split(data):
		folds['fold' + str(i)] = {}

		folds['fold' + str(i)]['train'] = data.loc[train_index,]
		folds['fold' + str(i)]['X_test'] = data.loc[test_index,]


	return folds