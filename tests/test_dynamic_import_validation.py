#####################################################################################
#
#
# 	Custom ML Airflow Feature Engineering
#  
#	Author: Sam Showalter
#	Date: October 6, 2018
#
#####################################################################################


#####################################################################################
# External Library and Module Imports
#####################################################################################

#Helper packages
from itertools import chain
import sys

#Data packages
import pytest

#Airbender
import os
sys.path.append(os.path.abspath(os.path.join(__file__, "../../")))
import airbender
from airbender.dag.generator import DagGenerator
from airbender.dag.layers import DagLayer


## Imports for TestDynamicImportCompleteness
import pandas as pd
from sklearn.metrics import precision_score, recall_score, accuracy_score, f1_score
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from airbender.static.feature_engineering import normalize_values, winsorize, encode_labels
from airbender.static.preprocessing import impute
from airbender.static.splitting import train_test_split

#####################################################################################
# Fixtures -- Validation
#####################################################################################

@pytest.fixture
def obtain_correct_import_validation():
	def _obtain_correct_import_validation(config):
		dg = DagGenerator(config)
		dg.detect_external_imports()
		return set(chain(*dg.import_dict.values()))

	return _obtain_correct_import_validation



#####################################################################################
# Test Class: Test Validation
#####################################################################################

class TestDynamicImportCompleteness:

	data_sources = {'iris':         #Tag
   		DagLayer(
            {
             'https://raw.githubusercontent.com/SamShowalter/WMP_training/master/airbender_iris_demo.csv': \
             {pd.read_csv: {'sep': ','}},
            }
           )
               }

	splitting = {'split':

	                DagLayer({'sklearn': {train_test_split: {"target": "flower_label",
	                                                        "test_ratio": 0.25,
	                                                        "random_state": 42}
	                                     }
	                        })
	            }

	preprocessing = {'missing_data':

	                        DagLayer({
	                                    # tag name             # Operator Family
	                                    'median_impute':       {impute: {'method': 'median'}}
	                                })
	            }


	feature_engineering = {'col_transformations':

	                        DagLayer({
	                                    # Column name             # Operator Family

	                                    'sepal_width':            {normalize_values: None},
	                                    'sepal_length':           None,
	                                    'petal_length':           {winsorize:            {'limits': [0.05, 0.05]},
	                                                               normalize_values:     None},
	                                    #Pass-through
	                                    'petal_width':            None,
	                                    'flower_label':           {encode_labels: None}
	                                })
	                        }

	modeling = {'modeling': 
	            
	            DagLayer({
	                        'LOG': {LogisticRegression:         {'solver':'lbfgs'}},           
	                        'RF':  {RandomForestClassifier:     {'n_estimators': 10}},
	                        'SVM': {SVC:                        {'kernel': 'linear'}}
	                    })  
	       }


	evaluation = {'metrics':
	                
	                 DagLayer({
	                            'acc':            {accuracy_score: None},
	                            'recall':         {precision_score: {'average': 'weighted'}},
	                            'precision':      {recall_score: {'average': 'weighted'}},
	                            'f1':             {f1_score: {'average': 'weighted'}}
	                         })
	         }

	iris_config = {
	            'data_sources':            data_sources,
	            'splitting':               splitting,
	            'preprocessing':           preprocessing,
	            'feature_engineering':     feature_engineering,
	            'modeling':                modeling,
	            'evaluation':              evaluation
	          }

	input_config = { 
	                    'dag_name': "Airbender_Iris_Tutorial",
	                    
	                    'dag':      {
	                                    'owner': 'airbender',
	                                    # 'email': [<EMAIL>, <EMAIL>, ...],
	                                    # 'op_args':{},
	                                    # 'op_kwargs': {},
	                                },
	                                
	                    #DAG configuration we just created
	                    'config' : iris_config
	               }

	correct_imports = set({'from airbender.static.feature_engineering import encode_labels\n',
						 'from airbender.static.feature_engineering import normalize_values\n',
						 'from airbender.static.feature_engineering import winsorize\n',
						 'from airbender.static.preprocessing import impute\n',
						 'from airbender.static.splitting import train_test_split\n',
						 'from pandas.io.parsers import read_csv\n',
						 'from sklearn.ensemble.forest import RandomForestClassifier\n',
						 'from sklearn.linear_model.logistic import LogisticRegression\n',
						 'from sklearn.metrics.classification import accuracy_score\n',
						 'from sklearn.metrics.classification import f1_score\n',
						 'from sklearn.metrics.classification import precision_score\n',
						 'from sklearn.metrics.classification import recall_score\n',
						 'from sklearn.svm.classes import SVC\n'})

	@pytest.mark.parametrize("input_config,correct_imports", 
		                     [(input_config,correct_imports)], 
		                     ids = ["basic"])
	def test_import_completeness(self, obtain_correct_import_validation, 
								 input_config, correct_imports):
		assert obtain_correct_import_validation(input_config) ==\
			   correct_imports

	# @pytest.mark.parametrize("input_config,correct_config", 
	# 	                     [(22,1), (22,22)], 
	# 	                     ids = ["basic", "hard"])
	# def test_import_lists(self, obtain_correct_import_validation, 
	# 							 input_config, correct_config):
	# 	assert obtain_correct_import_validation(input_config) ==\
	# 		   obtain_correct_import_validation(correct_config)
	# 	