#####################################################################################
#
#
# 	Airbender Generated DAG Experiment: Airbender_Iris_Tutorial 
#  
#	Author: airbender 
#	Date: 10-28-2019--15.26.01
#
#####################################################################################


#####################################################################################
# External Library and Module Imports
#####################################################################################

# System and OS
import os
import sys

#Delete this once package matures
#For now, change to your own path
sys.path.append(os.path.abspath(os.path.join(__file__, "../../../repos/airbender/")))


# Airflow DAG
from airflow import DAG

#Airflow Operators
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator

#Airflow Utils
from airflow.utils.helpers import *

#Datetime information
from datetime import datetime, timedelta

##############################################################################
# External Package Imports Automagically Detected
##############################################################################

## Import statements for PANDAS.IO.PARSERS
from pandas.io.parsers import read_csv

## Import statements for AIRBENDER.STATIC.SPLITTING
from airbender.static.splitting import train_test_split

## Import statements for AIRBENDER.STATIC.PREPROCESSING
from airbender.static.preprocessing import impute

## Import statements for AIRBENDER.STATIC.FEATURE_ENGINEERING
from airbender.static.feature_engineering import normalize_values
from airbender.static.feature_engineering import winsorize
from airbender.static.feature_engineering import encode_labels

## Import statements for SKLEARN.LINEAR_MODEL.LOGISTIC
from sklearn.linear_model.logistic import LogisticRegression

## Import statements for SKLEARN.ENSEMBLE.FOREST
from sklearn.ensemble.forest import RandomForestClassifier

## Import statements for SKLEARN.SVM.CLASSES
from sklearn.svm.classes import SVC

## Import statements for SKLEARN.METRICS.CLASSIFICATION
from sklearn.metrics.classification import accuracy_score
from sklearn.metrics.classification import precision_score
from sklearn.metrics.classification import recall_score
from sklearn.metrics.classification import f1_score

## Import statements for AIRBENDER.AIRFLOW.OP_CONVERTER
from airbender.airflow.op_converter import read_data_operation
from airbender.airflow.op_converter import split_operation
from airbender.airflow.op_converter import bulk_data_operation
from airbender.airflow.op_converter import col_data_operation
from airbender.airflow.op_converter import merge_data_operation
from airbender.airflow.op_converter import model_split_operation
from airbender.airflow.op_converter import fit_operation
from airbender.airflow.op_converter import predict_operation
from airbender.airflow.op_converter import evaluation_operation
from airbender.airflow.op_converter import merge_metrics_operation



#####################################################################################
# DAG Construction
#####################################################################################

default_args = {
    "owner": "airbender",
    "depends_on_past": False,
    "email": [
        "airflow@example.com"
    ],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "op_args": {},
    "op_kwargs": {},
    "params": {}
}

dag = DAG( 'Airbender_Iris_Tutorial', 
		   default_args=default_args,
		   start_date = datetime.today(), 
		   schedule_interval='@once')

#####################################################################################
# Operators Construction
#####################################################################################



###########################################################
# Operators pertaining to DATA_SOURCES dag layer with tag DS_IRIS
###########################################################

airbender_iris_demo_csv_read_csv = PythonOperator( 
							task_id='airbender_iris_demo_csv_read_csv',
							provide_context=True,
							python_callable=read_data_operation,
							params = {'filepath': 'https://raw.githubusercontent.com/SamShowalter/airbender/master/tutorials/iris/airbender_iris_demo.csv',
							 'func': read_csv,
							 'params': {'sep': ','}},
							dag = dag)

					

###########################################################
# Operators pertaining to SPLITTING dag layer with tag S_SPLIT
###########################################################

sklearn_train_test_split = PythonOperator( 
							task_id='sklearn_train_test_split',
							provide_context=True,
							python_callable=split_operation,
							params = {'func': train_test_split,
							 'params': {'random_state': 42, 'target': 'flower_label', 'test_ratio': 0.25}},
							dag = dag)

					

###########################################################
# Operators pertaining to PREPROCESSING dag layer with tag P_MISSING_DATA
###########################################################

median_impute_train_impute = PythonOperator( 
							task_id='median_impute_train_impute',
							provide_context=True,
							python_callable=bulk_data_operation,
							params = {'func': impute,
							 'params': {'method': 'median'},
							 'split': 'train'},
							dag = dag)

					
median_impute_test_impute = PythonOperator( 
							task_id='median_impute_test_impute',
							provide_context=True,
							python_callable=bulk_data_operation,
							params = {'func': impute,
							 'params': {'method': 'median'},
							 'split': 'test'},
							dag = dag)

					

###########################################################
# Operators pertaining to FEATURE_ENGINEERING dag layer with tag FE_COL_TRANSFORMATIONS
###########################################################

sepal_width_train_normalize_values = PythonOperator( 
							task_id='sepal_width_train_normalize_values',
							provide_context=True,
							python_callable=col_data_operation,
							params = {'column_data_id': 'sepal_width',
							 'func': normalize_values,
							 'inherits': False,
							 'params': {},
							 'split': 'train'},
							dag = dag)

					
petal_length_train_winsorize = PythonOperator( 
							task_id='petal_length_train_winsorize',
							provide_context=True,
							python_callable=col_data_operation,
							params = {'column_data_id': 'petal_length',
							 'func': winsorize,
							 'inherits': False,
							 'params': {'limits': [0.05, 0.05]},
							 'split': 'train'},
							dag = dag)

					
petal_length_train_normalize_values = PythonOperator( 
							task_id='petal_length_train_normalize_values',
							provide_context=True,
							python_callable=col_data_operation,
							params = {'column_data_id': 'petal_length_train_winsorize',
							 'func': normalize_values,
							 'inherits': True,
							 'params': {},
							 'split': 'train'},
							dag = dag)

					
flower_label_train_encode_labels = PythonOperator( 
							task_id='flower_label_train_encode_labels',
							provide_context=True,
							python_callable=col_data_operation,
							params = {'column_data_id': 'flower_label',
							 'func': encode_labels,
							 'inherits': False,
							 'params': {},
							 'split': 'train'},
							dag = dag)

					
fe_col_transformations_train_merge_layer = PythonOperator( 
							task_id='fe_col_transformations_train_merge_layer',
							provide_context=True,
							python_callable=merge_data_operation,
							params = {'merge_ids': ['sepal_width_train_normalize_values',
							               'petal_length_train_normalize_values',
							               'flower_label_train_encode_labels'],
							 'params': {},
							 'pass_through_cols': ['sepal_length', 'petal_width'],
							 'split': 'train'},
							dag = dag)

					
sepal_width_test_normalize_values = PythonOperator( 
							task_id='sepal_width_test_normalize_values',
							provide_context=True,
							python_callable=col_data_operation,
							params = {'column_data_id': 'sepal_width',
							 'func': normalize_values,
							 'inherits': False,
							 'params': {},
							 'split': 'test'},
							dag = dag)

					
petal_length_test_winsorize = PythonOperator( 
							task_id='petal_length_test_winsorize',
							provide_context=True,
							python_callable=col_data_operation,
							params = {'column_data_id': 'petal_length',
							 'func': winsorize,
							 'inherits': False,
							 'params': {'limits': [0.05, 0.05]},
							 'split': 'test'},
							dag = dag)

					
petal_length_test_normalize_values = PythonOperator( 
							task_id='petal_length_test_normalize_values',
							provide_context=True,
							python_callable=col_data_operation,
							params = {'column_data_id': 'petal_length_test_winsorize',
							 'func': normalize_values,
							 'inherits': True,
							 'params': {},
							 'split': 'test'},
							dag = dag)

					
flower_label_test_encode_labels = PythonOperator( 
							task_id='flower_label_test_encode_labels',
							provide_context=True,
							python_callable=col_data_operation,
							params = {'column_data_id': 'flower_label',
							 'func': encode_labels,
							 'inherits': False,
							 'params': {},
							 'split': 'test'},
							dag = dag)

					
fe_col_transformations_test_merge_layer = PythonOperator( 
							task_id='fe_col_transformations_test_merge_layer',
							provide_context=True,
							python_callable=merge_data_operation,
							params = {'merge_ids': ['sepal_width_test_normalize_values',
							               'petal_length_test_normalize_values',
							               'flower_label_test_encode_labels'],
							 'params': {},
							 'pass_through_cols': ['sepal_length', 'petal_width'],
							 'split': 'test'},
							dag = dag)

					

###########################################################
# Operators pertaining to MODELING dag layer with tag M_MODELING
###########################################################

model_data_split = PythonOperator( 
							task_id='model_data_split',
							provide_context=True,
							python_callable=model_split_operation,
							params = {'params': {}},
							dag = dag)

					
LOG_fit = PythonOperator( 
							task_id='LOG_fit',
							provide_context=True,
							python_callable=fit_operation,
							params = {'model': LogisticRegression,
							 'params': {'solver': 'lbfgs'}},
							dag = dag)

					
LOG_predict = PythonOperator( 
							task_id='LOG_predict',
							provide_context=True,
							python_callable=predict_operation,
							params = {'model': 'LOG_fit', 'params': {'solver': 'lbfgs'}},
							dag = dag)

					
RF_fit = PythonOperator( 
							task_id='RF_fit',
							provide_context=True,
							python_callable=fit_operation,
							params = {'model': RandomForestClassifier,
							 'params': {'n_estimators': 10}},
							dag = dag)

					
RF_predict = PythonOperator( 
							task_id='RF_predict',
							provide_context=True,
							python_callable=predict_operation,
							params = {'model': 'RF_fit', 'params': {'n_estimators': 10}},
							dag = dag)

					
SVM_fit = PythonOperator( 
							task_id='SVM_fit',
							provide_context=True,
							python_callable=fit_operation,
							params = {'model': SVC, 'params': {'kernel': 'linear'}},
							dag = dag)

					
SVM_predict = PythonOperator( 
							task_id='SVM_predict',
							provide_context=True,
							python_callable=predict_operation,
							params = {'model': 'SVM_fit', 'params': {'kernel': 'linear'}},
							dag = dag)

					

###########################################################
# Operators pertaining to EVALUATION dag layer with tag E_METRICS
###########################################################

LOG_predict_acc = PythonOperator( 
							task_id='LOG_predict_acc',
							provide_context=True,
							python_callable=evaluation_operation,
							params = {'func': accuracy_score,
							 'model_id': 'LOG_predict',
							 'params': {}},
							dag = dag)

					
LOG_predict_recall = PythonOperator( 
							task_id='LOG_predict_recall',
							provide_context=True,
							python_callable=evaluation_operation,
							params = {'func': precision_score,
							 'model_id': 'LOG_predict',
							 'params': {'average': 'weighted'}},
							dag = dag)

					
LOG_predict_precision = PythonOperator( 
							task_id='LOG_predict_precision',
							provide_context=True,
							python_callable=evaluation_operation,
							params = {'func': recall_score,
							 'model_id': 'LOG_predict',
							 'params': {'average': 'weighted'}},
							dag = dag)

					
LOG_predict_f1 = PythonOperator( 
							task_id='LOG_predict_f1',
							provide_context=True,
							python_callable=evaluation_operation,
							params = {'func': f1_score,
							 'model_id': 'LOG_predict',
							 'params': {'average': 'weighted'}},
							dag = dag)

					
RF_predict_acc = PythonOperator( 
							task_id='RF_predict_acc',
							provide_context=True,
							python_callable=evaluation_operation,
							params = {'func': accuracy_score,
							 'model_id': 'RF_predict',
							 'params': {}},
							dag = dag)

					
RF_predict_recall = PythonOperator( 
							task_id='RF_predict_recall',
							provide_context=True,
							python_callable=evaluation_operation,
							params = {'func': precision_score,
							 'model_id': 'RF_predict',
							 'params': {'average': 'weighted'}},
							dag = dag)

					
RF_predict_precision = PythonOperator( 
							task_id='RF_predict_precision',
							provide_context=True,
							python_callable=evaluation_operation,
							params = {'func': recall_score,
							 'model_id': 'RF_predict',
							 'params': {'average': 'weighted'}},
							dag = dag)

					
RF_predict_f1 = PythonOperator( 
							task_id='RF_predict_f1',
							provide_context=True,
							python_callable=evaluation_operation,
							params = {'func': f1_score,
							 'model_id': 'RF_predict',
							 'params': {'average': 'weighted'}},
							dag = dag)

					
SVM_predict_acc = PythonOperator( 
							task_id='SVM_predict_acc',
							provide_context=True,
							python_callable=evaluation_operation,
							params = {'func': accuracy_score,
							 'model_id': 'SVM_predict',
							 'params': {}},
							dag = dag)

					
SVM_predict_recall = PythonOperator( 
							task_id='SVM_predict_recall',
							provide_context=True,
							python_callable=evaluation_operation,
							params = {'func': precision_score,
							 'model_id': 'SVM_predict',
							 'params': {'average': 'weighted'}},
							dag = dag)

					
SVM_predict_precision = PythonOperator( 
							task_id='SVM_predict_precision',
							provide_context=True,
							python_callable=evaluation_operation,
							params = {'func': recall_score,
							 'model_id': 'SVM_predict',
							 'params': {'average': 'weighted'}},
							dag = dag)

					
SVM_predict_f1 = PythonOperator( 
							task_id='SVM_predict_f1',
							provide_context=True,
							python_callable=evaluation_operation,
							params = {'func': f1_score,
							 'model_id': 'SVM_predict',
							 'params': {'average': 'weighted'}},
							dag = dag)

					
e_metrics_LOG_predict_merge_metrics = PythonOperator( 
							task_id='e_metrics_LOG_predict_merge_metrics',
							provide_context=True,
							python_callable=merge_metrics_operation,
							params = {'merge_ids': ['LOG_predict_acc',
							               'LOG_predict_recall',
							               'LOG_predict_precision',
							               'LOG_predict_f1'],
							 'model': 'LOG_predict',
							 'params': {}},
							dag = dag)

					
e_metrics_RF_predict_merge_metrics = PythonOperator( 
							task_id='e_metrics_RF_predict_merge_metrics',
							provide_context=True,
							python_callable=merge_metrics_operation,
							params = {'merge_ids': ['RF_predict_acc',
							               'RF_predict_recall',
							               'RF_predict_precision',
							               'RF_predict_f1'],
							 'model': 'RF_predict',
							 'params': {}},
							dag = dag)

					
e_metrics_SVM_predict_merge_metrics = PythonOperator( 
							task_id='e_metrics_SVM_predict_merge_metrics',
							provide_context=True,
							python_callable=merge_metrics_operation,
							params = {'merge_ids': ['SVM_predict_acc',
							               'SVM_predict_recall',
							               'SVM_predict_precision',
							               'SVM_predict_f1'],
							 'model': 'SVM_predict',
							 'params': {}},
							dag = dag)

					

#####################################################################################
# Operator Family Construction
#####################################################################################



###########################################################
# Operator families pertaining to DATA_SOURCES dag layer with tag DS_IRIS
###########################################################

ds_iris_airbender_iris_demo_csv = airbender_iris_demo_csv_read_csv

###########################################################
# Operator families pertaining to SPLITTING dag layer with tag S_SPLIT
###########################################################

s_split_sklearn = sklearn_train_test_split

###########################################################
# Operator families pertaining to PREPROCESSING dag layer with tag P_MISSING_DATA
###########################################################

train_p_missing_data_median_impute = median_impute_train_impute
test_p_missing_data_median_impute = median_impute_test_impute

###########################################################
# Operator families pertaining to FEATURE_ENGINEERING dag layer with tag FE_COL_TRANSFORMATIONS
###########################################################

train_fe_col_transformations_sepal_width = sepal_width_train_normalize_values
train_fe_col_transformations_petal_length = petal_length_train_winsorize >> petal_length_train_normalize_values
train_fe_col_transformations_flower_label = flower_label_train_encode_labels
train_fe_col_transformations_merge_layer = fe_col_transformations_train_merge_layer
test_fe_col_transformations_sepal_width = sepal_width_test_normalize_values
test_fe_col_transformations_petal_length = petal_length_test_winsorize >> petal_length_test_normalize_values
test_fe_col_transformations_flower_label = flower_label_test_encode_labels
test_fe_col_transformations_merge_layer = fe_col_transformations_test_merge_layer

###########################################################
# Operator families pertaining to MODELING dag layer with tag M_MODELING
###########################################################

m_modeling_model_data_split = model_data_split
m_modeling_LOG = LOG_fit >> LOG_predict
m_modeling_RF = RF_fit >> RF_predict
m_modeling_SVM = SVM_fit >> SVM_predict

###########################################################
# Operator families pertaining to EVALUATION dag layer with tag E_METRICS
###########################################################


## Families pertaining to LOG_predict conditional sublayer

LOG_predict_e_metrics_acc = LOG_predict_acc
LOG_predict_e_metrics_recall = LOG_predict_recall
LOG_predict_e_metrics_precision = LOG_predict_precision
LOG_predict_e_metrics_f1 = LOG_predict_f1

## Families pertaining to RF_predict conditional sublayer

RF_predict_e_metrics_acc = RF_predict_acc
RF_predict_e_metrics_recall = RF_predict_recall
RF_predict_e_metrics_precision = RF_predict_precision
RF_predict_e_metrics_f1 = RF_predict_f1

## Families pertaining to SVM_predict conditional sublayer

SVM_predict_e_metrics_acc = SVM_predict_acc
SVM_predict_e_metrics_recall = SVM_predict_recall
SVM_predict_e_metrics_precision = SVM_predict_precision
SVM_predict_e_metrics_f1 = SVM_predict_f1

## Families pertaining to LOG_predict conditional sublayer

LOG_predict_e_metrics_merge_metrics = e_metrics_LOG_predict_merge_metrics

## Families pertaining to RF_predict conditional sublayer

RF_predict_e_metrics_merge_metrics = e_metrics_RF_predict_merge_metrics

## Families pertaining to SVM_predict conditional sublayer

SVM_predict_e_metrics_merge_metrics = e_metrics_SVM_predict_merge_metrics

#####################################################################################
# Layers Construction
#####################################################################################



###########################################################
# Sublayer pertaining to DATA_SOURCES dag layer with tag DS_IRIS
###########################################################

## Generating HEAD of DS_IRIS_CORE sublayer for DS_IRIS dag layer
ds_iris_core_head = airbender_iris_demo_csv_read_csv

## Generating TAIL of DS_IRIS_CORE sublayer for DS_IRIS dag layer
ds_iris_core_tail = ds_iris_core_head


###########################################################
# Sublayer pertaining to SPLITTING dag layer with tag S_SPLIT
###########################################################

## Generating HEAD of S_SPLIT_CORE sublayer for S_SPLIT dag layer
s_split_core_head = sklearn_train_test_split

## Generating TAIL of S_SPLIT_CORE sublayer for S_SPLIT dag layer
s_split_core_tail = s_split_core_head


###########################################################
# Sublayer pertaining to PREPROCESSING dag layer with tag P_MISSING_DATA
###########################################################

## Generating HEAD of P_MISSING_DATA_CORE_TRAIN sublayer for P_MISSING_DATA dag layer
p_missing_data_core_train_head = median_impute_train_impute

## Generating TAIL of P_MISSING_DATA_CORE_TRAIN sublayer for P_MISSING_DATA dag layer
p_missing_data_core_train_tail = p_missing_data_core_train_head

## Generating HEAD of P_MISSING_DATA_CORE_TEST sublayer for P_MISSING_DATA dag layer
p_missing_data_core_test_head = median_impute_test_impute

## Generating TAIL of P_MISSING_DATA_CORE_TEST sublayer for P_MISSING_DATA dag layer
p_missing_data_core_test_tail = p_missing_data_core_test_head

## Connecting all sublayers (if > 1) for PREPROCESSING dag layer with tag P_MISSING_DATA
p_missing_data_core_train_head >> p_missing_data_core_test_tail

###########################################################
# Sublayer pertaining to FEATURE_ENGINEERING dag layer with tag FE_COL_TRANSFORMATIONS
###########################################################

## Generating HEAD of FE_COL_TRANSFORMATIONS_CORE_TRAIN sublayer for FE_COL_TRANSFORMATIONS dag layer
fe_col_transformations_core_train_head = [sepal_width_train_normalize_values,
						 petal_length_train_normalize_values,
						 flower_label_train_encode_labels]

## Generating TAIL of FE_COL_TRANSFORMATIONS_CORE_TRAIN sublayer for FE_COL_TRANSFORMATIONS dag layer
fe_col_transformations_core_train_tail = [sepal_width_train_normalize_values,
						 petal_length_train_winsorize,
						 flower_label_train_encode_labels]

## Generating HEAD of FE_COL_TRANSFORMATIONS_MERGE_LAYERTRAIN sublayer for FE_COL_TRANSFORMATIONS dag layer
fe_col_transformations_merge_layertrain_head = fe_col_transformations_train_merge_layer

## Generating TAIL of FE_COL_TRANSFORMATIONS_MERGE_LAYERTRAIN sublayer for FE_COL_TRANSFORMATIONS dag layer
fe_col_transformations_merge_layertrain_tail = fe_col_transformations_merge_layertrain_head

## Generating HEAD of FE_COL_TRANSFORMATIONS_CORE_TEST sublayer for FE_COL_TRANSFORMATIONS dag layer
fe_col_transformations_core_test_head = [sepal_width_test_normalize_values,
						 petal_length_test_normalize_values,
						 flower_label_test_encode_labels]

## Generating TAIL of FE_COL_TRANSFORMATIONS_CORE_TEST sublayer for FE_COL_TRANSFORMATIONS dag layer
fe_col_transformations_core_test_tail = [sepal_width_test_normalize_values,
						 petal_length_test_winsorize,
						 flower_label_test_encode_labels]

## Generating HEAD of FE_COL_TRANSFORMATIONS_MERGE_LAYERTEST sublayer for FE_COL_TRANSFORMATIONS dag layer
fe_col_transformations_merge_layertest_head = fe_col_transformations_test_merge_layer

## Generating TAIL of FE_COL_TRANSFORMATIONS_MERGE_LAYERTEST sublayer for FE_COL_TRANSFORMATIONS dag layer
fe_col_transformations_merge_layertest_tail = fe_col_transformations_merge_layertest_head

## Connecting all sublayers (if > 1) for FEATURE_ENGINEERING dag layer with tag FE_COL_TRANSFORMATIONS
fe_col_transformations_core_train_head >> fe_col_transformations_merge_layertrain_tail
fe_col_transformations_merge_layertrain_head >> fe_col_transformations_core_test_tail
fe_col_transformations_core_test_head >> fe_col_transformations_merge_layertest_tail

###########################################################
# Sublayer pertaining to MODELING dag layer with tag M_MODELING
###########################################################

## Generating HEAD of M_MODELING_MODEL_DATA_SPLIT sublayer for M_MODELING dag layer
m_modeling_model_data_split_head = model_data_split

## Generating TAIL of M_MODELING_MODEL_DATA_SPLIT sublayer for M_MODELING dag layer
m_modeling_model_data_split_tail = m_modeling_model_data_split_head

## Generating HEAD of M_MODELING_CORE sublayer for M_MODELING dag layer
m_modeling_core_head = [LOG_predict, RF_predict, SVM_predict]

## Generating TAIL of M_MODELING_CORE sublayer for M_MODELING dag layer
m_modeling_core_tail = [LOG_fit, RF_fit, SVM_fit]

## Connecting all sublayers (if > 1) for MODELING dag layer with tag M_MODELING
m_modeling_model_data_split_head >> m_modeling_core_tail

###########################################################
# Sublayer pertaining to EVALUATION dag layer with tag E_METRICS
###########################################################

## Generating HEAD of E_METRICS_LOG_PREDICT sublayer for E_METRICS dag layer
e_metrics_LOG_predict_head = [LOG_predict_acc,
						 LOG_predict_recall,
						 LOG_predict_precision,
						 LOG_predict_f1]

## Generating TAIL of E_METRICS_LOG_PREDICT sublayer for E_METRICS dag layer
e_metrics_LOG_predict_tail = e_metrics_LOG_predict_head

## Generating HEAD of E_METRICS_RF_PREDICT sublayer for E_METRICS dag layer
e_metrics_RF_predict_head = [RF_predict_acc, RF_predict_recall, RF_predict_precision, RF_predict_f1]

## Generating TAIL of E_METRICS_RF_PREDICT sublayer for E_METRICS dag layer
e_metrics_RF_predict_tail = e_metrics_RF_predict_head

## Generating HEAD of E_METRICS_SVM_PREDICT sublayer for E_METRICS dag layer
e_metrics_SVM_predict_head = [SVM_predict_acc,
						 SVM_predict_recall,
						 SVM_predict_precision,
						 SVM_predict_f1]

## Generating TAIL of E_METRICS_SVM_PREDICT sublayer for E_METRICS dag layer
e_metrics_SVM_predict_tail = e_metrics_SVM_predict_head

## Generating HEAD of E_METRICS_MERGE_METRICS_LOG_PREDICT sublayer for E_METRICS dag layer
e_metrics_merge_metrics_LOG_predict_head = e_metrics_LOG_predict_merge_metrics

## Generating TAIL of E_METRICS_MERGE_METRICS_LOG_PREDICT sublayer for E_METRICS dag layer
e_metrics_merge_metrics_LOG_predict_tail = e_metrics_merge_metrics_LOG_predict_head

## Generating HEAD of E_METRICS_MERGE_METRICS_RF_PREDICT sublayer for E_METRICS dag layer
e_metrics_merge_metrics_RF_predict_head = e_metrics_RF_predict_merge_metrics

## Generating TAIL of E_METRICS_MERGE_METRICS_RF_PREDICT sublayer for E_METRICS dag layer
e_metrics_merge_metrics_RF_predict_tail = e_metrics_merge_metrics_RF_predict_head

## Generating HEAD of E_METRICS_MERGE_METRICS_SVM_PREDICT sublayer for E_METRICS dag layer
e_metrics_merge_metrics_SVM_predict_head = e_metrics_SVM_predict_merge_metrics

## Generating TAIL of E_METRICS_MERGE_METRICS_SVM_PREDICT sublayer for E_METRICS dag layer
e_metrics_merge_metrics_SVM_predict_tail = e_metrics_merge_metrics_SVM_predict_head

## Connecting all sublayers (if > 1) for EVALUATION dag layer with tag E_METRICS

## Printing conditional associations for LOG_predict
e_metrics_LOG_predict_head >> e_metrics_merge_metrics_LOG_predict_tail

## Printing conditional associations for RF_predict
e_metrics_RF_predict_head >> e_metrics_merge_metrics_RF_predict_tail

## Printing conditional associations for SVM_predict
e_metrics_SVM_predict_head >> e_metrics_merge_metrics_SVM_predict_tail

#####################################################################################
# Layers Association and Structure
#####################################################################################


ds_iris_core_head >> s_split_core_tail
s_split_core_head >> p_missing_data_core_train_tail
p_missing_data_core_test_head >> fe_col_transformations_core_train_tail
fe_col_transformations_merge_layertest_head >> m_modeling_model_data_split_tail
chain(m_modeling_core_head, [e_metrics_LOG_predict_tail,
 e_metrics_RF_predict_tail,
 e_metrics_SVM_predict_tail])
