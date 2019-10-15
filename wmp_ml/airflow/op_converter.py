#####################################################################################
#
#
# 	Operator Converter for Airflow Transformation
#  
#	Author: Sam Showalter
#	Date: October 3, 2018
#
#####################################################################################


#####################################################################################
# External Library and Module Imports
#####################################################################################

# System and OS
import os
import sys

#Time 
from datetime import datetime, timedelta

#String conversion for dictionaries
import json
import inspect

#####################################################################################
# Class and Constructor
#####################################################################################


def evaluation_operation(func, model_id, params, dag, **kwargs):

	ti = kwargs['ti']

	preds = ti.xcom_pull(task_id = model_id)
	y_test = ti.xcom_pull(key = 'y_test')

	return func(y_test, preds, **params)


def merge_data_operation(merge_ids, dag, **kwargs):

	ti = kwargs['ti']
	data = xcom_pull(key = 'data')


	for column_name, task_id in merge_ids.item():
		task_data = ti.xcom_pull(task_id = task_id)
		data[column_name] = task_data

	ti.xcom_push(key = 'data', value = data)

def bulk_data_operation(func, params, dag, **kwargs):
	ti = kwargs['ti']

	data = ti.xcom_pull(key = 'data')
	data = func(data, **params)

	ti.xcom_push(key = 'data', value = data)


def col_data_operation(func, column_data_id, inherits, params, dag, **kwargs):
	ti = kwargs['ti']
	data = None

	if not inherits:
		data = ti.xcom_pull(key = 'data')
		data = data.loc[:, column_data_id]
	else:
		data = ti.xcom_pull(task_id = column_data_id) 

	return func(data, **params)

def fit_operation(model, params, dag, **kwargs):
	ti = kwargs['ti']

	X_train = ti.xcom_pull(key = "X_train")
	y_train = ti.xcom_pull(key = "y_train")

	model = model(**params)
	model.fit(X_train, y_train)

	return model

def read_data_operation(func, filepath, params):

	ti = kwargs['ti']

	data = func(filepath, **params)

	ti.xcom_push(key = 'data', value = data)


def predict_operation(model, params, dag, **kwargs):
	ti = kwargs['ti']

	X_test = ti.xcom_pull(key = "X_test")

	model = ti.xcom_pull(task_id = model)
	
	predictions = model.predict(X_text)

	return predictions


def split_operation(func, params, dag, **kwargs):

	ti = kwargs['ti']

	data = ti.xcom_pull(key = 'data')

	X_train, X_test, y_train, y_test = func(data, **params)

	ti.xcom_push(key = 'X_train', value = X_train)
	ti.xcom_push(key = 'X_test', value = X_test)
	ti.xcom_push(key = 'y_train', value = y_train)
	ti.xcom_push(key = 'y_test', value = y_test)


def k_fold_operation(func, params, dag, **kwargs):
	pass

def void_operation(func, params, dag, **kwargs):

	return func(data, **params)

