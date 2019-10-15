#####################################################################################
#
#
# 	Layers for new DAG
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
import copy
import pprint

#Time 
from datetime import datetime, timedelta

#String conversion for dictionaries
import json
import inspect

# Import package specific information
sys.path.append("../../")
from wmp_ml.airflow.op_converter import *


#####################################################################################
# Class and Constructor
#####################################################################################

class DagLayer:

	def __init__(self, layer_config):

		#Configuration dictionary given by user
		#Must be validated before operations can proceed
		self.config = layer_config
		self.validate_config()

		# Parent concept. Influences execution
		# order and tagging
		self.parent = None 

		#Full configuration lineage
		#Used in tagging
		self.lineage = []

		#Parent DAG
		self.dag = None

		#Collection of sublayers
		self.sublayers = []
		
		#Collection of operator families
		self.op_families = {}
		self.family_ids = set()

		#List of IDs needed to merge operators
		#TODO: Replace with op_family and sublayer logic
		self.merge_ids = {}

		#Operator router (defined in function below)
		#TODO: Find a better place for it
		self.op_router = None

		#Storage of all operators in layer
		self.operators = {}

#####################################################################################
# Methods Pertaining to Imports and Validation
#####################################################################################

	def import_layer_operation(self, operation):
		'''
		Dynamically imports a callable or object
		if it is not already provided. See
		the DagGenerator file for more
		information.

		Args:
			operation:				A python object, function, or class

		'''

		#Calls function from dag generator to
		# import methods dynamically
		self.dag.import_dynamically(operation)

	def validate_config(self):
		'''
		Validates configuration to ensure that it is of an 
		acceptable structure. The configuration MUST be
		represented in a python dictionary. Each operation 
		must also be either None, or a sub-dictionary.

		'''

		#Must be a dictionary
		if not isinstance(self.config, dict):
			raise ValueError("Layer configuration object must be a dictionary. Please check your inputs.")

		#For each key, the value must be None or a dictionary
		for key in self.config:
			if (self.config[key] is not None and
				not isinstance(self.config[key], dict)):
					raise ValueError('''Values in layer configuration key-value pairs must be one of the following:
					- None: No additional arguments
					- Dict: Argument dictionary\n\nPlease check inputs.''')


	def generate_tag(self, rank = None):
		'''
		Generates the tag for the entire 
		DagLayer. This is an name derived 
		from the lineage of the DagLayer

		'''

		self.tag = ""
		for i in range(len(self.lineage)):
			if i == 0:
				#First letters of the parent concept
				self.tag += "".join([token[0] for token in self.lineage[i].split("_")])
			else:
				#Full name of the subconcepts
				self.tag += "_" + self.lineage[i]

		#For lists of DagLayers, ensure that
		# you label them in the correct order
		if rank is not None:
			self.tag += "_l" + str(rank)

	def delineate(self, 
				 lineage, 
				 order,
				 subrank,
				 dag):
		'''
		De-lineates the entire DagLayer by attributing
		a parent, lineage, order of execution, and subrank.

		This is useful for defining the dag later

		'''

		#Full lineage and layer parent concept
		self.lineage = lineage
		self.parent = lineage[0]

		#Generates tag for layer
		self.generate_tag(rank)

		#Define execution order
		self.exec_order = order
		if subrank is not None:
			self.exec_order += subrank

		#Attribute the parent DAG
		self.dag = dag 


	def __create_task_id(self, tag_info):
		'''
		Generate a task ID for each operator in DAG
		This takes a list of inputs and concatenates them

		Args:
			tag_info: 				List of tag information

		Returns:
			task_id:				Unique Task ID name
		'''

		#Join tag data
		task_id = "_".join(tag_info)
		
		#Raise an error if this task is already 
		#defined in the dag
		if task_id in self.dag.tasks:
			raise AttributeError("Task with the same name has already been created. Check your inputs")
		
		#Add the task ID to the dag
		self.dag.tasks.add(task_id)

		#Return the task_id
		return task_id

	def __register_model(self, family, model):
		'''
		EXPERIMENTAL: May be a good way to 
		generate all of the tasks for evaluation of 
		Machine Learning models

		Args:
			family:					Model family
			model:					Model object
	
		Returns:
			model:					Model object, unchanged
		'''
		if self.dag.is_callable(model):
			self.dag.models[family] = False 

		return model

	def prime_operator(self, 
						parent, 
						family, 
						family_upstream_task,
						op, 
						params,
						inherits = False):
		'''
		One of the most import functions for the layer. This
		process takes general configuration input and
		re-organizes it based on the parent concept that the
		layer belongs to. It accommodates dynamic parameter
		switching to take input from upstream tasks, generates
		the appropriate task tags, and also accommodates 
		holistic, triggered parsing options.

		Args:
			parent:							Parent concept. Usually self.parent, but can be replaced by holistic parsing.
			family:							Task family. Logical chain of tasks acting on a specific target.
			family_upstream_task:			Upstream task id from same family that may be used to replace params.
			op:								Operator function acting on the specified data (delineated by family). 
											This will be wrapped in a shell function
			params:							Parameters to provide for specific function.

		Kwargs:
			inherits:						Default = False. Determines if task will inherit upstream params.

		Returns:
			op_detail_list:					List of operators (tasks) and their details for execution

		'''
		#Holistic or custom operators may come in as strings
		op_name = op.__name__ if self.dag.is_callable(op) else op

		#Operator router
		#TODO: Find a better place to put this
		self.op_router = \
			{'splitting': 
							{'operator': split_operation, 
							'args': {'func': op,
									'params': params},
							 'task_tag': [self.tag, family, op_name]},
             'data_sources': 
             				{'operator':read_data_operation, 
             				'args': {'func': op, 
             						'params': params,
             						'filepath': family},
             				'task_tag': [self.tag, family, op_name]},
             'preprocessing': 
             				{'operator':bulk_data_operation, 
             				'args': {'func': op,
             						'params': params},
             				'task_tag':[self.tag, family, op_name]},
             'evaluation': 
             				{'operator':evaluation_operation, 
             				'args': {'func': op,
             						 'params': params,
             						 #Figure out model id generation for eval tasks
             						 'model_id': None},
             				'task_tag': [self.tag, family]},
             # Will wait to do any EDA design patterns
             # 'eda': 
             # 				{'operator':bulk_data_operation, 
             # 				'args': {'func'},
             # 				'task_tag':[]},

             'modeling': 
             				{'operator':[('fit',fit_operation), 
             							 ('predict',predict_operation)], 

             				#Registers model for evaluation functions later
             				'args': {'model': self.__register_model(family, op), 
             						'params': params},
             				'arg_xcom_update': ['model'],
             				'task_tag':[self.tag, family]},

             'feature_engineering': 
             				#Airflows op_converter needs to be determined
             				{'operator': col_data_operation, 
             				'args': {'func': op,
             						 'params': params,
             						 'inherits': inherits,
             						 'column_name': family_upstream_task},
             				'holistic': {'merge_layer': #Parent
             								{'merge_cols': #Family
             									{'merge_key': self.tag}}},
             				'task_tag': [self.tag, family, op_name]},

             #HOLISTIC LAYER OPERATIONS START HERE
             'merge_layer': 
             				{'operator': merge_data_operation, 
             				'args': {'params': params,
             				'merge_ids': self.merge_ids},
             				'task_tag': [self.tag, 'merge_layer']}
		}

		#Must be converted to a list to be added to operator list later
		python_callables = self.op_router[parent]['operator']
		if isinstance(python_callables, list):
			python_callables = python_callables
		else:
			python_callables = [python_callables]

		#Initialize the final_operator, upstream task id, and detail list
		op_detail_list = []
		upstream_task_id = None
		final_operator = None

		#Iterate through shell python callables
		for p_callable in python_callables:

			#Copy the initial task tag
			task_id = copy.deepcopy(self.op_router[parent]['task_tag'])

			#If there are upstream tasks that will update xcom arguments
			#Replace the existing arguments with correct values
			#TODO: Put in its own function
			if upstream_task_id is not None:
				for update in self.op_router[parent]['arg_xcom_update']:
					params[update] = upstream_task_id

			#If there are is more than one callable in router queue
			#Update the task IDs to ensure that there is not duplication
			if len(python_callables) > 1:
				task_id.append(p_callable[0])
				final_operator = p_callable[1]
			else:
				final_operator = p_callable

			#Generate the task id for the task
			#And verify it is not a duplicate
			task_id = self.__create_task_id(task_id)

			#Isolate parameters that have been updated with
			#The operator router
			params = self.op_router[parent]['args']
			
			#Dictionary with all task details added to
			#Operator detail dictionary
			op_detail_list.append({'task_id': task_id,
									   'python_callable': final_operator,
									   'params': self.__parse_parameters(params)})
			
			#Update the upstream task_id (for later inheritance)
			upstream_task_id = task_id

		#Return op_detail_list
		return op_detail_list

	def __parse_param_callables(self, params, obj_dict):

		if isinstance(params, dict):

			for k,v in params.items():
				if self.dag.is_callable(k):
					obj_dict[str(k)] = k
				elif self.dag.is_callable(v):
					obj_dict[str(v)] = v

				if isinstance(v, dict):
					self.__parse_param_callables(v, obj_dict)


	def __parse_parameters(self, params):
	    params_str = pprint.pformat(params).replace("\n", "\n" + "\t"*7)
	    obj_dict = {}
	    self.__parse_param_callables(params, obj_dict)
	    
	    for key in obj_dict:
	        params_str = params_str.replace(key, obj_dict[key].__name__)
	    
	    return params_str

	def __add_python_operator(self, task_id,
									python_callable, 
									params):

		template = '''PythonOperator( 
							task_id='{}',
							provide_context=True,
							python_callable={},
							params = {},
							dag = dag)\n
					'''

		self.dag.import_dynamically(python_callable)

		#Replaces the variable name to be shorter, but there should be no overlap
		shortened_id = task_id.replace(self.tag + "_", "")
		self.operators[shortened_id] =  "\n" + shortened_id +\
										 " = " + \
										template.format(task_id,
														python_callable.__name__,
														params)

	def __parse_tuple_task_family(self,
							parent, 
							family_set, 
							operator_dict,
							holistic = False):

		for family in family_set:
			self.__parse_string_task_family(parent,
									family,
									operator_dict,
									holistic)

	def __parse_string_task_family(self,
							parent,
							family, 
							operator_dict,
							holistic = False):

		params = {}
		count = 0
		inherits = False
		family_ops = []

		#Verify correct formatting if there are filetypes
		family = family.split("/")[-1].replace(".","_")

		#May need to update tasks with their new_family
		family_upstream_task = family 

		for op in operator_dict.keys():
			if operator_dict[op] != None:
				params = operator_dict[op]


			op_detail_list = self.prime_operator(parent, family,
												family_upstream_task, 
												op, params, inherits)

			family_ops += op_detail_list

			for op_details in op_detail_list:
				self.__add_python_operator(**op_details)

			#After first iteration, all tasks inherit from upstream
			#Need last item's task id to facilitate inheritance.
			inherits = True
			family_upstream_task = op_detail_list[-1]['task_id']

		family_id = "_".join([self.tag, family])
		if family_id in self.family_ids:
			raise ValueError("A Task Family with the same ID has already been created.\n\
							 Please check your inputs.")
		if not holistic:
			self.families.setdefault('core', {})[family_id] = family_ops
		else:
			self.families.setdefault(parent, {})[family_id] = family_ops

		if family in self.merge_ids:
			raise AttributeError("Family {} already represented in merge\
			registry. Please check your inputs.".format(family))

		self.merge_ids[family] = family_upstream_task
		self.family_ids.add(family_id)

	def write_ops(self):
		self.dag.operators += \
		'''
\n###########################################################
# Operators pertaining to {} dag layer with tag {}
###########################################################\n'''\
		.format(self.parent.upper(), self.tag.upper())

		for op_string in self.operators:
			self.dag.operators += self.operators[op_string]

	def holistic_layer_parsing(self):

		self.holistic = self.op_router[self.parent].get('holistic', None)
		if self.holistic is None:
			return

		for op in self.holistic:
			self.sublayers.append(op)
			self.__parse_string_task_family(op, op, self.holistic[op], holistic = True)

	def __write_op_family(self, family, members):
		op_family_string ='''\n{} = {}'''
		member_list = [member_dict['task_id'].replace(self.tag + "_", "") 
							for member_dict in members]

		member_string = " >> ".join(member_list).replace("'", "")
		self.dag.op_families += op_family_string.format(family, member_string)

	def write_op_families(self):
		self.dag.op_families += \
		'''
\n###########################################################
# Operator families pertaining to {} dag layer with tag {}
###########################################################\n'''\
		.format(self.parent.upper(), self.tag.upper())

		for sublayer, family in self.families.items():
			for family, members in family.items():
				self.__write_op_family(family, members)


	def __write_sublayer(self,sublayer):

		sublayer_families = self.families[sublayer].keys()

		sublayer_str = "\n{} = {}\n"

		self.dag.layers += "\n## Creating {} sublayer for {} dag layer"\
												.format(sublayer.upper(), 
														self.tag.upper())
		sublayer_str = sublayer_str.format(self.tag + "_" + sublayer, 
														pprint.pformat(list(sublayer_families))\
															.replace("'", "")\
															.replace("\n", "\n" + "\t"*6))
		

		print(sublayer_families)
		print(len(sublayer_families))													
		if (len(sublayer_families) == 1):
			sublayer_str = sublayer_str.replace("[","")\
										.replace("]", "")

		self.dag.layers += sublayer_str

	def write_sublayer_associations(self):
		connected_layer = "\n{} = {}\n"

		sublayer_families = ["_".join([self.tag, sublayer]) for sublayer in self.sublayers]
		self.dag.layers += connected_layer.format(self.tag, " >> ".join(sublayer_families)\
															.replace("'", ""))

	def write_sublayers(self):
		self.dag.layers += \
		'''
\n###########################################################
# Sublayer  pertaining to {} dag layer with tag {}
###########################################################\n'''\
		.format(self.parent.upper(), self.tag.upper())

		for sublayer in self.sublayers:
			self.__write_sublayer(sublayer)

		self.dag.layers += "\n## Connecting all sublayers for {} dag layer with tag {}"\
															.format(self.parent.upper(),
																	self.tag.upper())
		#Write sublayer associations
		self.write_sublayer_associations()
		

	def parse_layer(self):
		for family in self.config:
			if isinstance(family, str):
				#String parsing
				self.__parse_string_task_family(self.parent, 
										family, 
										self.config[family])

			if isinstance(family, tuple):
				self.__parse_tuple_task_family(self.parent, 
										family, 
										self.config[family])



		#General rules about holistic layer parsing
		self.holistic_layer_parsing()






