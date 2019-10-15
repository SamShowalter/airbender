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
	'''
	Layer object for generating the ML DAG. Contains sublayers, op_families, and operators

	'''

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
# Public, Orchestration Methods
#####################################################################################

	def parse_layer(self):
		'''
		Parse through all layers provided to Dag config and create
		building blocks for families, sublayers, and layers.

		'''

		#For each operator family in the config
		for family in self.config:

			#If family has a string key
			if isinstance(family, str):
				#String parsing
				self.__parse_string_task_family(self.parent, 
										family, 
										self.config[family])

			#If family has a tuple key
			if isinstance(family, tuple):
				self.__parse_tuple_task_family(self.parent, 
										family, 
										self.config[family])

		#General parsing rules for specified layer
		self.holistic_layer_parsing()


	def holistic_layer_parsing(self):
		'''
		Holistic layer parsing involves operations that
		are not specified by the user but must be created in order
		to create a successful DAG. A prime example is a merge operation
		after several feature engineering operations are completed.

		Sub_Functions:
			__parse_string_task_family:			Parses config tasks into operators

		'''

		#Determine if holistic parsing needs to be run for a layer
		self.holistic = self.op_router[self.parent].get('holistic', None)
		if self.holistic is None:
			return

		#For each operation in the holistic dictionary found in the op_router
		for op in self.holistic:

			#Add a new sublayer
			self.sublayers.append(op)

			#Parse the string task family provided by the holistic operation
			self.__parse_string_task_family(op, op, self.holistic[op], holistic = True)

#####################################################################################
# Public Methods for Writing Dag Layer
#####################################################################################

	def write_operators(self):
		'''
		Write all operators owned by later to the parent DAG
		'''

		#Add section tag for operators in provided layer
		self.dag.operators += \
		'''
\n###########################################################
# Operators pertaining to {} dag layer with tag {}
###########################################################\n'''\
		.format(self.parent.upper(), self.tag.upper())

		#For each operator string in the list of operators
		#Add the operator to the parent dag
		for op_string in self.operators:
			self.dag.operators += self.operators[op_string]


	def write_op_families(self):
		'''
		Write all operator families belonging to layer to parent DAG.

		Sub_Function:
			__write_op_family:			Write operator family for one family

		'''

		#Partition python file to describe operator families
		self.dag.op_families += \
		'''
\n###########################################################
# Operator families pertaining to {} dag layer with tag {}
###########################################################\n'''\
		.format(self.parent.upper(), self.tag.upper())

		#Write families for each sublayer
		for sublayer, family in self.op_families.items():
			for family, members in family.items():
				self.__write_op_family(family, members)


	def write_sublayers(self):
		'''
		Write all sublayers to owning DAG. A sublayer is defined by 
		whether or not is can be run in parallel with other tasks in
		a layer, or involves the output from the previous sublayer. As
		of now, the only time you have more than one sublayer is if
		you are merging columns back into the dataset

		Sub_Function:
			__write_sublayer:				Writes sublayer for specific sublayer
			write_sublayer_associations:	Connect sublayers into a single layer

		'''

		#Partition for sublayer for organization in final DAG .py file
		self.dag.layers += \
		'''
\n###########################################################
# Sublayer  pertaining to {} dag layer with tag {}
###########################################################\n'''\
		.format(self.parent.upper(), self.tag.upper())

		#Iterate through all sublayers, in order
		for sublayer in self.sublayers:
			self.__write_sublayer(sublayer)

		#Partition final DAG file before establishing connections
		self.dag.layers += "\n## Connecting all sublayers for {} dag layer with tag {}"\
															.format(self.parent.upper(),
																	self.tag.upper())
		#Write sublayer associations
		self.write_sublayer_associations()


	def write_sublayer_associations(self):
		'''
		Write assocations for sublayers and give to full layer variable.
		This is the final step before connecting all DAG layers.

		'''

		#Template for generating connected layer
		connected_layer = "\n{} = {}\n"

		#Sublayer names generated from sublayer list
		#TODO: Make it so this new naming is not needed
		sublayer_names = ["_".join([self.tag, sublayer]) for sublayer in self.sublayers]

		#Add layers to the final dag.
		#TODO: This will need to be changed to fix Airflow dependency issues
		self.dag.layers += connected_layer.format(self.tag, " >> ".join(sublayer_names)\
															.replace("'", ""))
#####################################################################################
# Public Validation Methods
#####################################################################################

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

	def delineate(self, 
				 lineage, 
				 order,
				 subrank,
				 dag):
		'''
		De-lineates the entire DagLayer by attributing
		a parent, lineage, order of execution, and subrank.

		This is useful for defining the dag later

		Args:
			lineage:			Lineage of concepts that lead to DAG in configuration
			order:				Running order of DagLayer, provided by configuration
			subrank:			Subrank of DagLayers owned by the same concept
			dag:				Owning DagGenerator object

		'''

		#Full lineage and layer parent concept
		self.lineage = lineage
		self.parent = lineage[0]

		#Generates tag for layer
		self.generate_tag(subrank)

		#Define execution order
		self.exec_order = order
		if subrank is not None:
			self.exec_order += (subrank/10.0)

		#Attribute the parent DAG
		self.dag = dag 

	def generate_tag(self, subrank = None):
		'''
		Generates the tag for the entire 
		DagLayer. This is an name derived 
		from the lineage of the DagLayer
		
		Kwargs:
			rank:				Subrank of dag layer owned by a specific concept

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
		if subrank is not None:
			self.tag += "_l" + str(subrank)

#####################################################################################
# Private, Supplementary Methods for Assisting Orchestration
#####################################################################################

	def __parse_string_task_family(self,
							parent,
							family, 
							operator_dict,
							holistic = False):
		'''
		Parses input from the Layer configuration into a family of
		operations that sequentially act on a target piece of data.

		This is the most atomic function for converting configuration-
		based functionality into DAG operators.

		Args:	
			parent:						Parent concept. Often layer parent, unless holistic parsing
			familt_set:					Owning family for task. In this case, a tuple of col names
			operator_dict:				Dictionary of callables and their parameters

		Kwargs:
			holistic:					Boolean determining if this is a holistic operation

		Sub_Functions:
			__generate_python_operator:	Generates python operator string and stores it
			__create_family_id:			Create unique ID for specific task family

		'''

		#Initialize parameter, inheritance, family operation, and count vars
		params = {}
		count = 0
		inherits = False
		family_ops = []

		#Verify correct formatting if there are filetypes
		family = family.split("/")[-1].replace(".","_")

		#May need to update tasks with their new_family
		family_upstream_task = family 

		#For operation in operator dictionary (all within one family)
		for op in operator_dict.keys():

			#Overwrite params var if input is not None
			if operator_dict[op] != None:
				params = operator_dict[op]

			#Operation detail list as generated by priming function
			op_detail_list = self.__prime_operator(parent, 
													family,
													family_upstream_task, 
													op, 
													params, 
													inherits)

			#Add operation detail list of family operators 
			#Could have multiple shell operators for one operation
			#Therefore, we combine with list addition
			family_ops += op_detail_list

			#Generate python operators for each operator in family set
			for op_details in op_detail_list:
				self.__generate_python_operator(**op_details)

			#After first iteration, all tasks inherit from upstream
			#Need last item's task id to facilitate inheritance.
			inherits = True
			family_upstream_task = op_detail_list[-1]['task_id']

		#Create a family ID
		family_id = self.__create_family_id(family)

		#Determine which sublayer this family applies to
		#Then add the task family to the layer
		if not holistic:
			self.op_families.setdefault('core', {})[family_id] = family_ops
		else:
			self.op_families.setdefault(parent, {})[family_id] = family_ops

		#Add family to list of merge_ids in case the operations need to be merged
		#TODO: Separate out and handle in different way.
		if family in self.merge_ids:
			raise AttributeError("Family {} already represented in merge\
			registry. Please check your inputs.".format(family))

		self.merge_ids[family] = family_upstream_task

	def __parse_tuple_task_family(self,
							parent, 
							family_set, 
							operator_dict,
							holistic = False):
		"""
		Function that parses tuple tasks. It iterate through the tuple key
		(family_set) and parses each as its own string task.

		Args:	
			parent:						Parent concept. Often layer parent, unless holistic parsing
			familt_set:					Owning family for task. In this case, a tuple of col names
			operator_dict:				Dictionary of callables and their parameters

		Kwargs:
			holistic:					Boolean determining if this is a holistic operation
		
		Sub_Functions:
			__parse_string_task_family	Parses string task family into operators

		"""

		#Iterate through tuple
		for family in family_set:

			#Parse string task for each member of family
			self.__parse_string_task_family(parent,
									family,
									operator_dict,
									holistic)

	def __prime_operator(self, 
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

		Sub_Functions:
			__create_task_id:				Generate and validate task id for an operator
			__parse_parameters:				Parse operator parameters

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

		#Must be converted to a list to be iterated on later
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


	def __parse_parameters(self, params):
		'''
		Parent function for parsing parameters. Recursively dives into dictionaries and sub_dictionaries
		in the configuration. It then detects callables, documents them, and replaces them with their
		correct format so they are deemed functions when written as a final DAG.

		Args:
			params:								Full parameter set for a specific operator

		Child_Functions:
			__parse_param_callables:			Updates the provided slice of configuration with correct format

		Returns:
			params_str:							String version of all parameters

		'''

		#Initial creation of the final parameter string.
		#Includes formatting changes
		params_str = pprint.pformat(params).replace("\n", "\n" + "\t"*7)

		#Object dictionary
		obj_dict = {}
		self.__parse_param_callables(params, obj_dict)

		#For key in the object dictionary
		#Replace the function with the correct name
		for key in obj_dict:
		    params_str = params_str.replace(key, obj_dict[key].__name__)

		#Return final parameter string
		return params_str


	def __parse_param_callables(self, params, obj_dict):
		'''
		Recursive function to parse parameters for callable
		function so that they are writte correctly in the
		final Python file. It documents any function objects 
		and their string format. It then replaces their format
		after the entire configuration is converted to string format.
		This ensures that the functions are represented as functions in
		the final file.

		Args:
			params:						Sub-dict of parameters, including numbers, strings, and callables
			obj_dict:					Reference object dictionary with string and correct object formats

		'''
		if isinstance(params, dict):

			for k,v in params.items():
				if self.dag.is_callable(k):
					obj_dict[str(k)] = k
				elif self.dag.is_callable(v):
					obj_dict[str(v)] = v

				if isinstance(v, dict):
					self.__parse_param_callables(v, obj_dict)


	def __generate_python_operator(self, 
								task_id,
								python_callable, 
								params):
		'''
		Write a python operator to the owning DAG. This is a direct connection to
		the final Python file that will be submitted to airflow.

		Args:
			task_id:						Task identifier for operator
			python_callable:				Shell callable for Python function
			params:							Params for shell callable, including the true callable

		Sub_Functions:
			import_dynamically:				Dynamically imports functionality needed for script

		'''

		#Template for python operator
		template = '''PythonOperator( 
							task_id='{}',
							provide_context=True,
							python_callable={},
							params = {},
							dag = dag)\n
					'''

		#Import callables as strings into final Python file
		self.__import_layer_operation(python_callable)

		#Replaces the variable name to be shorter, but there should be no overlap
		#There would likely never be a case of a name thats the same, with different lineages
		shortened_id = task_id.replace(self.tag + "_", "")

		#Add operator to operators list as a string.
		self.operators[shortened_id] =  "\n" + shortened_id +\
										 " = " + \
										template.format(task_id,
														python_callable.__name__,
														params)

#####################################################################################
# Private Methods for Writing Dag Layer
#####################################################################################
	
	def __import_layer_operation(self, operation):
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

	def __write_op_family(self, family_id, members):
		'''
		Write out associations for an entire operator family.

		Args:
			family:						String representing family_id
			members:					Dictionary of family members

		'''

		#Templated string
		op_family_string ='''\n{} = {}'''

		#Isolate the names of the members (variable name where family will be stored)
		member_list = [member_dict['task_id'].replace(self.tag + "_", "") 
							for member_dict in members]

		#Write out all family members and their sequential association
		member_string = " >> ".join(member_list).replace("'", "")

		#Add family to list of operator families
		self.dag.op_families += op_family_string.format(family_id, member_string)


	

	def __write_sublayer(self,sublayer):
		'''
		Function that writes sublayer and its associations
		for a provided sublayer. 

		Args:
			sublayer:				Sublayer found in DAG

		'''

		#Get families that belong to specific sublayer
		sublayer_families = self.op_families[sublayer].keys()

		#Sublayer string template
		sublayer_str = "\n{} = {}\n"

		#Adding partition for generating sublayers for dag
		self.dag.layers += "\n## Creating {} sublayer for {} dag layer"\
												.format(sublayer.upper(), 
														self.tag.upper())

		#Generate the sublayer string with pretty formatting
		sublayer_str = sublayer_str.format(self.tag + "_" + sublayer, 
														pprint.pformat(list(sublayer_families))\
															.replace("'", "")\
															.replace("\n", "\n" + "\t"*6))
		
		#To prevent list collision issues in Airflow
		#Any sublayer that does not have more than one operator
		#Will not be represented as a list								
		if (len(sublayer_families) == 1):
			sublayer_str = sublayer_str.replace("[","")\
										.replace("]", "")

		#Add sublayer string to parent dag
		self.dag.layers += sublayer_str

	

#####################################################################################
# Private Validation Methods
#####################################################################################

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

	def __create_family_id(self, family):
		'''
		Create a family id and then check to ensure that it is not a duplicate
		from somewhere else.

		Args:
			family:				String name for specific operation family

		Raises:
			ValueError:			If task family ID is already taken

		Returns:
			family_id:			Final, created family_id

		'''

		#Generate family id
		family_id = "_".join([self.tag, family])

		#Check to ensure that the specific family_id is not taken
		if family_id in self.family_ids:
			raise ValueError("A Task Family with the same ID has already been created.\n\
							 Please check your inputs.")

		#Add family_id to list
		self.family_ids.add(family_id)

		#Return family id
		return family_id

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

		#Set model to false until it has been evaluated
		if self.dag.is_callable(model):
			self.dag.models[family] = False 

		#Return model object, unchanged
		return model

	