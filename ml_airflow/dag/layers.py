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
import copy

#Time 
from datetime import datetime, timedelta

#String conversion for dictionaries
import json
import inspect

# Import package specific information
sys.path.append("../../")
from ml_airflow.airflow.op_converter import *

#Import SubLayer object
from ml_airflow.dag.sublayers import DagSubLayer
from ml_airflow.dag.op_families import OpFamily 
from ml_airflow.dag.operators import DagOperator

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
		self.sublayers = {}
		self.sublayer_order = []
		
		#Collection of operator families
		self.family_ids = set()

		#List of IDs needed to merge operators
		self.merge_ids = {}

		#Head and tail of layer
		self.head = None
		self.tail = None

		# Merge head
		self.merge_head = 'core'

		#Holistic order of the sublayers
		self.holistic_order = -1

		#Operator router (defined in function below)
		#TODO: Find a better place for it
		self.op_router = None

		#Conditional mapping for dynamic splitting
		self.conditional_mapping = None

#####################################################################################
# Public, Orchestration Methods
#####################################################################################

	def parse_layer(self):
		"""
		Public function to parse layer based on conditional mappings

		Sub_Functions:
			__parse_layer:					Private function that parses all op_families 
			holistic_layer_parsing:			Parsing processes necessitated inherently by layer
			generate_sublayer_order:		Determine order of sublayers

		"""

		#Parse layer
		self.__parse_layer_split()

		#Generate sublayer order
		self.generate_sublayer_order()

	

	def holistic_layer_parsing(self, order = 'post', conditional_mapping = None, split = None):
		'''
		Holistic layer parsing involves operations that
		are not specified by the user but must be created in order
		to create a successful DAG. A prime example is a merge operation
		after several feature engineering operations are completed.

		Sub_Functions:
			__parse_string_task_family:			Parses config tasks into operators

		'''

		#Determine if holistic parsing needs to be run for a layer
		if self.op_router is None:
			self.__prime_operator('init', 
									'init', 
									'init',
									'init', 
									'init')
		holistic = self.op_router[self.parent].get('holistic', None)
		if holistic is None or holistic.get(order, None) is None:
			return

		holistic = holistic[order]

		#For each operation in the holistic dictionary found in the op_router
		for op in holistic:

			#Increment holistic order
			self.holistic_order += 1


			if split:

					if self.conditional_mapping:
					#Parse the string task family provided by the holistic operation

						for cond_mapping in self.sublayers["_".join(['core',split])].keys():
							self.__parse_string_task_family(op, 
															op, 
															holistic[op], 
															holistic = True,
															conditional_mapping = cond_mapping,
															split = split)

					else:
						#Parse the string task family provided by the holistic operation
						self.__parse_string_task_family(op, 
														op, 
														holistic[op], 
														holistic = True,
														conditional_mapping = conditional_mapping,
														split = split)
			else:
				if self.conditional_mapping:
					#Parse the string task family provided by the holistic operation

						for cond_mapping in self.sublayers['core'].keys():
							self.__parse_string_task_family(op, 
															op, 
															holistic[op], 
															holistic = True,
															conditional_mapping = cond_mapping,
															split = split)

				else:
					#Parse the string task family provided by the holistic operation
					self.__parse_string_task_family(op, 
													op, 
													holistic[op], 
													holistic = True,
													conditional_mapping = conditional_mapping,
													split = split)


#####################################################################################
# Public Methods for Writing Dag Layer
#####################################################################################

	def write_operators(self):
		'''
		Write all operators owned by later to the parent DAG
		These are all stored as strings
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
		#Write families for each sublayer
		for sublayer_name, sublayer in self.sublayers.items():
			if isinstance(sublayer, dict):
				for cond_sublayer_name, cond_sublayer in sublayer.items():
					cond_sublayer.write_operators()
			else:
				sublayer.write_operators()


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
		for sublayer_name, sublayer in self.sublayers.items():
			if isinstance(sublayer, dict):
				for cond_sublayer_name, cond_sublayer in sublayer.items():
					self.dag.op_families += \
					"\n\n## Families pertaining to {} conditional sublayer\n".format(cond_sublayer_name)
					cond_sublayer.write_op_families()
			else:
				sublayer.write_op_families()

			


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
# Sublayer pertaining to {} dag layer with tag {}
###########################################################\n'''\
		.format(self.parent.upper(), self.tag.upper())

		#Iterate through all sublayers, in order
		for sublayer_name, sublayer in self.sublayers.items():
			if isinstance(sublayer, dict):
				for cond_sublayer_name, cond_sublayer in sublayer.items():
					cond_sublayer.write(head = True)
					cond_sublayer.write(head = False)
			else:
				sublayer.write(head = True)
				sublayer.write(head = False)

		if len(self.sublayer_order) > 1:
			#Partition final DAG file before establishing connections
			#TODO: Make more intelligent
			self.dag.layers += "\n## Connecting all sublayers (if > 1) for {} dag layer with tag {}"\
																.format(self.parent.upper(),
																		self.tag.upper())
		#Write sublayer associations
		self.write_sublayer_associations()


	def generate_sublayer_order(self):
		#Generate order for sublayers
		self.sublayer_order = [None]*len(self.sublayers)
		for sublayer_name, sublayer in self.sublayers.items():
			#print(sublayer.name)
			sub_order = None
			if isinstance(sublayer,dict):
				sub_order = sublayer[list(sublayer.keys())[0]].order
			else:
				sub_order = sublayer.order

			self.sublayer_order[sub_order] = sublayer



	def generate_layer_head_tail(self):

		if self.conditional_mapping:
			self.tail = {}
			self.head = {}
			for cond_sublayer_name, cond_sublayer in self.sublayers['core'].items():
				self.head[cond_sublayer_name] = cond_sublayer.refs['head']
				self.tail[cond_sublayer_name] = cond_sublayer.refs['tail']


		else:
			self.head = self.sublayer_order[-1].refs['head']
			self.tail = self.sublayer_order[0].refs['tail']


	def write_sublayer_associations(self):
		'''
		Write assocations for sublayers and give to full layer variable.
		This is the final step before connecting all DAG layers.

		'''

		#Generate sublayer order
		self.generate_layer_head_tail()

		if self.conditional_mapping:

			for conditional_mapping in self.head:
				self.dag.layers += "\n\n## Printing conditional associations for {}".format(conditional_mapping)
				sublayer = copy.deepcopy(self.sublayer_order)
				for i in range(len(sublayer)):
					sublayer[i] = sublayer[i][conditional_mapping]
				self.__write_sublayer_associations(sublayer)
		else:
			self.__write_sublayer_associations(self.sublayer_order)


	
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
				 conditional_mapping,
				 split,
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
		self.split = split
		self.conditional_mapping = conditional_mapping

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
				self.tag += "".join([token[0] 
								for token in 
								self.lineage[i].split("_")])
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

	def __parse_layer_split(self):


		if self.split:

			for split_section in self.split:

				

				#General parsing rules for specified layer
				self.holistic_layer_parsing(order = "pre", split = split_section)
				
				#Parse the conditionality of the layer
				self.holistic_order += 1
				self.__parse_layer_conditionality(split = split_section)

				#General parsing rules for specified layer
				self.holistic_layer_parsing(order = "post", split = split_section)

		else:
			
			#General parsing rules for specified layer
			self.holistic_layer_parsing(order = "pre")

			#Parse layer conditionality
			self.holistic_order += 1
			self.__parse_layer_conditionality()

			#General parsing rules for specified layer
			self.holistic_layer_parsing(order = "post")

	def __parse_layer_conditionality(self, split = None):

		#Look for conditional mapping
		if self.conditional_mapping:

			#Set conditional mappings as the head of the previous layer
			self.conditional_mappings = self.dag.layerbag[self.exec_order - 1].sublayer_order[-1].head

			#Iterate through mappings and parse layer
			for mapping in self.conditional_mappings:
				self.__parse_layer_core(conditional_mapping = mapping, split = split)

		else:
			self.__parse_layer_core(split = split)



	def __parse_layer_core(self, conditional_mapping = None, split = None):
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
										self.config[family],
										conditional_mapping = conditional_mapping,
										split = split)

			#If family has a tuple key
			if isinstance(family, tuple):

				#Tuple parsing
				self.__parse_tuple_task_family(self.parent, 
										family, 
										self.config[family],
										conditional_mapping = conditional_mapping,
										split = split)


	def __parse_string_task_family(self,
							parent,
							family, 
							operator_dict,
							holistic = False,
							conditional_mapping = None,
							split = None):
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
			__create_family_id:			Create unique ID for specific task family

		'''

		#Initialize parameter, inheritance, family operation, and count vars
		count = 0
		inherits = False
		family_ops = []

		#May need to update tasks with their new_family
		family_upstream_task = family 

		#For operation in operator dictionary (all within one family)
		for op in operator_dict.keys():

			#Initialize params
			params = {}

			#Overwrite params var if input is not None
			if operator_dict[op] != None:
				params = operator_dict[op]

			#Operation detail list as generated by priming function
			op_detail_list = self.__prime_operator(parent, 
													family,
													family_upstream_task, 
													op, 
													params, 
													inherits,
													conditional_mapping,
													split)


			#Add operation detail list of family operators 
			#Could have multiple shell operators for one operation
			#Therefore, we combine with list addition
			family_ops += op_detail_list

			#After first iteration, all tasks inherit from upstream
			#Need last item's task id to facilitate inheritance.
			inherits = True
			family_upstream_task = op_detail_list[-1].task_id

		self.__store_op_family(parent, family, family_ops, holistic, conditional_mapping, split)


	def __store_op_family(self, parent, family, family_ops, holistic, conditional_mapping, split):

		#Create a family ID
		#Verify correct formatting if there are filetypes
		family_id = self.__create_family_id(family, split, conditional_mapping)

		#Determine which sublayer this family applies to
		#Then add the task family to the layer
		if not holistic and conditional_mapping == None:
			if split:
				self.sublayers.setdefault('core_' + split, 
										DagSubLayer('core_' + split, self.holistic_order, self))\
										.add_op_family(family_id, family_ops)

			else:
				self.sublayers.setdefault('core', 
										DagSubLayer('core', self.holistic_order, self))\
										.add_op_family(family_id, family_ops)

		elif not holistic and conditional_mapping != None:

			if split:
				dsl = self.sublayers.setdefault('core', {})\
									.setdefault(split, {})\
									.setdefault(conditional_mapping,
									DagSubLayer("_".join([split,conditional_mapping]), 
										self.holistic_order, self))

				self.sublayers['core'][split][conditional_mapping].add_op_family(family_id, family_ops)
				
			else:
				dsl = self.sublayers.setdefault('core', {})\
									.setdefault(conditional_mapping,
									DagSubLayer(conditional_mapping, 
										self.holistic_order, self))

				self.sublayers['core'][conditional_mapping].add_op_family(family_id, family_ops)
				
									
		elif conditional_mapping != None:

			if split:
				dsl = self.sublayers.setdefault(parent + "_" + split, {})\
									.setdefault(split, {})\
									.setdefault(conditional_mapping,
									DagSubLayer("_".join([parent, split, conditional_mapping]), 
										self.holistic_order, self))

				self.sublayers[parent][split][conditional_mapping].add_op_family(family_id, family_ops)
				

			else:
				dsl = self.sublayers.setdefault(parent, {})\
									.setdefault(conditional_mapping,
									DagSubLayer(parent + "_" + conditional_mapping, 
										self.holistic_order, self))

				self.sublayers[parent][conditional_mapping].add_op_family(family_id, family_ops)

			self.merge_head = parent


		else:
			if split:
				self.sublayers.setdefault(parent + split, 
										DagSubLayer(parent + split, self.holistic_order, self))\
										.add_op_family(family_id, family_ops)
				self.merge_head = parent

			else:
				self.sublayers.setdefault(parent, 
										DagSubLayer(parent, self.holistic_order, self))\
										.add_op_family(family_id, family_ops)
				self.merge_head = parent


	def __parse_tuple_task_family(self,
							parent, 
							family_set, 
							operator_dict,
							holistic = False,
							conditional_mapping = None,
							split = None):
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
									holistic,
									conditional_mapping,
									split)


	def __prime_operator(self, 
						parent, 
						family, 
						family_upstream_task,
						op, 
						params,
						inherits = False,
						conditional_mapping = None,
						split = None):
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
							 'task_tag': [family, op_name]},
             'data_sources': 
             				{'operator':read_data_operation, 
             				'args': {'func': op, 
             						'params': params,
             						'filepath': family},
             				'task_tag': [family, 
             							op_name]},
             'preprocessing': 
             				{'operator':bulk_data_operation, 
             				'args': {'func': op,
             						'split': split,
             						'params': params},
             				'task_tag':[family, split, op_name]},
             'evaluation': 
             				{'operator':evaluation_operation, 
             				'args': {'func': op,
             						 'params': params,
             						 #Figure out model id generation for eval tasks
             						 'model_id': conditional_mapping},
             				'holistic': {
             							"post":
             								{'merge_metrics': #Parent
             								{'merge_metrics': {}}}},
             				'task_tag': [conditional_mapping, family]},
             # Will wait to do any EDA design patterns
             # 'eda': 
             # 				{'operator':bulk_data_operation, 
             # 				'args': {'func'},
             # 				'task_tag':[]},

             'modeling': 
             				{'operator':[('fit',fit_operation), 
             							 ('predict',predict_operation)], 

             				#Registers model for evaluation functions later
             				'args': {'model': op, 
             						'params': params},
             				'holistic': {'pre':
             								{'model_data_split':
             									{'model_data_split': {}}}},
             				'arg_xcom_update': ['model'],
             				'task_tag':[family]},

             'feature_engineering': 
             				#Airflows op_converter needs to be determined
             				{'operator': col_data_operation, 
             				'args': {'func': op,
             						 'params': params,
             						 'split': split,
             						 'inherits': inherits,
             						 'column_data_id': family_upstream_task},
             				'holistic': {"post":
             								{'merge_layer': #Parent
             								{'merge_cols': {}}}},
             				'task_tag': [family, split, op_name]},

             #HOLISTIC LAYER OPERATIONS START HERE
             'merge_layer': 
             				{'operator': merge_data_operation, 
             				'args': {'params': params,
             				'merge_ids': self.__get_merge_ids(parent, conditional_mapping, split),
             				'split': split},
             				'task_tag': [self.tag, split, 'merge_layer']},

             'merge_metrics': 
             				{'operator': merge_metrics_operation, 
             				'args': {'params': params,
             				'merge_ids': self.__get_merge_ids(parent, conditional_mapping, split),
             				'model': conditional_mapping},
             				'task_tag': [self.tag, split, conditional_mapping, 'merge_metrics']},

             'model_data_split': 
             				{'operator': model_split_operation, 
             				'args': {'params': params},
             				'task_tag': ['model_data_split']}
		}

		#Returns if it is just init
		if parent == 'init':
			return

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


			new_op = DagOperator(task_id,
							   final_operator,
							   copy.deepcopy(params))

			#Add new operator to detail list
			op_detail_list.append(new_op)


			#Update the upstream task_id (for later inheritance)
			upstream_task_id = task_id
		
		#Return op_detail_list
		return op_detail_list

	def __write_sublayer_associations(self, sublayer_order):
		"""
		Private method that, given a sublayer order, parses through
		the layer to determine sublayer associations

		Args:
			sublayer_order:						List of sublayers, in order

		"""

		#Template for generating connected layer
		connected_layer = "\n{}"


		#Add layers to the final dag.
		#TODO: This will need to be changed to fix Airflow dependency issues
		for sublayer_index in range(len(sublayer_order) - 1):
			self.dag.layers += connected_layer\
									.format(" >> "\
										.join([sublayer_order[sublayer_index]\
															.refs['head'],
											   sublayer_order[sublayer_index + 1]\
											   				.refs['tail']])\
																.replace("'", ""))

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

		#Trim out tag info if there is null data
		tag_info = [tag for tag in tag_info if tag is not None]

		#Join tag data
		task_id = "_".join(tag_info)

		#Clean task_id
		task_id = task_id.split("/")[-1].replace(".", "_")
		
		#Raise an error if this task is already 
		#defined in the dag
		if task_id in self.dag.tasks:
			raise AttributeError("Task with the same name (task_id = {})\
							 has already been created. Check your inputs"\
								.format(task_id))
		
		#Add the task ID to the dag
		self.dag.tasks.add(task_id)

		#Return the task_id
		return task_id


	def __create_family_id(self, family, split = None, conditional_mapping = None):
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

		#Clean family ID
		family = family.split("/")[-1].replace(".", "_")

		#Generate family id
		family_id = "_".join([self.tag, family])

		#Add conditional mapping information, if necessary
		if conditional_mapping is not None:
			family_id = conditional_mapping + "_" + family_id

		#Add split information, if necessary
		if split not in [None, ""]:
			family_id = split + "_" + family_id

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

	def __get_merge_ids(self, parent, conditional_mapping = None, split = None):
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

		split_str = ""
		if split is not None:
			split_str = "_" + split

		if parent in ['merge_layer', 'merge_metrics']:
			if conditional_mapping is not None and split is not None:
				return self.sublayers['core' + split_str][conditional_mapping].head

			elif conditional_mapping:
				return self.sublayers['core' + split_str][conditional_mapping].head

			elif split is not None:
				return self.sublayers['core' + split_str].head

			else:
				return self.sublayers[self.merge_head].head




	