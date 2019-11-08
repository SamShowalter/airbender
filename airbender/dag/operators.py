#####################################################################################
#
#
# 	Operators for DAG: Owned by OpFamilies
#  
#	Author: Sam Showalter
#	Date: October 3, 2018
#
#####################################################################################

#####################################################################################
# External Library and Module Imports
#####################################################################################

import pprint
from airbender.dag.utils import is_callable

#####################################################################################
# Class and Constructor
#####################################################################################

class DagOperator:

	def __init__(self, task_id, p_callable, params):

		self.task_id = task_id
		self.callable = p_callable
		self.params = params
		self.op_family = None

	def write(self):
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
		self.op_family.sublayer.layer.dag.import_dynamically(self.callable)


		#Add operator to operators list as a string.
		self.operator_str =  "\n{} = {}".format(self.task_id,
												template.format(self.task_id,
												self.callable.__name__,
												self.__parse_parameters(self.params)))

		#Add operator string to the 
		self.__write_operator_to_dag()


	def __write_operator_to_dag(self):

		self.op_family.sublayer.layer.dag.operators += self.operator_str

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
				if is_callable(k):
					obj_dict[str(k)] = k
				if is_callable(v):
					obj_dict[str(v)] = v

				if isinstance(v, dict):
					self.__parse_param_callables(v, obj_dict)