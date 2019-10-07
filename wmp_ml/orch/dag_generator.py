#####################################################################################
#
#
# 	DAG Generator Orchestration Airflow
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

# Import package specific information
# -- (MAYBE DO THIS IN THE INIT) -- 


#####################################################################################
# Class and Constructor
#####################################################################################

class DagGenerator():

	def __init__(self, config):
		self.config = config
		self.dag_config = config['config']
		self.output_dag = None
		self.author = self.config['dag']['owner']
		self.dag_name = self.config['dag_name']
		self.date = datetime.now().strftime("%m-%d-%Y--%H.%M.%S")
		self.import_dict = {}
		self.blocks = {}
		self.operators = {}
		self.structure = {}
		self.execution_hierarchy_config = os.path.abspath(os.path.join(__file__, '../../config/dag_hierarchy.cfg'))
		self.dag_args = {
						    'owner': 'airflow',
						    'depends_on_past': False,
						    'email': ['airflow@example.com'],
						    'email_on_failure': False,
						    'email_on_retry': False,
						    'retries': 1,
						    'op_args':{},
						    'op_kwargs': {},
						    'params': {}
						    # 'retry_delay': str(timedelta(minutes=5)),
						    # 'queue': 'bash_queue',
						    # 'pool': 'backfill',
						    # 'priority_weight': 10,
						    # 'end_date': datetime(2016, 1, 1),
						}
		#Update parameters that are not provided
		for key in self.config['dag']:
			self.dag_args[key] = self.config['dag'][key]

		self.dag_args = json.dumps(self.dag_args, indent = 4)

		with open(
				os.path.abspath(
					os.path.join(__file__, '../dag_template.txt')), 'r') as template:
			self.output_dag = template.read()


		#self.execution_hierarchy = json.loads(self.execution_hierarchy_config)
		self.imports = ""

	def partition_blocks(self):
		
		pass
		# self.blocks[key] = self.__determine_handler(self, self.config[key])
		# self.blocks[key].generate_skeleton()

	def generate_subdag(self, handler):
		pass

	def __recursive_operator_creation(self, block):
		if not isinstance(block, list):
			return self.__add_python_operator(block)

	def write_blocks(self):
		for block in self.blocks:

			self.operators += '''##############################################################################
# Operators tied to the {} Subdag
##############################################################################'''

	def __import_dynamically(self, obj):
	    import_statement = None
	    module = None
	    try:
	        import_statement = "from {} import {}\n".format(
	                                                      obj.__module__,
	                                                      obj.__name__)
	        module = obj.__module__

	    except Exception as e:

	            print("Dynamic import failed. Trying simpler import.")
	            module = "Abnormal Imports"
	            print(obj)
	            import_statement = 'import {}\n'.format(obj.__name__)

	    return import_statement, module

	def __rec_imports(self,
					  config_section, 
					  import_check):
	    
		try:
			for key in config_section.keys():
	            #print(key)
				if any([inspect.isclass(key),
				       inspect.isfunction(key),
				       inspect.ismodule(key)]) and key not in import_check:
					import_statement, module = self.__import_dynamically(key)
					self.import_dict.setdefault(module.split(".")[0], []).append(import_statement)
					import_check.add(key)
	            
				value = config_section[key]
				if any([inspect.isclass(value),
				       inspect.isfunction(value),
				       inspect.ismodule(value)]) and value not in import_check:
					import_statement, module = self.__import_dynamically(value)
					self.import_dict.setdefault(module.split(".")[0], []).append(import_statement)
					import_check.add(value)
	            
				self.__rec_imports(config_section[key], 
	            				   import_check)
	    
		#Will run into errors because checks everything
		#TODO: Make this system more intelligent
		except Exception as e:
			pass
	            

	def detect_external_imports(self):
		import_check = set()
		for key in self.dag_config:
			if isinstance(self.dag_config[key], dict):
				self.__rec_imports(self.dag_config[key], 
									import_check)


		self.imports += '''\n\n##############################################################################
# External Package Imports Automagically Detected
##############################################################################\n\n'''
	    
		for key in self.import_dict:
			self.imports += "\n## Import statements for {}\n".format(key.upper())
			self.imports += "".join([item for item in self.import_dict[key] if item != ""])



	def add_operator(self, op_type, detail_dict):
		pass


	def __add_python_operator(self, detail_dict):

		template = '''\nPythonOperator( 
							task_id={},
							provide_context=True,
							python_callable={},
							params = {},
							dag = dag
							)\n
					'''

		self.operators.append(template.format(detail_dict.task_id,
											  detail_dict.callable,
											  detail_dict.params))



	def generate_file(self):

		self.detect_external_imports()
		self.partition_blocks()

		self.output_dag = self.output_dag.format(self.dag_name,
												 self.author,
												 self.date,
												 self.imports,
												 self.dag_args,
												 "'" + self.dag_name + "'",
												 self.operators,
												 self.structure)

		self.dag_filename = "_".join([self.dag_name.replace(" ","-"), 
									  self.author.replace(" ","-"), 
									  self.date]) + ".py"
		with open(self.dag_filename, 'w') as file:
			file.write(self.output_dag)
			file.close()