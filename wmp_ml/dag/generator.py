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
import pprint

#Time 
from datetime import datetime, timedelta

#String conversion for dictionaries
import json
import inspect

#Preserve order of inputs python
from collections import OrderedDict

# DAG information package specific information
sys.path.append("../../")
from wmp_ml.dag.layers import DagLayer


#####################################################################################
# Class and Constructor
#####################################################################################

class DagGenerator():
	'''
	Dag generator orchestration object. This object will take a specific configuration
	and generate an airflow ML experiment for you.
	'''
	def __init__(self, config):
		self.config = config
		self.dag_config = config['config']
		self.output_dag = None
		self.author = self.config['dag']['owner']
		self.dag_name = self.config['dag_name']
		self.date = datetime.now().strftime("%m-%d-%Y--%H.%M.%S")
		self.import_dict = {}
		self.layerbag = []
		self.layertags = []
		self.import_check = set()
		self.tasks = set()
		self.models = {}
		self.op_families = ''''''
		self.operators = ''''''
		self.layers = ''''''
		self.structure = ''''''
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

		self.dag_args = json.dumps(self.dag_args, indent = 4)\
							.replace('false', 'False')\
							.replace('true','True')

		with open(
				os.path.abspath(
					os.path.join(__file__, '../../config/dag_template.txt')), 'r') as template:
			self.output_dag = template.read()

		self.execution_hierarchy = None
		with open(self.execution_hierarchy_config) as exec_config:
			self.execution_hierarchy = json.load(exec_config)
		self.imports = ""

	def is_callable(self, obj):
		return any([inspect.isfunction(obj),
				   inspect.ismethod(obj), 
				   inspect.ismodule(obj),
				   inspect.isclass(obj)])


	def __rec_dag_config(self, config_section):

		if isinstance(config_section, dict):
			for key in config_section:
				if isinstance(config_section[key], dict):
					config_section[key] = OrderedDict(config_section[key])
					self.__rec_dag_config(config_section[key])

	def parse_dag_config(self):

		self.__rec_dag_config(self.dag_config)


	def __block_lineage(self, subsection, lineage):

		if isinstance(subsection, DagLayer):
			block_order = self.execution_hierarchy[lineage[0]]
			subsection.delineate(lineage, 
								order = block_order, subrank = None,
								dag = self)

			self.layerbag[block_order] = self.layerbag[block_order] + [subsection]

		elif ((isinstance(subsection, set) or
		     isinstance(subsection,list)) and 
			len(subsection) == 1):
			subsection = subsection[0]
			block_order = self.execution_hierarchy[lineage[0]]
			subsection.delineate(lineage, 
								order = block_order, subrank = None,
								dag = self)

			self.layerbag[block_order] = self.layerbag[block_order] + [subsection]

		elif (isinstance(subsection, set) or
			 isinstance(subsection, list)):
			order = 0
			sublayer = []
			for item in subsection:
				if isinstance(item, DagLayer):
					block_order = self.execution_hierarchy[lineage[0]]
					item.delineate(lineage, 
									order = block_order, subrank = order, 
									dag = self)
					sublayer.append(item)
					order += 1
			self.layerbag[block_order] = self.layerbag[block_order] + [sublayer]
				
		elif (isinstance(subsection,dict)):
				for key in subsection:
					self.__block_lineage(subsection[key], lineage + [key])

	def __flatten_layers(self, l, sublist):

		for item in l:
			if isinstance(item, list):
				self.__flatten_layers(item, sublist)
			else:
				if item in self.layertags:
					raise ValueError('''Two DagLayers have the same unique tag. 
									Please check your inputs.''')
				
				self.layertags.append(item.tag)
				sublist.append(item)

		return sublist
    
	def flatten_layers(self):
	    self.layerbag = self.__flatten_layers(self.layerbag, [])

	def determine_block_lineage(self):
		self.layerbag = [[]]*len(self.execution_hierarchy.keys())
		self.__block_lineage(self.dag_config, [])
		self.layerbag = [block for block in self.layerbag if block != []]


	def parse_layers(self):
		for layer in self.layerbag:
			layer.parse_layer()


	def write_layers(self):

		for layer in self.layerbag:

			#Write everything related to layer
			layer.write_operators()
			layer.write_op_families()
			layer.write_sublayers()

		layertag_string = pprint.pformat(self.layertags)\
							.replace("[", "")\
							.replace("]", "")\
							.replace("'", "")\
							.replace("\n", "\n" + "\t"*2)

		self.structure += "chain({})".format(layertag_string)

	def import_dynamically(self, obj):
		import_statement = None
		module = None
		try:
			if obj not in self.import_check:
				import_statement = "from {} import {}\n".format(
				                                              obj.__module__,
				                                              obj.__name__)
				module = obj.__module__
				self.import_check.add(obj)
				self.import_dict.setdefault(module, []).append(import_statement)

		except Exception as e:

			print("Dynamic import failed. Trying simpler import.")
			module = "Abnormal Imports"
			import_statement = 'import {}\n'.format(obj.__name__)

	def __rec_imports(self,
					  config_section):
	    
		try:
			if isinstance(config_section, DagLayer):
				config_section = config_section.config

			for key in config_section:

				if (isinstance(key, set)
					  or isinstance(key, list)):
					for item in key:
						self.__rec_imports(item)

				elif isinstance(key, DagLayer):
					self.__rec_imports(key)

				elif (self.is_callable(key) 
					and not isinstance(key, DagLayer)):

					self.import_dynamically(key)
				
				if (not isinstance(config_section, DagLayer) and
					isinstance(config_section, dict)):

					if (not isinstance(config_section[key], DagLayer) and
						self.is_callable(config_section[key])):

						self.import_dynamically(config_section[key])


					#Check if value is a new dictionary, set, or daglayer
					#This will fail a ton. That is ok. It needs to check everything
					else:
						self.__rec_imports(config_section[key])
	    
		#Will run into errors because checks everything
		#TODO: Make this system more intelligent
		except Exception as e:
			#print(str(e))
			pass
	            

	def detect_external_imports(self):
		import_check = set()
		for key in self.dag_config:

			if isinstance(self.dag_config[key], dict):
				self.__rec_imports(self.dag_config[key])

			if isinstance(self.dag_config[key], list):
				for item in self.dag_config[key]:
					self.__rec_imports(item)

			if isinstance(self.dag_config[key], DagLayer):
				self.__rec_imports(self.dag_config[key].config)

	def write_imports(self):
		self.imports += '''##############################################################################
# External Package Imports Automagically Detected
##############################################################################\n'''
		    
		for key in self.import_dict:
			self.imports += "\n## Import statements for {}\n".format(key.upper())
			self.imports += "".join([item for item in self.import_dict[key] if item != ""])



	def generate_file(self):

		self.parse_dag_config()
		self.detect_external_imports()
		self.determine_block_lineage()
		self.flatten_layers()
		self.parse_layers()
		self.write_layers()
		self.write_imports()
		# pprint.pprint(self.import_dict)
		# sys.exit(1)

		for item in self.layerbag:
			if isinstance(item, DagLayer):
				print(item.exec_order,item.lineage, item.tag)

		self.output_dag = self.output_dag.format(self.dag_name,
												 self.author,
												 self.date,
												 self.imports,
												 self.dag_args,
												 "'" + self.dag_name + "'",
												 self.operators,
												 self.op_families,
												 self.layers,
												 self.structure)

		self.dag_filename = "_".join([self.dag_name.replace(" ","-"), 
									  self.author.replace(" ","-"), 
									  self.date]) + ".py"
		# with open(self.dag_filename, 'w') as file:
		# 	file.write(self.output_dag)
		# 	file.close()