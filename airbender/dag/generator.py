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
from airbender.dag.layers import DagLayer
from airbender.dag.utils import is_callable


#####################################################################################
# Class and Constructor
#####################################################################################

class DagGenerator():
	'''
	Dag generator orchestration object. This object will take a specific configuration
	and generate an airflow ML experiment for you.
	'''
	def __init__(self, config):

		#Configuration variables
		self.config = config

		#Check to see if there is a dag config
		try:
			self.dag_config = config['config']
		except:
			raise AttributeError("DAG Config not specified. Please specify a config and try again.")

		#Owner of the experiment
		try:
			self.author = self.config['dag']['owner']
		except:
			raise AttributeError("DAG Owner not specified. Please specify an author and try again.")

		#Name of the experiment
		try:
			self.dag_name = self.config['dag_name']
		except:
			raise AttributeError("DAG Name not specified. Please specify a dag name and try again.")

		#Date of execution
		self.date = datetime.now().strftime("%m-%d-%Y--%H.%M.%S")

		#Package import information
		self.import_dict = {}
		self.import_check = set()
		self.imports = ''''''

		#Dag operator information
		#Including operator families
		self.tasks = set()

		#Collection of operator families
		self.op_family_ids = set()

		#OPerator and op_family strings
		self.operators = ''''''
		self.op_families = ''''''
		
		#Dag layers information

		self.layerbag = []
		self.layer_tags = []
		self.layers = ''''''
		self.structure = ''''''
		
		#Dag models information (for informing evaluation)
		self.models = {}
		
		
		# Output dag information
		self.output_dag = None
		with open(
				os.path.abspath(
					os.path.join(__file__, 
						'../../config/dag_template.txt')), 'r') as template:
			self.output_dag = template.read()

		#Default configuration hierarchy (config driven)
		self.execution_hierarchy_config_path = os.path.abspath(
											os.path.join(__file__, 
												'../../config/dag_hierarchy.cfg'))

		#Update execution hierarchy with hierarchy at provided filepath above
		self.execution_hierarchy = None
		with open(self.execution_hierarchy_config_path) as exec_config:
			self.execution_hierarchy = json.load(exec_config)

		self.validate_dag_config()

		#Dag argument defaults
		self.dag_args = {
						    'owner': 'airflow',
						    'depends_on_past': False,
						    'email': ['airbender@defaultemail.com'],
						    'email_on_failure': False,
						    'email_on_retry': False,
						    'retries': 0,
						    'op_args':{},
						    'op_kwargs': {},
						    'params': {}
						    # 'retry_delay': str(timedelta(minutes=5)),
						    # 'queue': 'bash_queue',
						    # 'pool': 'backfill',
						    # 'priority_weight': 10,
						    # 'end_date': datetime(2016, 1, 1),
						}

		#Update parameters of self.dag_args that are provided by user
		for key in self.config['dag']:
			self.dag_args[key] = self.config['dag'][key]

		#Convert dag arguments dictionary to a string
		self.dag_args = json.dumps(self.dag_args, indent = 4)\
							.replace('false', 'False')\
							.replace('true','True')\
							.replace('null', 'None')

#####################################################################################
# Orchestration Method, Executing all Logic
#####################################################################################

	def generate_file(self):
		'''
		Orchestration function for the entire dag. This method calls all of the necessary
		auxiliary methods for created the DAG experiment as a Python file.

		'''

		#Parse the user-provided dag configuration
		self.parse_dag_config()

		#Detect external imports
		self.detect_external_imports()

		#Determine lineage for each layer
		#And flatten into single sequence
		self.determine_layer_lineage()
		self.flatten_layers()

		#Parse all layers in the dag
		self.parse_layers()

		#Write all layer information to dag output
		self.write_layers()

		# #Connect all of the layers
		self.connect_layers()

		#Write all imports to dag output
		self.write_imports()


		#Debugging statement. Can be commented out or removed
		print("\nDisplaying Ordered Dag Layers with Tags:\n")
		for item in self.layerbag:
			if isinstance(item, DagLayer):
				print(item.exec_order,
					  item.lineage)

		#Create output dag string
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

		#Generate the dag filename
		self.dag_filename = "_".join([self.dag_name.replace(" ","-"), 
									  self.author.replace(" ","-"), 
									  self.date]) + ".py"
		
		#Write the dag configuration
		print(os.getcwd())
		self.file_root = "../../../airflow/dags/"

		#Used for debugging, can be removed
		print("\nGenerated airflow file with name: {}"\
			.format(os.path.abspath(self.dag_filename).split("/")[-1]))

		#Write the file
		with open(self.file_root + self.dag_filename, 'w') as file:
			file.write(self.output_dag)
			file.close()

#####################################################################################
# Supplemental Public Methods
#####################################################################################

	def parse_dag_config(self):
		'''
		Orchestration run for parsing the dag configuration

		Sub_Function:
			__rec_dag_config:		Rescursive DAG configuration order locking

		'''

		self.__rec_dag_config(self.dag_config)


	def detect_external_imports(self):
		'''
		Orchestration function for detecting all of 
		the potential callables in the import configuration.
		
		Sub_Function:
			__rec_imports:				Recursively import all callables

		'''

		#Iterate through all of the conceptual layers
		#Of the dag config
		for key in self.dag_config:

			#If the value is a dictionary
			if isinstance(self.dag_config[key], dict):
				self.__rec_imports(self.dag_config[key])

			#If the item is a list (should NOT be a set)
			# Sets do not have order
			if isinstance(self.dag_config[key], list):
				for item in self.dag_config[key]:
					self.__rec_imports(item)

			#If the item is a DagLayer Object
			if isinstance(self.dag_config[key], DagLayer):
				self.__rec_imports(self.dag_config[key].config)

	def __validate_dag_config(self, lineage, sub_config):

		if (not isinstance(sub_config, DagLayer) and 
			isinstance(sub_config, dict)):
			for key in sub_config:
				if not isinstance(key, str):
					raise AttributeError("""All keys in conceptual Dag Layer configuration must be strings.
This ensures that all physical DAG layers have approparite tags for understandability.
\nPlease check your config at the following lineage: {}
Data type of key found: {}""".format(lineage, type(key)))
				self.__validate_dag_config(lineage + [key], sub_config[key])

		elif (isinstance(sub_config, set) or 
			isinstance(sub_config, list)):
			for dag_layer in sub_config:
				if not isinstance(dag_layer, DagLayer):
					raise AttributeError("""Found a non-DagLayer object in a list / set.
Lists / sets can only contain DagLayers.
Please track the provided lineage and check your inputs.
\nLineage: {}
Type of object found: {}""".format(lineage, type(dag_layer)))

		elif isinstance(sub_config, DagLayer):
			#Appropriate mapping
			return

		else:
			raise AttributeError("""Unexpected object found in the Dag Configuration.
Key-Value pairs in the Dag Configuration must be one of the following:
- String -> dictionary
- String -> list or set (list preferred to maintain order)
- String -> DagLayer
\n Please check your inputs at lineage {}
Type of object found: {}""".format(lineage, type(sub_config)))




	def validate_dag_config(self):
		'''
		Validates configuration to ensure that it is of an 
		acceptable structure. The configuration MUST be
		represented in a python dictionary. Each operation 
		must also be either None, or a sub-dictionary.

		'''

		if self.dag_config == {} or None:
			raise AttributeError("""The Dag Generator configuration you provided is empty. 
Please check your inputs.""")

		#Must be a dictionary
		if not isinstance(self.dag_config, dict):
			raise AttributeError("""Airbender Dag configuration must be a dictionary. 
Please check your inputs.""")

		#For each key, the value must be None or a dictionary
		for key in self.dag_config:

			#Check information for operator families
			if key not in list(self.execution_hierarchy.keys()):
				raise AttributeError('''An unrecognized conceptual DAG layer (dag_config keyword) was discovered.
\nValue found: {}
\nMake sure all keys belong to execution hierarchy configuration.
Valid conceptual dag layers were set for this experiment to be:\n
 - {}'''.format(key, "\n - ".join(list(self.execution_hierarchy.keys()))))

			if not any([isinstance(self.dag_config[key], dict),
				   isinstance(self.dag_config[key], set),
				   isinstance(self.dag_config[key], DagLayer)]):
				raise AttributeError("""Conceptual Dag layer configuration must be a dictionary, DagLayer, or list.
\nPlease check your inputs for conceptual Dag layer = {}
Type of object that was found = {}""".format(key, type(self.dag_config[key])))

			self.__validate_dag_config([key], self.dag_config[key])


	def determine_layer_lineage(self):
		'''
		Orchestration for determining layer lineage.
		For each concept layer provided in the configuration,
		initialize a spot in a list.

		Then, recursively populate this list using the __layer_lineage 
		function. Finally, remove any empty cells and attribute the 
		resultant artifact to the layerbag.

		'''

		#Initialize list for all conceptual layers in the hierarchy
		self.layerbag = [[]]*len(self.execution_hierarchy.keys())

		#Determine lineage for all dag layers and populate layerbag
		self.__layer_lineage(self.dag_config, [])

		#Remove missing cells in the layerbag
		self.layerbag = [layer for layer in self.layerbag if layer != []]


	def flatten_layers(self):
		'''
		Function to orchestrate the flattening of the nested
		Dag Layer configuration.

		Sub_Function:
			__flatten_layers:			Recursively flatter layer configuration
			
		'''
		self.layerbag = self.__flatten_layers(self.layerbag, [])

		#Re-assign order
		for i in range(len(self.layerbag)):
			self.layerbag[i].exec_order = i


	def parse_layers(self):
		'''
		Parse all layers in the DAG's 
		layerbag. This is a HUGE operation for
		the layers and assembles all code in the 
		Daglayer in the correct format

		'''
		for layer in self.layerbag:
			layer.parse_layer()


	def write_layers(self):
		'''
		Writes the logic for all layers in the
		dag to the dag itself. This is one of the final
		steps before the final output file for the 
		airflow dag is generated.

		'''
		for layer in self.layerbag:

			#Write everything related to layer
			layer.write_operators()
			layer.write_op_families()
			layer.write_sublayers()


	def connect_layers(self):
		'''
		Connect all layers of the graph together, including 
		any complicated sublayer connections or unrolling of previous
		layers. 
		'''

		#Template for generating connected layer
		connected_layer = "\n{}"

		#Add layers to the final dag.
		#TODO: This will need to be changed to fix Airflow dependency issues
		for layer_index in range(len(self.layerbag) - 1):

				if isinstance(self.layerbag[layer_index + 1].tail, dict):
					chain_items = list(self.layerbag[layer_index + 1].tail.values())
					self.structure += "\nchain({}, {})".format(
														self.layerbag[layer_index].head,
														pprint.pformat(chain_items)).replace("'", "")

				else:
					self.structure += connected_layer\
											.format(" >> "\
												.join([self.layerbag[layer_index]\
																	.head,
													   self.layerbag[layer_index + 1]\
													   				.tail])\
																		.replace("'", ""))


	def write_imports(self):
		'''
		Write all of the imports found programmatically in the
		configuration or that were used in the generation of
		the layer and operator functionality.

		'''

		#Partition the imports section
		self.imports += '''##############################################################################
# External Package Imports Automagically Detected
##############################################################################\n'''
		
		#Iterate through the items of the import dictionary
		for key in self.import_dict:
			self.imports += "\n## Import statements for {}\n".format(key.upper())
			self.imports += "".join([item for item in self.import_dict[key] if item != ""])


	def import_dynamically(self, obj):
		'''
		Function that generates the string necessary to
		ensure that all functions will be imported correctly in the 
		final output python file that is sent to airflow.

		Args:
			obj:				Potential object to be imported
		'''
		import_statement = None
		module = None

		try:
			#If we do not already have the import
			if obj not in self.import_check:
				import_statement = "from {} import {}\n".format(
				                                              obj.__module__,
				                                              obj.__name__)
				module = obj.__module__

				#Add item to the import check
				self.import_check.add(obj)

				#Add item to the import dictionary and categorize by module
				self.import_dict.setdefault(module, []).append(import_statement)

		except Exception as e:

			print("Dynamic import failed. Trying simpler import.")
			module = "Abnormal Imports"

			#Simpler import statement
			import_statement = 'import {}\n'.format(obj.__name__)

			#Add item to the import check
			self.import_dict.setdefault(module, []).append(import_statement)

			#Add item to the import check
			self.import_check.add(obj)


#####################################################################################
# Supplemental Private Methods
#####################################################################################

	def __rec_dag_config(self, config_section):
		'''
		Recursively locks in order of input configuration 
		such that the order of the tasks is maintained

		Args:
			config_section:			Section or sub-section of configuration

		'''
		if isinstance(config_section, dict):
			for key in config_section:
				if isinstance(config_section[key], dict):
					config_section[key] = OrderedDict(config_section[key])
					self.__rec_dag_config(config_section[key])

	
	def __layer_lineage(self, subsection, lineage):
		'''
		Conceptual layer lineage function, recursively executed. 
		For a given conceptual layer in the dag it determines 
		which physical DagLayers belong and delineates them

		Args:
			subsection:				Subsection of configuration to parse
			lineage:				Lineage of conceptual layer, provided

		Sub_Function:
			__layer_delineate:		Delineates layer with all information
		'''

		#If the subsection is a DagLayer
		#Delineate
		if isinstance(subsection, DagLayer):
			
			#Delineate layer
			self.__layer_delineate(subsection, lineage, None)


		#If the subsection is a list or set of length one
		#Delineate and remove list data structure
		elif ((isinstance(subsection, set) or
		     isinstance(subsection,list)) and 
			len(subsection) == 1):

			#Pluck item from list
			subsection = subsection[0]

			#Delineate layer
			self.__layer_delineate(subsection, lineage, None)


		#If subsection is a list or set with length longer
		# Than one, Delineate and add subrank order
		elif (isinstance(subsection, set) or
			 isinstance(subsection, list)):

			#Keeping running tally of subrank order
			order = 0
			sublayer = []

			#Iterate through data store
			for item in subsection:
				if isinstance(item, DagLayer):

					#Delineate as a subranked subsection, which will return layer order
					layer_order = self.__layer_delineate(item, lineage, order)
					sublayer.append(item)
					order += 1

			#Add new physical artifact to conceptual layer
			self.layerbag[layer_order] = self.layerbag[layer_order] + [sublayer]
		
		#If it is a dictionary, recurse on itself and track lineage
		elif (isinstance(subsection,dict)):
				for key in subsection:
					self.__layer_lineage(subsection[key], lineage + [key])

	def __layer_delineate(self, subsection, lineage, subrank = None):
		"""
		Delineates the layer of the dag by giving it all of the heritage
		and parent hierarchy information needed to construct the
		full graph.
		"""
		#Clear out existing layer in case things have changed
		subsection.clear()

		#Give new attributes and order to layer
		layer_order = self.execution_hierarchy[lineage[0]]['order']
		conditional_mapping = self.execution_hierarchy[lineage[0]]['map']
		split = self.execution_hierarchy[lineage[0]]['split']
		subsection.delineate(lineage, 
					  order = layer_order, subrank = subrank, 
					  conditional_mapping = conditional_mapping,
					  split = split,
					  dag = self)

		if subrank is None:

			#Add new physical artifact to conceptual layer
			self.layerbag[layer_order] = self.layerbag[layer_order] + [subsection]
		
		else:
			return layer_order
					

	def __flatten_layers(self, l, sublist):
		'''
		Recursively flattens layers into a single layer stack. This will
		ultimately generate the dag network. 

		Args:
			l:				Full list of layers
			sublist:		Nested list, which may have more lists or layers

		Raises:
			ValueError:		If two layers have the same tag

		Returns:
			sublist:		sublist of items found in nested lists

		'''

		#Iterate through list 
		for item in l:

			#If you find a sublist, recurse on it
			if isinstance(item, list):
				self.__flatten_layers(item, sublist)

			else:
				#Add all of the items, assuming they are not duplicated
				if item in self.layer_tags:
					raise ValueError('''Two DagLayers have the same unique tag. 
									Please check your inputs.''')
				
				#Add term to layer tags, and to sublist
				self.layer_tags.append(item.tag)
				sublist.append(item)

		return sublist
    


	def __rec_imports(self,
					  config_section):
		"""
		Recursive function that traverses through a configuration
		dictionary, previously validated by the owning dag layer,
		and generated import statements for all non-primitive objects 
		it finds. This is crucial for the final script to compile.

		Args:
			config_section:					Section or subsection of DAG configuration

		"""
 
		try:

			#If the section is a Dag layer, switch to its config file
			if isinstance(config_section, DagLayer):
				config_section = config_section.config

			#Iterate through the config section
			for key in config_section:

				#If the file is a set or list
				# Recursively import on the items
				if (isinstance(key, set) or
					isinstance(key, list)):
					for item in key:
						self.__rec_imports(item)

				#If the key is a DagLayer
				#Recurse on the DagLayer
				elif isinstance(key, DagLayer):
					self.__rec_imports(key)

				#If the key is callable
				#Import it dynamically
				elif (is_callable(key) and not 
					  isinstance(key, DagLayer)):

					self.import_dynamically(key)
				
				#If the configuration is a dictionary
				if (not isinstance(config_section, DagLayer) and
					isinstance(config_section, dict)):

					#If the value is not a dag layer and is callable
					#Import it dynamically
					if (not isinstance(config_section[key], DagLayer) and
						is_callable(config_section[key])):

						self.import_dynamically(config_section[key])


					# If not, then recursively try to import the value
					else:
						self.__rec_imports(config_section[key])
	    
		#Will run into errors because checks everything
		#This should not be an issue, but could be more efficient
		except Exception as e:
			#print(str(e))
			pass
	            
