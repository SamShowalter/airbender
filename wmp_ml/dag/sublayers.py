#####################################################################################
#
#
# 	Sublayer for new DAG: Owned by parent layer
#  
#	Author: Sam Showalter
#	Date: October 3, 2018
#
#####################################################################################


#####################################################################################
# External Library and Module Imports
#####################################################################################

from wmp_ml.dag.op_families import OpFamily

import pprint

#####################################################################################
# Class and Constructor
#####################################################################################


class DagSubLayer:

	def __init__(self, name, order, layer):

		self.name = layer.tag + "_" + name
		self.layer = layer
		self.refs = {}
		self.order = order
		self.op_families = []
		self.head = []
		self.tail = []
		self.num_families = 0


#####################################################################################
# Class and Constructor
#####################################################################################

	def add_op_family(self, family_id, family_ops):
		new_family = OpFamily(family_id, family_ops, self)
		self.op_families.append(OpFamily(family_id, family_ops, self))
		self.head.append(new_family.head)
		self.tail.append(new_family.tail)
		self.num_families += 1


	def write(self, head = True):
		'''
		Function that writes sublayer and its associations
		for a provided sublayer. 

		Args:
			sublayer:				Sublayer found in DAG

		'''


		sublayer_ref = None
		ref = None
		if head:
			ref = "head"
			sublayer_ref = self.head
		else:
			ref = "tail"
			sublayer_ref = self.tail

		#Sublayer string template
		sublayer_str = "\n{} = {}\n"

		#Adding partition for generating sublayers for dag HEAD
		self.layer.dag.layers += "\n## Generating {} of {} sublayer for {} dag layer"\
												.format(ref.upper(),
														self.name.upper(), 
														self.layer.tag.upper())



		if (self.head == self.tail
			and ref == 'tail'):
			self.refs[ref] = "_".join([self.name, ref])
			sublayer_str = sublayer_str.format(self.refs[ref], 
												self.refs['head'])

		else:

			#Generate the sublayer string with pretty formatting
			self.refs[ref] = "_".join([self.name, ref])
			sublayer_str = sublayer_str.format(self.refs[ref], 
															pprint.pformat(sublayer_ref)\
																.replace("'", "")\
																.replace("\n", "\n" + "\t"*6))
		
		#To prevent list collision issues in Airflow
		#Any sublayer that does not have more than one operator
		#Will not be represented as a list								
		if (len(self.op_families) == 1):
			sublayer_str = sublayer_str.replace("[","")\
										.replace("]", "")

		#Add sublayer string to parent dag
		self.layer.dag.layers += sublayer_str

	def write_op_families(self):
		for family in self.op_families:
			family.write()

	def write_operators(self):
		for family in self.op_families:
			family.write_operators()

