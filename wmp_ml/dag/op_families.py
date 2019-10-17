#####################################################################################
#
#
# 	Op_Families for new DAG: Owned by Sublayers
#  
#	Author: Sam Showalter
#	Date: October 3, 2018
#
#####################################################################################

from wmp_ml.dag.operators import DagOperator

#####################################################################################
# External Library and Module Imports
#####################################################################################

class OpFamily:

	def __init__(self, family_id, members, sublayer):

		self.family_id = family_id
		self.members = members
		self.sublayer = sublayer
		self.head = members[-1].task_id
		self.tail = members[0].task_id

		#Give faimly reference to all members
		for operator in members:
			operator.op_family = self

	def write(self):
		'''
		Write out associations for an entire operator family.

		Args:
			family:						String representing family_id
			members:					Dictionary of family members

		'''

		#Templated string
		op_family_string ='''\n{} = {}'''

		#Isolate the names of the members (variable name where family will be stored)
		member_list = [op.task_id for op in self.members]

		#Write out all family members and their sequential association
		member_string = " >> ".join(member_list).replace("'", "")

		#Add family to list of operator families
		self.sublayer.layer.dag.op_families += op_family_string.format(self.family_id, 
																		member_string)

	def write_operators(self):
		for operator in self.members:
			operator.write()