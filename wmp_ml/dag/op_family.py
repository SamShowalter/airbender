#####################################################################################
#
#
# 	Op_Families for new DAG: Owned by Sublayers
#  
#	Author: Sam Showalter
#	Date: October 3, 2018
#
#####################################################################################


#####################################################################################
# External Library and Module Imports
#####################################################################################

class OpFamily:

	def __init__(self, op_family_config):

		self.op_family_config = op_family_config
		self.op_family = None
		self.head = []
		self.tail = []
