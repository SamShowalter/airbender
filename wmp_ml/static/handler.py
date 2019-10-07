#####################################################################################
#
#
# 	Missing Data Preprocessing for Airflow
#  
#	Author: Sam Showalter
#	Date: October 3, 2018
#
#####################################################################################


#####################################################################################
# External Library and Module Imports
#####################################################################################

#Pandas
import pandas as pd 

#####################################################################################
# Class and Constructor
#####################################################################################

class Handler():

	def __init__(self, DagGenerator):
		self.DagGenerator = DagGenerator
		self.artifacts = []

	def contribute_artifact(self):
		pass

	def contribute_layer(self):
		pass
