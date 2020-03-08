#####################################################################################
#
#
# 	Static utility functions for airbender dags
#  
#	Author: Sam Showalter
#	Date: October 3, 2018
#
#####################################################################################

import inspect
import types

#####################################################################################
# External Library and Module Imports
#####################################################################################

def is_callable(obj):
	'''
	Determines whether or not obj is a
	Python callable.

	Args:
		obj:			Potential Python object

	'''
	return any([inspect.isfunction(obj),
			   inspect.ismethod(obj), 
			   inspect.ismodule(obj),
			   inspect.isclass(obj),
			   isinstance(obj, types.BuiltinFunctionType)])
