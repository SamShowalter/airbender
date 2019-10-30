#####################################################################################
#
#
# 	Custom ML Airflow Feature Engineering
#  
#	Author: Sam Showalter
#	Date: October 6, 2018
#
#####################################################################################


#####################################################################################
# External Library and Module Imports
#####################################################################################

#Import pytest
import pytest

#Helper packages
from itertools import chain
import sys
import copy

#Data packages

#Airbender
import os
import sys
sys.path.append(os.path.abspath(os.path.join(__file__, "../../")))
import airbender
from airbender.dag.generator import DagGenerator
from airbender.dag.layers import DagLayer


#####################################################################################
# Test Fixtures -- Used across files
#####################################################################################

@pytest.fixture
def obtain_correct_import_validation():
	def _obtain_correct_import_validation(config):
		dg = DagGenerator(config)
		dg.detect_external_imports()
		return set(chain(*dg.import_dict.values()))

	return _obtain_correct_import_validation