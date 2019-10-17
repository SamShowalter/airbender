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

import pandas as pd

#####################################################################################
# Class and Constructor
#####################################################################################

def normalize_values(data, tag = 'Standard_Scaler_'):
    
    if not isinstance(data, pd.Series):
        raise ValueError("Input data has more than one column")

    if data.std() == 0:
        raise ValueError("ERROR: Data is all the same value and provides not insight.\
        Please re-create dag without column included.")

    if tag != None:
        tag = "_" + tag
    else:
        tag = ""

    data = (data - data.mean()) / data.std()

    return data

def mla_get_dummies(data):

    dummy_df = pd.get_dummies(data)

    return dummy_df

def mla_linear_transformation(data, method):

    res, _ = method(data)

    return pd.Series(res, name = data.name)



def create_boolean_df(data, boolean_names_and_values, tag = "_bool"):

    boolean_df = data

    if not isinstance(boolean_df, pd.Series):
        raise ValueError("Input data has more than one column")

    if tag != None:
        tag = "_" + tag
    else:
        tag = ""

    yes = boolean_names_and_values[data.name][0]
    no = boolean_names_and_values[data.name][1]

    boolean_df = boolean_df.replace({yes: 1,
                                  no: 0}, 
                                  inplace = False)

    boolean_df.name = boolean_df.name + tag


    return boolean_df

def create_ordinal_df(data, ordinal_dict, tag = "ord"):
    
    ordinal_df = data
        
    if tag != None:
    	tag = "_" + tag
    else:
    	tag = ""

    #Make new ordinal column (suffix = "ord") by replacing values with ordinal dictionary
    ordinal_df = ordinal_df.replace(ordinal_dict, inplace = False)

    ordinal_df.name = ordinal_df.name + tag
    
    return ordinal_df