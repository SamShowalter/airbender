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
from sklearn.preprocessing import LabelEncoder

#####################################################################################
# Class and Constructor
#####################################################################################

def normalize_values(data, prefit = None):
    
    mean = None
    std = None

    if not isinstance(data, pd.Series):
        print(type(data))
        raise ValueError("Input data has more than one column")

    if prefit:
        std = prefit['std']
        mean = prefit['mean']
    else:
        std = data.std()
        mean = data.mean()

    if std == 0:
        raise ValueError("ERROR: Data is all the same value and provides not insight.\
        Please re-create dag without column included.")

    data = (data - mean) / std

    if prefit:
        return data
    else:
        prefit = {'mean': mean, 'std': std}
        return data, prefit

def encode_labels(data, prefit = None):
    col_name = data.name
    le = None

    if prefit:
        
        data = pd.Series(prefit['label_encoder'].transform(data), name = col_name)
        return data

    else:
        le = LabelEncoder()
        data = pd.Series(le.fit_transform(data), name = col_name)

        return data, {'label_encoder': le}

def winsorize(data, limits = [0.05, 0.05], prefit = None):

    feature = data

    lower = None
    upper = None

    if prefit:
        lower = prefit['lower']
        upper = prefit['upper']

    else:
        lower = feature.quantile(limits[0])
        upper = feature.quantile(1 - limits[1])

    #Winsorize
    feature[feature > upper] = upper
    feature[feature < lower] = lower

    if prefit:
        return feature
        
    return feature, {'upper': upper, 'lower': lower}

def linear_transformation(data, method, prefit = None):

    if prefit:
        res = method(data, lmbda = prefit['lambda_c'])
        return pd.Series(res, name = data.name)

    else:
        res, lambda_c = method(data)
        return pd.Series(res, name = data.name), {'lambda_c': lambda_c}

    
def create_boolean_df(data, boolean_names_and_values):

    boolean_df = data

    if not isinstance(boolean_df, pd.Series):
        raise ValueError("Input data has more than one column")

    yes = boolean_names_and_values[data.name][0]
    no = boolean_names_and_values[data.name][1]

    boolean_df = boolean_df.replace({yes: 1,
                                  no: 0}, 
                                  inplace = False)

    boolean_df.name = boolean_df.name

    return boolean_df

def create_ordinal_df(data, ordinal_dict):
    
    ordinal_df = data
        
    #Make new ordinal column (suffix = "ord") by replacing values with ordinal dictionary
    ordinal_df = ordinal_df.replace(ordinal_dict, inplace = False)

    ordinal_df.name = ordinal_df.name
    
    return ordinal_df