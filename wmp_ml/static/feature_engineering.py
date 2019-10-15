#####################################################################################
#
#
# 	Custom WMP_ML Feature Engineering
#  
#	Author: Sam Showalter
#	Date: October 6, 2018
#
#####################################################################################


#####################################################################################
# External Library and Module Imports
#####################################################################################


#####################################################################################
# Class and Constructor
#####################################################################################

def normalize_values(data, transformer, tag = 'Standard_Scaler_'):
    columns = data.columns
    if data.shape[0] == 1:
        data = data.values.reshape(-1,1)
    else:
        data = data.values

    normalized_df = transformer().fit_transform(data)
    
    if tag == None:
    	tag = ""
    
    normalized = pd.DataFrame(normalized_df)
    normalized.columns = [tag + col for col in columns]

    return normalized

def convert_boolean_df(data, boolean_names_and_values, tag = "_bool"):

    if data.shape[1]> 1:
        raise ValueError("Input data has more than one column")

    if tag != None:
        tag = "_" + tag
    else:
        tag = ""

    yes = boolean_names_and_values[data.name][0]
    no = column_names_and_values[data.name][0]

    data[column_name + tag] = data[column_name]\
                                        .replace({yes: 1,
                                                  no: 0}, 
                                                  inplace = False)

def create_ordinal_df(data, ordinal_dict, tag = "ord"):
    
    ordinal_df = data
        
    if tag != None:
    	tag = "_" + tag
    else:
    	tag = ""

    #Make new ordinal column (suffix = "ord") by replacing values with ordinal dictionary
    ordinal_df[data.name + tag] = ordinal_df[data.name].replace(ordinal_dict, inplace = False)

    if tag != None:
    	ordinal_df.drop(data.name, inplace = True, axis = 1)
    
    return ordinal_df