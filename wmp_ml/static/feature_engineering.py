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

def normalize_values(data, column_names, transformer, tag = 'Standard_Scaler_'):
    normalized_df = transformer.fit_transform(data.loc[:,
                                                            column_names])
    
    if tag == None:
    	tag = ""
    
    normalized = pd.DataFrame(normalized_df, columns = [tag + col for col in column_names])

    return normalized

def convert_boolean_df(data, boolean_name, boolean_names_and_values):
    for name_value in boolean_names_and_values:
        data[name_value[0] + "_bool"] = data[name_value[0]].replace({name_value[1]: 1,
                                                       name_value[2]: 0}, inplace = False)

def create_ordinal_df(data, ordinal_name, ordinal_dict, tag = "ord"):
    
    ordinal_df = data.loc[:,ordinal_names]
        
    if tag != None:
    	tag = "_" + tag
    else:
    	tag = ""

    #Make new ordinal column (suffix = "ord") by replacing values with ordinal dictionary
    ordinal_df[name + tag] = ordinal_df[name].replace(ordinal_dict, inplace = False)

    if tag != None:
    	ordinal_df.drop(name, inplace = True, axis = 1)
    
    return ordinal_df