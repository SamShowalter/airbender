#Provided function for detecting outliers
def detect_outliers(col_names, data, std_thresh = 6):
    
    for column in col_names:
        
        #Create z_score proxy for each column
        data['z_score'] = np.absolute(zscore(data[column]))
        
        #Determine if there are outliers, as defined by z_score threshold
        outliers = data.loc[data.z_score > std_thresh, [column, 'z_score']]
        
        #If there are no outliers
        if outliers.shape[0] == 0:
            print("No outliers for column {} at threshold of {} stdevs".format(column, std_thresh))
        
        #If there are outliers
        else:
            print("\n {} outlier(s) found for column {} at threshold of {} stdevs. See below".format(outliers.shape[0],
                                                                                        column, std_thresh))
            print("\n")
            print(outliers)
            print("\n")
        
        #Drop z_score from data
        data.drop('z_score', axis = 1, inplace = True)

    return data