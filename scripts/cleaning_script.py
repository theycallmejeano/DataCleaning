#data_cleaning
#import libraries

#data manipulation
import pandas as pd
import numpy as np
import os
import csv

#import defined variables
from cleaning_vars import *


class DataCleaner:
    def __init__(self, filename):
        self.file = filename
        self.data = pd.read_csv(filename, index_col=0)
        self.file_name = self.file.rsplit('.', 1)[0]
        self.absolute_path = "%s_cleaned.csv" % (self.file_name)
        self.checkFile()
        

    #function to clean region names
    def clean_locations(self):
        """
        This function will standardise the area names in province and district columns, identified during the data exploration phase

        params: dataframe, p
        returns : cleaned dataframe
        """

        #replace --- entire dataset and drop entire nulls 
        frame_copy = self.data.replace({'---': np.nan}, regex = True)
        
        #drop nulls
        frame_copy = frame_copy.dropna(axis =1 , how = 'all')
        
        #standardise province names with values from province_mapping dict
        frame_copy[province_col] = frame_copy[province_col].replace(to_replace = province_map)
        
        #standardise district values to sentence case
        frame_copy[district_col] = frame_copy[district_col].applymap(lambda x: x.title() if isinstance(x, str) else x)
        
        #save as dataframe
        frame_new = pd.DataFrame(frame_copy)
        return frame_new


    #convert columns to numeric
    def get_numeric(self, loc_col):
        """
        Converts numeric object columns to their appropriate numeric form
        
        params : clean data frame from clean locations, province location
        returns : dataframe with numeric columns in their appropriate form
        """
        
        #Input dataframe from clean locations
        frame_copy = DataCleaner.clean_locations(self)
        
        #define province loc
        loc_col = province_col
        
        #create list to store object columns
        obj_list = []
        
        #extract object columns
        for obj_col in frame_copy.select_dtypes(include = [object]).columns:
            obj_list.append(obj_col)
        
        #loop for numeric column
        for col in obj_list:
            try:
                frame_copy[col] = frame_copy[col].apply(pd.to_numeric)\
                                                 .convert_dtypes() #convert applicable object columns to numeric
        
            except ValueError:
                frame_copy[col] = frame_copy[col] #retain applicable object columns

        #drop if province is null, since this shall be the grouping variable
        frame_new = frame_copy.dropna(subset = loc_col)
    
        return frame_new


    #identify nulls
    def get_missing_id(self, loc_col):
        """
        Finds the proportion of missing data across all columns in a dataframe
        
        :param frame : input dataframe
        :returns : dataframe, which indicating the proportion of missing values in each column and the column type
            
        """    
        #TODO fix this
        #read data
        loc_col = province_col
        frame = DataCleaner.get_numeric(self,loc_col)
        
        
        #find the datatypes of all columns
        col_dtypes = frame.dtypes.to_frame()\
                        .reset_index()\
                        .rename(columns = {"index":"column",
                                            0:"col_type"})
        
        #cast coltype as str, as it was causing an error otherwise
        col_dtypes['col_type'] = col_dtypes['col_type'].astype(str)
        
        #find missing proportions
        missing_cols = (frame.isnull().sum()*100/len(frame)).to_frame()\
                                        .reset_index()\
                                            .rename(columns = {"index":'column', 
                                                                0 :'prop_missing'})\
                                        .merge(col_dtypes, 
                                                how = 'left', 
                                                on = 'column')\
                                        .sort_values('prop_missing', 
                                                        ascending = False)            
        return missing_cols

    #define categorical
    def get_categorical(self, loc_col):

        """
        Identifies likely categorical variables, from object values
        params : frame, dataframe
                location_id, list of location columns, which are not categorical
                
        returns : list of categorical values
        """
        #TODO fix this
        loc_col = province_col
        #read data
        frame = DataCleaner.get_numeric(self, province_col)

        possible_cat = {}
        #ratio of unique values to approximate if the column is categorical
        for col in frame.columns:
            possible_cat[col] = 1.*frame[col].nunique()/frame[col].count() < 0.05 #threshold for categorical
            
        #convert categorical dictionary to dataframe
        categorical_frame = pd.DataFrame.from_dict(possible_cat,orient='index')\
                                    .reset_index()\
                                    .rename(columns = {"index":"column", 
                                                        0:"categorical"})
        
        #merge with missing data identifier
        missing_vals = DataCleaner.get_missing_id(self, loc_col).merge(categorical_frame, 
                                                   how = 'left', 
                                                   on = 'column')
        
        #exclude all location and identifier columns
        missing_vals = missing_vals[~missing_vals['column'].isin(loc_col)]
        
        
        return missing_vals


    #fill missing categorical values
    def fill_categorical(self, loc_col):

        """
        Fills in null categrical values, by mode of column
        params: dataframe from get_numeric, province loc
        returns :dataframe
        """
        
        loc_col = province_col
        #copy dataframe 
        frame_copy = DataCleaner.get_numeric(self, loc_col)

        #get categorical list
        categorical_list = DataCleaner.get_categorical(self, loc_col).query('col_type == "object" & categorical == True & 1<=prop_missing<=50')['column'].to_list() #select object categorical

        #only select categorical vals whose mode == 1
        new_cat = []
        for col in categorical_list:
            if len(frame_copy[col].mode())==1:
                new_cat.append(col)

        #create extended list, with categorical and location, for grouping
        #   lst_ext = new_cat + location_id

        #TODO modify this to work with group modes
        #fill categorical values
        # frame_copy[lst_ext] = frame_copy[lst_ext].groupby(location_id)\
        #                                         .apply(lambda x: x.fillna(x.mode().iloc[0])).reset_index(drop=True)


        #fill categorical with mode of column
        frame_copy[new_cat] = frame_copy[new_cat].fillna(frame_copy[new_cat].mode().iloc[0])

        return frame_copy

    #fill numeric columns
    def fill_numeric(self, loc_col):

        """
        Fills null numeric columns with median

        params:  params: dataframe from fill_categorical, province loc
        returns dataframe
        """
        
        loc_col = province_col
        #get categorical df
        frame_copy = DataCleaner.fill_categorical(self, loc_col)
        
        #identify numeric columns for filling
        numeric_fill = DataCleaner.get_missing_id(self, loc_col).query('col_type != "object" & prop_missing>0')['column'].to_list()
        
        #fill numeric with median
        frame_copy[numeric_fill] = frame_copy[numeric_fill].fillna(frame_copy[numeric_fill].median().iloc[0])
        
        return frame_copy

    
    #transform to ML readable
    def transform_data(self, loc_col, ex_col):

        """
        Encode the categotical variables, excluding certain columns as defined in col_ex, that were not necessary for analysis
        params : dataframe from fill numeric, province col, cols to exclude 
        """
        
        loc_col = province_col
        ex_col = col_ex
        
        #read numeric cleaned data
        frame_copy = DataCleaner.fill_numeric(self,loc_col)
        
        #drop columns
        frame_clean = frame_copy.drop(columns = ex_col)
        
        #identify dummy cols, all categorical
        categorical_temp = DataCleaner.get_categorical(self, loc_col)
        
        #cast coltype as str, as it was causing an error otherwise
        categorical_temp['col_type'] = categorical_temp['col_type'].astype(str)
        
        #convert to list
        categorical_lst = categorical_temp.query('categorical == True & col_type=="object"')['column'].to_list()
        
        #obtain dummy vars
        frame_cat = pd.get_dummies(data = frame_clean, 
                                  columns = categorical_lst)
        return frame_cat



    ##export
    def checkFile(self):
        """Create output file if file does not exist
        """
        if not os.path.isfile(self.absolute_path):
            open(self.absolute_path, 'w')

     
    def writeContent(self, clean_data):
        """
        Write content to csv file
        :param content: csv
        :returns csv file
        """        
        
        with open(self.absolute_path, 'w') as fp:
           writer = csv.writer(fp, delimiter=',')
           writer.writerow(clean_data.transform_data(province_col,col_ex))
        
        clean_data.transform_data(province_col,col_ex).to_csv(self.absolute_path, index = False)
        print('Successfully wrote output csv file')


