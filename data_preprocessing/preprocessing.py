import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer

class Preprocessor:
    """
    This class shall be used for performing all preprocessing
    steps on the data
    """

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def remove_columns(self, data, columns):
        """
        Method: remove_columns
        Description: Used to remove the mentioned columns from the dataset
        return: Pandas Dataframe after removing the columns
        on failure: raise Exception

        """

        self.logger_object.log(self.file_object, 'Entered the remove_columns method of Preprocessor class')
        self.data = data
        self.columns = columns
        try:
            self.useful_data = self.data.drop(labels=self.columns, axis=1)
            self.logger_object.log(self.file_object,
                                   'Column removal Successful. '
                                   'Exited the remove_columns method of the Preprocessor class')
            return self.useful_data

        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured : %s' % e)
            self.logger_object.log(self.file_object,
                                   'Removing columns unsuccessful.'
                                   'Exited the remove_columns method of the Preprocessor class')
            raise Exception


    def separate_label_feature(self, data, label_feature):
        """
        Method: separate_label_feature
        Description: Separate the label column from the original dataset
        return: Updated Pandas dataframe
        on failure: raise Exception

        """
        self.logger_object.log(self.file_object, 'Entered the separate_label_feature method of the Preprocessor class')
        try:
            self.X = data.drop(labels=label_feature, axis=1)
            self.Y = data[label_feature]
            self.logger_object.log(self.file_object,
                                   'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
            return self.X, self.Y

        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured : %s' % e)
            self.logger_object.log(self.file_object,
                                   'Separating label feature unsuccessful.'
                                   'Exited the separate_label_feature method of the Preprocessor class')
            raise Exception

    def is_null_present(self,data):
        """
        Method: is_null_present
        Description: Used to check whether there are any missing values in our dataset
        return: A boolean value. True if there is a null value, false otherwise.
        on failure: raise Exception

        """

        self.logger_object.log(self.file_object, 'Entered the is_null_present method of class Preprocessor')
        self.null_present = False
        try:
            self.null_counts = data.isna().sum()
            for i in self.null_counts:
                if i>0:
                    self.null_present = True
                    break
            if(self.null_present):
                dataframe_with_null = pd.DataFrame()
                dataframe_with_null['columns'] = data.columns()
                dataframe_with_null['missing value count'] = np.asarray(data.isna().sum())
                dataframe_with_null.to_csv('preprocessing_data/null_values.csv')
            self.logger_object.log(self.file_object,
                                   'Finding missing values is a success.Data written to the null values file.'
                                   ' Exited the is_null_present method of the Preprocessor class')
            return self.null_present

        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception has occured : % s' % e)
            self.logger_object.log(self.file_object, 'Unsuccessful null value counting.'
                                                     'Exited the is_null_present method of the Preprocessor class')
            raise Exception

    def impute_missing_value(self, data):
        """
        Method: impute_missing_value
        Description: Used to convert all the missing values using KNN imputer
        return: A converted pandas Dataframe
        on failure: raise Exception

        """

        self.logger_object.log(self.file_object,
                               'Entered the impute_missing_values method of class Preprocessor')
        self.data = data
        try:
            imputer = KNNImputer(n_neighbors=3, weights='uniform', missing_values= np.nan)
            self.new_array = imputer.fit_transform(self.data)
            self.new_data = pd.DataFrame(data=self.new_array, columns=self.data.columns)
            self.logger_object.log(self.file_object,
                                   'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
            return self.new_data

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in impute_missing_values method of the Preprocessor class.'
                                   ' Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,
                                   'Imputing missing values failed. '
                                   'Exited the impute_missing_values method of the Preprocessor class')
            raise Exception()


    def get_columns_with_zero_std_deviation(self, data):
        """
        Method: get_columns_with_zero_std_deviation
        Description: It will give us the columns which have zero standard deviation
        return: List of the columns with standard deviation of zero
        On failure: Raise Exception
        """

        self.logger_object.log(self.file_object,
                               'Entered the get_columns_with_zero_std_deviation method of the Preprocessor class')
        self.columns = data.columns
        self.data_n = data.describe()
        self.col_to_drop = []
        try:
            for col in self.columns:
                if(self.data_n[col]['std'] == 0):
                    self.col_to_drop.append(col)
            self.logger_object.log(self.file_object,
                                   'Column search for Standard Deviation of Zero Successful. '
                                   'Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            return self.col_to_drop

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occurred in get_columns_with_zero_std_deviation method of the '
                                   'Preprocessor class. ''Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,
                                   'Column search for Standard Deviation of Zero Failed. '
                                   'Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            raise Exception()