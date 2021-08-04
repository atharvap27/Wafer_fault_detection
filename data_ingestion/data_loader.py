import pandas as pd

class data_getter:
    """
    This class should be used for getting the data from the source
    """

    def __init__(self, file_object, logger_object):
        self.training_file = 'Training_FileFromDB/InputFile.csv'
        self.file_object = file_object
        self.logger_object = logger_object

    def get_data(self):
        """
        Method: get_data
        Description: This method should be used for
                     getting data from the source
        return: A pandas Dataframe
        on failure: raise Exception

        """
        self.logger_object.log(self.file_object, 'Entered the get_data method of class data_getter')
        try:
            self.data = pd.read_csv(self.training_file)
            self.logger_object.log(self.file_object,
                                   'Successfully retrieved file. Exited the get_data method of class data_getter')
            return self.data

        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured: %s' % e)
            self.logger_object.log(self.file_object,
                                   'Data getting unsuccessful. Exiting the get_data method of class data_getter')
            raise Exception