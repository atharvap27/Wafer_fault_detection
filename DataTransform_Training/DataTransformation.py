from os import listdir
from application_logger.logger import App_logger
import pandas as pd

class dataTransform:
    """
    This class shall be used for converting the data
    into suitable format for pushing it to database.

    """

    def __init__(self):
        self.goodDataPath = 'Training_Raw_files_validated/GoodData'
        self.logger = App_logger()

    def missingValuetoNULL(self):
        """
        Method: missingValuetoNULL
        Description: This method shall be used to convert
                     the missing values in our dataset to NULL
                     so that we can load it in our dataset
        return: None
        on failure: raise Exception

        """
        log_file = open('Training_Logs/dataTransformLog.txt', 'a+')
        try:
            onlyfiles = [f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                csv = pd.read_csv(self.goodDataPath + '/' + file)
                csv.fillna('NULL', inplace=True)
                csv['wafer'] = csv['wafer'].str[6:]
                csv.to_csv(self.goodDataPath + '/' + file, header=True, index=None)
                self.logger.log(log_file, 'Successfully converted the missing values to NULL: %s' % file)

        except Exception as e:
            self.logger.log(log_file, 'DataTransform unsuccessful : %s' % e)
            log_file.close()
        log_file.close()