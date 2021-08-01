import os
import shutil
from application_logger.logger import App_logger
from datetime import datetime
import pandas as pd
import json
import re
from os import listdir


class Raw_Data_validation:
    """
    This class shall be used for validating
    raw training data.

    """

    def __init__(self, path):
        self.Batch_directory = path
        self.schema_path = 'schema_training.json'
        self.logger = App_logger()

    def valuesFromSchema(self):
        """
        Method: valuesFromSchema
        Description: It is used to get the schema values
                     from the given schema file
        return: Schema values
         on failure: raise key error, value error, Exception

        """

        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            NumberofColumns = dic['NumberofColumns']
            ColName = 'ColName'
            file = open('Training_Logs/valuesFromSchemaValidationLog.txt', 'r')
            msg = "LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + \
                  "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + \
                  "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(file, msg)

            file.close()

        except ValueError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open('Training_Logs/valuesFromSchemaValidationLog.txt', 'a+')
            self.logger.log(file, 'Exception occurred : %s' % e)
            file.close()
            raise Exception


    def manualRegexCreation(self):
        """
        Method: manualRegexCreation
        Description: A manual regex based on filename given
                     in the schema_training file. It is used
                     to validate the name of the file
        return: regex
        on failure: raise Exception

        """

        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex


    def createDirectoryForGoodBadRawData(self):
        """
        Method: createDirectoryForGoodBadRawData
        Description: This method creates two folder: GoodData and BadData.
        return: None
        on failure: raise OS error

        """

        try:
            path = os.path.join('Training_Raw_files_validated/', 'GoodData/')
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join('Training_Raw_files_validated/', 'BadData/')
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as oe:
            file = open('Training_Logs/GeneralLog.txt', 'a+')
            self.logger.log(file, 'OS error : %s' % oe)
            file.close()
            raise oe

    def deleteExistingGoodDataTrainingFolder(self):
        """
        Method: deleteExistingGoodDataTrainingFolder
        Description: This method is used to delete the existing
                     GoodData folder if it exists.
        return: None
        on failure: raise OS error

        """

        try:
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path + 'GoodData/'):
                shutil.rmtree(path + 'GoodData')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "GoodData directory deleted successfully!!!")
                file.close()

        except OSError as o:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, 'OS error : %s' % o)
            file.close()
            raise OSError


    def deleteExistingBadDataTrainingFolder(self):
        """
        Method: deleteExistingBadDataTrainingFolder
        Description: This method is used to delete the existing
                     BadData folder if it exists.
        return: None
        on failure: raise OS error

        """

        try:
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path + 'BadData/'):
                shutil.rmtree(path + 'BadData')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "BadData directory deleted successfully!!!")
                file.close()

        except OSError as o:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, 'OS error : %s' % o)
            file.close()
            raise OSError

    def ArchiveBadData(self):
        """
        Method: ArchiveBadData
        Description: Move the BadData folder files to
                     TrainingArchivedBadData directory.
        return: None
        on failure: raise Exception

        """

        now = datetime.now()
        date = now.date()
        time = now.strftime('%H%M%S')
        try:
            src = 'Training_Raw_files_validated/BadData/'
            if os.path.isdir(src):
                path = 'TrainingArchivedBadData'
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'TrainingArchivedBadData/BadData_' + str(date) + '_' + str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(src)
                for file in files:
                    if file not in os.listdir(dest):
                        shutil.move(src + file, dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'BadData'):
                    shutil.rmtree(path + 'BadRaw/')
                self.logger.log(file, "Bad Data Folder Deleted successfully!!")
                file.close()

        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e

    def validateFileNameRaw(self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile):
        """
        Method: validateFileNameRaw
        Description: This method is used to validate the filename of our file
                     based on training schema. We use regex for validation.
                     If the file is correct we move it to GoodData folder else
                     BadData folder.
        return: None
        on failure: raise Exception

        """

        self.deleteExistingGoodDataTrainingFolder()
        self.deleteExistingBadDataTrainingFolder()
        self.createDirectoryForGoodBadRawData()
        onlyfiles = [f for f in os.listdir(self.Batch_directory)]
        try:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtdot = re.split('.csv', filename)
                    splitAtdot = re.split('_', splitAtdot[0])
                    if len(splitAtdot[1]) == LengthOfDateStampInFile:
                        if len(splitAtdot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/GoodData")
                            self.logger.log(f, "Valid File name!! File moved to GoodData Folder :: %s" % filename)
                        else:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/BadData")
                            self.logger.log(f, "Invalid File name!! File moved to BadData Folder :: %s" % filename)
                    else:
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/BadData")
                        self.logger.log(f, "Invalid File name!! File moved to BadData Folder :: %s" % filename)
                else:
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/BadData")
                    self.logger.log(f, "Invalid File name!! File moved to BadData Folder :: %s" % filename)

            f.close()

        except Exception as e:
            file = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(file, "Error occurred while validating FileName %s" % e)
            file.close()
            raise e

    def validateColumnLength(self, NumberofColumns):
        """
        Method: validateColumnLength
        Description: Validate the length of the column using schema provided.
        return: None
        on failure: raise Exception

        """
        try:
            f = open('Training_Logs/ColumnLengthValidationLog.txt', 'a+')
            self.logger.log(f, 'Column length validation has started')
            for file in listdir('Training_Raw_files_validated/GoodData'):
                csv_file = pd.read_csv('Training_Raw_files_validated/GoodData' + file)
                if csv_file.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move('Training_Raw_files_validated/GoodData/' + file,
                                'Training_Raw_files_validated/BadData/')
                    self.logger.log(f, 'Invalid column length.File moved to BadData')
            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("Training_Logs/ColumnLengthValidationLog.txt", 'a+')
            self.logger.log(f, "Error occurred while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/ColumnLengthValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()

    def validateMissingValuesInWholeColumn(self):
        """
        Method: validateMissingValuesInWholeColumn
        Description: We use this function to see if any column have all the null values.
                     If any, we move that column to BadData folder
        return: None
        on failure: raise Exception

        """
        try:
            f = open('Training_Logs/missingValueLog.txt', 'a+')
            self.logger.log(f, 'Missing value in column validation started')
            for file in listdir('Training_Raw_files_validated/GoodData'):
                csv = pd.read_csv('Training_Raw_files_validated/GoodData' + file)
                count=0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move('Training_Raw_files_validated/GoodData/' + file,
                                    'Training_Raw_files_validated/BadData/')
                        self.logger.log(f, 'Invalid column length.File moved to BadData : %s' % file)
                        break
                if count == 0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)

        except OSError:
            f = open("Training_Logs/missingValueLog.txt", 'a+')
            self.logger.log(f, "Error occurred while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/missingValueLog.txt", 'a+')
            self.logger.log(f, "Error occurred:: %s" % e)
            f.close()
            raise e
        f.close()

