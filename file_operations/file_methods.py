import os
import shutil
import pickle

class File_Operation:

    """
    This class shall be used for model function
    creations like saving the model

    """

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.model_directory = 'models/'

    def save_model(self, model, filename):
        """
        Method:  save_model
        Description:  This method shall be used for saving the model
        return:  "success" message
        on failure:  raise Exception


        """

        self.logger_object.log(self.file_object, 'Entered the save_model method of class File_Operation')
        try:
            path = os.path.join(self.model_directory, filename)
            if os.path.isdir(path):
                shutil.rmtree(self.model_directory)
                os.makedirs(path)
            else:
                os.makedirs(path)
            with open(path + "/" + filename + ".sav", 'wb') as f:
                pickle.dump(model, f)
            self.logger_object.log(self.file_object, 'Model file' +
                                   filename + 'saved. Exiting the save_model method of class File_Operation')
            return 'success'

        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured : %s' % e)
            self.logger_object.log(self.file_object,
                                   'Model' + filename + 'saving unsuccessful.'
                                                        ' Exiting the method save_model of class File_Operations')
            raise Exception

    def load_model(self, filename):

        """
        Method:  load_model
        Description: This method should be used for loading the saved model
        return:  loaded model
        On failure:  raise Exception


        """

        self.logger_object.log(self.file_object,
                               'Entered the load_model method of class File_Operation')
        try:
            with open(self.model_directory + filename +
                      "/" + filename + ".sav", "rb") as f:
                self.logger_object.log(self.file_object, 'Model file' +
                                       filename + 'loaded successfully')
                return pickle.load(f)

        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured : %s' % e)
            self.logger_object.log(self.file_object, 'Exited the load_model method of class File_Operation')
            raise e

    def find_correct_model_file(self, cluster_number):
        """
        Method:  find_correct_model_file
        Description: find correct model file based on cluster number
        return: correct model file
        on failure:  raise Exception


        """

        self.logger_object.log(self.file_object,
                               'Entered the find_correct_model_file method of class File_Operation')
        try:
            self.cluster_number = cluster_number
            self.folder_name = self.model_directory
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)
            for self.file in self.list_of_files:
                try:
                    if (self.file.index(str(self.cluster_number)) != -1):
                        self.model_name = self.file
                except:
                    continue
            self.model_name = self.model_name.split('.')[0]
            self.logger_object.log(self.file_object,
                                   'Exited the find_correct_model_file method of the Model_Finder class.')
            return self.model_name

        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occurred : %s' % e)
            self.logger_object.log(self.file_object,
                                   'Exiting the find_correct_model_file method of class File_Operation')
            raise e