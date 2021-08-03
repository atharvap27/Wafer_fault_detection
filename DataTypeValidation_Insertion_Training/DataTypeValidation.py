from application_logger.logger import App_logger
import sqlite3
from os import listdir
import shutil
import os
import csv

class dbOperation:
    """
    This class shall be used to perform database
    related operations

    """

    def __init(self):
        self.path = 'Training_Database/'
        self.goodData = 'Training_Raw_files_validated/GoodData'
        self.badData = 'Training_Raw_files_validated/BadData'
        self.logger = App_logger()

    def databaseConnection(self, DatabaseName):
        """
        Method: databaseConnection
        Description: This method shall be used for creating a new database
                     or connecting to the existing database
        return: None
        on failure: raise ConnectionError

        """

        try:
            conn = sqlite3.connect(self.path + DatabaseName + '.db')
            file = open('Training_Logs/DataBaseConnectionLog.txt', 'a+')
            self.logger.log(file, "Opened %s database successfully" % DatabaseName)
            file.close()

        except ConnectionError:
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" % ConnectionError)
            file.close()
            raise ConnectionError
        return conn

    def createTableDb(self, DatabaseName, column_names):
        """
        Method: createTableDb
        Description: It will create a table into given database.
                     GoodData files will be uploaded to the table
        return: None
        on failure: raise Exception

        """

        try:
            conn = self.databaseConnection(DatabaseName)
            c = conn.cursor()
            c.execute("SELECT count(name) FROM sqlite_master WHERE type= 'table' AND name = 'Good_Raw_data'")
            if c.fetchone()[0] == 1:
                conn.close()
                file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()
                file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % DatabaseName)
                file.close()
            else:
                for key in column_names.keys():
                    type = column_names[key]
                    try:
                        conn.execute(
                            'ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'
                                .format(column_name=key,dataType=type))
                    except:
                        conn.execute(
                            'CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))
                conn.close()

                file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()

                file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % DatabaseName)
                file.close()

        except Exception as e:
            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()
            raise e

    def insertintoTableGoodData(self, Database):
        """
        Method: insertintoTableGoodData
        Description: Insert GoodData in the table in our database
        return: None
        on failure: raise Exception

        """

        conn = self.databaseConnection(Database)
        goodFilePath = self.goodData
        badFilePath = self.badData
        log_file = open('Training_Logs/DBinsertLog.txt', 'a+')
        onlyfiles = [f for f in listdir(goodFilePath)]

        for file in onlyfiles:
            try:
                with open(goodFilePath + '/' + file) as f:
                    next(f)
                    reader = csv.reader(f, delimiter='\n')
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=(list_)))
                                self.logger.log(log_file, " %s: File loaded successfully!!" % file)
                                conn.commit()
                            except Exception as e:
                                raise e

            except Exception as e:

                conn.rollback()
                self.logger.log(log_file, "Error while creating table: %s " % e)
                shutil.move(goodFilePath + '/' + file, badFilePath)
                self.logger.log(log_file, "File Moved Successfully %s" % file)
                log_file.close()
                conn.close()

        conn.close()
        log_file.close()