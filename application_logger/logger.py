from datetime import datetime

class App_logger():

    """
    This class should be used for creating
    logging functions.
    """

    def __init__(self):
        pass

    def log(self, file_object, logging_message):
        self.now = datetime.now()    # get the current datetime
        self.date = self.now.strftime("%H:%M:%S")     # current date
        self.current_time = self.now.time()    # current_time
        file_object.write(str(self.date) + "/" + str(self.current_time)
                          + "\t\t" + logging_message + "\n")   # log current date, time and message.
