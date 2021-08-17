from sklearn.model_selection import train_test_split
from application_logger import logger
from best_model_finder import tuner
from file_operations import file_methods
from data_preprocessing import preprocessing
from data_preprocessing import clustering
from data_ingestion import data_loader

class trainModel:
    """
    It is used for training the model
    """

    def __init__(self):
        self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')
        self.logger_object = logger.App_logger()

    def trainingModel(self):
        self.logger_object.log(self.file_object, 'Start of training')
        try:
            data_getter = data_loader.data_getter(self.file_object, self.logger_object)
            data = data_getter.get_data()

            preprocess = preprocessing.Preprocessor(self.file_object, self.logger_object)
            data = preprocess.remove_columns(data, columns=['Wafer'])
            X,Y = preprocess.separate_label_feature(data, ['Output'])
            is_null_present = preprocess.is_null_present(X)
            if(is_null_present):
                X = preprocess.impute_missing_value(X)
            cols_to_drop = preprocess.get_columns_with_zero_std_deviation(X)
            X = preprocess.remove_columns(X, cols_to_drop)

            kmeans = clustering.KMeansClustering(self.file_object, self.logger_object)
            num_of_clusters = kmeans.elbow_plot(X)
            X = kmeans.create_clusters(X,num_of_clusters)
            X['Labels'] = Y
            list_of_clusters = X['Cluster'].unique()
            for i in list_of_clusters:
                cluster_data = X[X['Clusters'] == i]
                cluster_features = cluster_data.drop(['Labels', 'Cluster'], axis=1)
                cluster_label = cluster_data['Labels']
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1 / 3,
                                                                    random_state=355)
                model_finder = tuner.Model_Finder(self.file_object, self.logger_object)

                best_model_name, best_model = model_finder.get_best_model(x_train, y_train, x_test, y_test)

                file_op = file_methods.File_Operation(self.file_object, self.logger_object)
                save_model = file_op.save_model(best_model, best_model_name + str(i))

            self.logger_object.log(self.file_object, 'Successful End of Training')
            self.file_object.close()

        except Exception:
            self.logger_object.log(self.file_object, 'Unsuccessful End of Training')
            self.file_object.close()
            raise Exception