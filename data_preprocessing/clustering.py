from file_operations import file_methods
import matplotlib.pyplot as plt
from kneed import KneeLocator
from sklearn.cluster import KMeans

class KMeansClustering:
    """
    This class creates the clusters of our data using KMeans.
    """

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def elbow_plot(self, data):
        """
        Method: elbow_plot
        Description: This method will give us the plot of num_of_cluster vs wcss.
                     It will help us to select optimized number of cluster.
        return: elbow plot.png and best number of clusters for the data
        on failure: raise Exception

        """

        self.logger_object.log(self.file_object, 'Entered the elbow_plot method of class KMeansClustering')
        wcss = []
        try:
            for i in range(1, 11):
                kmeans = KMeans(n_clusters=i, init='k-means++', random_state=42)
                kmeans.fit(data)
                wcss.append(kmeans.inertia_)
            plt.plot(range(1, 11), wcss)
            plt.title('The Elbow Method')
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            plt.savefig('preprocessing_data/K-Means_Elbow.PNG')
            self.kn = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')
            self.logger_object.log(self.file_object, 'The optimum number of clusters is: ' + str(
                self.kn.knee) + ' . Exited the elbow_plot method of the KMeansClustering class')
            return self.kn.knee

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in elbow_plot method of the KMeansClustering class.'
                                   ' Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,
                                   'Finding the number of clusters failed. '
                                   'Exited the elbow_plot method of the KMeansClustering class')
            raise Exception()

    def create_clusters(self, data, number_of_clusters):
        """
        Method: create_clusters
        Description: This is used to create clusters of our data.
        return: A dataframe with cluster column
        On failure: raise Exception

        """
        self.logger_object.log(self.file_object, 'Entered the create_cluster method of class KMeansClustering')
        self.data = data
        try:
            self.kmeans = KMeans(n_clusters=number_of_clusters, random_state=42)
            self.y_kmeans = self.kmeans.fit_predict(data)
            self.file_op = file_methods.File_Operation(self.file_object, self.logger_object)
            self.save_model = self.file_op.save_model(self.kmeans, 'KMeans')
            self.data['Cluster'] = self.y_kmeans
            self.logger_object.log(self.file_object, 'successfully created ' + str(self.kn.knee) +
                                   'clusters. Exited the create_clusters method of the KMeansClustering class')
            return self.data

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in create_clusters method of the KMeansClustering class.'
                                   ' Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,
                                   'Fitting the data to clusters failed. '
                                   'Exited the create_clusters method of the KMeansClustering class')
            raise Exception()