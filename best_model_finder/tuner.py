from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, roc_auc_score

class Model_Finder:
    """
    This class shall be used to find the
    model with best accuracy and roc_auc_score

    """

    def __init__(self, file_object, logging_object):
        self.file_object = file_object
        self.logger_object = logging_object
        self.clf = RandomForestClassifier
        self.xgb = XGBClassifier

    def get_best_params_for_RandomForest(self, train_x, train_y):
        """
        Method: get_best_params_for_RandomForest
        Description: It is used to find the best parameters for
                     Random Forest Classifier
        return: Model with best parameters
        on failure: raise Exception

        """

        self.logger_object.log(self.file_object, 'Finding the best parameters for Random Forest')
        try:
            self.rf_params = {"n_estimators": [10, 50, 100, 130], "criterion": ['gini', 'entropy'],
                               "max_depth": range(2, 4, 1), "max_features": ['auto', 'log2']}
            self.grid = GridSearchCV(self.clf, self.rf_params, cv=5, verbose=3)
            self.grid.fit(train_x, train_y)
            self.criterion = self.grid.best_params_['criterion']
            self.n_estimators = self.grid.best_params_['n_estimators']
            self.max_depth = self.grid.best_params_['max_depth']
            self.max_features = self.grid.best_params_['max_features']
            self.clf = RandomForestClassifier(n_estimators=self.n_estimators, criterion=self.criterion,
                                              max_depth=self.max_depth, max_features=self.max_features)
            self.clf.fit(train_x, train_y)
            self.logger_object.log(self.file_object,
                                   'Random Forest best params: ' + str(self.grid.best_params_) +
                                   '. Exited the get_best_params_for_random_forest method of the Model_Finder class')

            return self.clf

        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occurred in get_best_params_for_random_forest '
                                   'method of the Model_Finder class.'' Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,
                                   'Random Forest Parameter tuning  failed. '
                                   'Exited the get_best_params_for_random_forest method of the Model_Finder class')
            raise Exception()


    def get_best_params_for_xgboost(self, train_x, train_y):
        """
        Method: get_best_params_for_xgboost
        Description: It is used to find the best parameters for
                     XGBoost Classifier
        return: Model with best parameters
        on failure: raise Exception

        """
        self.logger_object.log(self.file_object,
                               'Entered the get_best_params_for_xgboost method of the Model_Finder class')
        try:
            self.param_grid_xgboost = {

                'learning_rate': [0.5, 0.1, 0.01, 0.001],
                'max_depth': [3, 5, 10, 20],
                'n_estimators': [10, 50, 100, 200]

            }
            self.grid = GridSearchCV(XGBClassifier(objective='binary:logistic'), self.param_grid_xgboost, verbose=3,
                                     cv=5)
            self.grid.fit(train_x, train_y)

            self.learning_rate = self.grid.best_params_['learning_rate']
            self.max_depth = self.grid.best_params_['max_depth']
            self.n_estimators = self.grid.best_params_['n_estimators']

            self.xgb = XGBClassifier(learning_rate=self.learning_rate, max_depth=self.max_depth,
                                     n_estimators=self.n_estimators)
            self.xgb.fit(train_x, train_y)
            self.logger_object.log(self.file_object,
                                   'XGBoost best params: ' + str(self.grid.best_params_) +
                                   '. Exited the get_best_params_for_xgboost method of the Model_Finder class')
            return self.xgb
        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occurred in get_best_params_for_xgboost '
                                   'method of the Model_Finder class. '
                                   'Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,
                                   'XGBoost Parameter tuning  failed. '
                                   'Exited the get_best_params_for_xgboost method of the Model_Finder class')
            raise Exception()

    def get_best_model(self, train_x, train_y, test_x, test_y):
        """
        Method: get_best_model
        Description: get best model out of Random Forest
                     and xgboost
        return: best_model
        on failure: raise Exception

        """
        self.logger_object.log(self.file_object, 'Getting the best model for you')
        try:
            self.xgboost = self.get_best_params_for_xgboost(train_x, train_y)
            self.predict_xgboost = self.xgboost.predict(test_x)

            if len(test_y.unique()) == 1:
                self.xgboost_score = accuracy_score(test_y, self.predict_xgboost)
                self.logger_object.log(self.file_object, 'Accuracy for XGBoost:' + str(self.xgboost_score))
            else:
                self.xgboost_score = roc_auc_score(test_y, self.predict_xgboost)
                self.logger_object.log(self.file_object, 'Accuracy for XGBoost:' + str(self.xgboost_score))

            self.rf = self.get_best_params_for_RandomForest(train_x, train_y)
            self.predict_rf = self.rf.predict(test_x)

            if len(test_y.unique()) == 1:
                self.rf_score = accuracy_score(test_y, self.predict_rf)
                self.logger_object.log(self.file_object, 'Accuracy for Random Forest:' + str(self.rf_score))
            else:
                self.rf_score = roc_auc_score(test_y, self.predict_rf)
                self.logger_object.log(self.file_object, 'Accuracy for Random Forest:' + str(self.rf_score))

            if (self.rf_score > self.xgboost_score):
                return 'Random Forest', self.rf
            else:
                return 'XGBoost', self.xgboost

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occurred in get_best_model method of the Model_Finder class. '
                                   'Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,
                                   'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')
            raise Exception()
