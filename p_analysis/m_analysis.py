import pandas as pd
from scipy import stats
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import OneHotEncoder
from lightgbm import LGBMRegressor
from sklearn.model_selection import RandomizedSearchCV

CAT_FEATS = ['SIGNIFICANT', 'SERIOUS']

NUM_FEATS = ['FATAL', 'INJURE', 'UNINTENTIONAL_RELEASE_BBLS', 'ACCIDENT_PSIG', 'MOP_PSIG', 'RECOVERED_BBLS',
             'PIPE_DIAMETER', 'PIPE_SMYS', 'EX_HYDROTEST_PRESSURE', 'MANUFACTURED_YEAR', 'NORMAL_PSIG',
             'ACCOMPANYING_LIQUID']

FEATS = NUM_FEATS + CAT_FEATS

TARGET = 'TOTAL_COST_CURRENT'

model_type = LGBMRegressor()

lgbm_param_grid = {'regressor__num_leaves': (20, 100),
                   'regressor__n_estimators': (20, 500),
                   'regressor__learning_rate': (0.05, 0.3),
                   'regressor__feature_fraction': (0.1, 0.9),
                   'regressor__bagging_fraction': (0.8, 1),
                   'regressor__max_depth': (15, 25),
                   'regressor__min_split_gain': (0.001, 0.1),
                   'regressor__min_child_weight': (10, 50),
                   'regressor__preprocessor__num__imputer__strategy': ['mean', 'median']}


def data_acquisition():

    return pd.read_csv('./data/processed/pipelines_incident_for_modelling.csv', low_memory=False)


def remove_outliers(df, column):

    """ Removes outliers for a designated column in a dataframe and filter """

    return df[(np.abs(stats.zscore(df[column])) < 3)]


# let's define a preprocessing transformer for numerical columns...

numeric_transformer = Pipeline(steps=[('imputer', SimpleImputer(strategy='median')),
                                      ('scaler', StandardScaler())])

# let's define a preprocessing transformer for categorical columns...

categorical_transformer = Pipeline(steps=[('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
                                          ('onehot', OneHotEncoder(handle_unknown='ignore'))])

# let's join these transformers using a `ColumnTransformer`:

preprocessor = ColumnTransformer(transformers=[('num', numeric_transformer, NUM_FEATS),
                                               ('cat', categorical_transformer, CAT_FEATS)])


def create_ml_model(model_type):

    """ This function returns a machine learning model with complete feature engineering"""

    return Pipeline(steps=[('preprocessor', preprocessor),
                           ('regressor', model_type)])


def optimize_model(ml_model, param_grid):

    """ Optimize hyperparameters of the model by using RandomizedSearchCV and drops the scoring using
        Cross Validation"""

    return RandomizedSearchCV(ml_model,
                                 param_grid,
                                 cv=10,
                                 verbose=0,
                                 scoring='neg_root_mean_squared_error',
                                 n_jobs=-1,
                                 n_iter=10)


def fit_model(grid_search, df, FEATS, TARGET):

    """ Train the machine learning model"""

    return grid_search.fit(df[FEATS], df[TARGET])


def cost_prediction(trained_model):

    incident_to_predict = pd.read_csv('./data/results/incidents_predict.csv')

    y_pred = trained_model.predict(incident_to_predict[FEATS])

    cost_prediction = pd.DataFrame({'id': incident_to_predict['id'], 'incident_cost': y_pred})

    return cost_prediction.to_csv('./data/results/cost_prediction.csv', index=False)


def machine_learning_modelling():

    df_for_modelling = data_acquisition()

    incidents = remove_outliers(df_for_modelling, TARGET)

    lgbm_model = create_ml_model(model_type)

    trained_model = optimize_model(lgbm_model, lgbm_param_grid)

    fit_model(trained_model, incidents, FEATS, TARGET)

    return cost_prediction(trained_model)