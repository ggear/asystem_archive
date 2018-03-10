###############################################################################
#
# PRE-PROCESSED LIBRARY - EDITS WILL BE CLOBBERED BY MAVEN BUILD
#
# This file is in the LIBRARY pre-processed state with template available by the
# same package and file name under the modules src/main/template directory.
#
# When editing the template directly (as indicated by the presence of the
# TEMPLATE.PRE-PROCESSOR.RAW_TEMPLATE tag at the top of this file), care should
# be taken to ensure the maven-resources-plugin generate-sources filtering of the
# TEMPLATE.PRE-PROCESSOR tags, which comment and or uncomment blocks of the
# template, leave the file in a consistent state, as a script or library,
# post filtering.
#
# It is desirable that in template form, the file remains both compilable and
# runnable as a script in your IDEs (eg Eclipse, IntelliJ, CDSW etc). To setup
# your environment, it may be necessary to run the pre-processed script once
# (eg to execute AddJar commands with dependency versions completely resolved) but
# from then on the template can be used for direct editing and distribution via
# the source code control system and maven repository for dependencies.
#
# The library can be tested during the standard maven compile and test phases.
#
# Note that pre-processed files will be overwritten as part of the Maven build
# process. Care should be taken to either ignore and not edit these files (eg
# libraries) or check them in and note changes post Maven build (eg scripts)
#
# This file was adapted from a project authored by Michiaki Ariga:
# https://github.com/chezou/solar-power-prediction
#
###############################################################################

# Add plotting libraries# IGNORE SCRIPT BOILERPLATE #import matplotlib.pyplot as plt
# Add plotting libraries# IGNORE SCRIPT BOILERPLATE #import seaborn as sns

# Add working directory to the system path# IGNORE SCRIPT BOILERPLATE #sys.path.insert(0, 'asystem-amodel/src/main/script/python')

import os
import os.path
import sys
import shutil
import tempfile
import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.metrics import r2_score
from sklearn.model_selection import LeaveOneOut
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import ElasticNetCV
from sklearn.linear_model import RidgeCV
from sklearn.linear_model import LassoCV
from sklearn.feature_extraction import DictVectorizer
from pyspark.sql import SparkSession
from script_util import hdfs_make_qualified
from publish_util import publish


def execute(model=None, features=None, labels=False, engineering=False, prediction=False):
    import pandas as pd
    from sklearn.pipeline import Pipeline
    from sklearn.linear_model import ElasticNetCV
    from sklearn.linear_model import RidgeCV
    from sklearn.linear_model import LassoCV
    from sklearn.feature_extraction import DictVectorizer
    FEATURES = [
        'temperature',
        'rain_mm',
        'humidity_mbar',
        'wind_power',
        'day_length_sec',
        'condition'
    ]
    FEATURES_ORIGINAL = [
        'datum__bin__date',
        'energy__production__inverter',
        'temperature__forecast__glen_Dforrest',
        'rain__forecast__glen_Dforrest',
        'humidity__forecast__glen_Dforrest',
        'wind__forecast__glen_Dforrest',
        'conditions__forecast__glen_Dforrest',
        'day_length',
        'day_length_sec'
    ]
    FEATURES_RENAME = {
        'datum__bin__date': 'date',
        'energy__production__inverter': 'energy',
        'temperature__forecast__glen_Dforrest': 'temperature',
        'rain__forecast__glen_Dforrest': 'rain_mm',
        'humidity__forecast__glen_Dforrest': 'humidity_mbar',
        'wind__forecast__glen_Dforrest': 'wind_power',
        'conditions__forecast__glen_Dforrest': 'condition'
    }
    if labels:
        return FEATURES, FEATURES_ORIGINAL, FEATURES_RENAME
    elif engineering:
        features_engineered = features.copy(deep=True)
        features_engineered['sun_rise_at'] = pd.to_datetime(features_engineered['sun__outdoor__rise'], unit='s')
        features_engineered['sun_set_at'] = pd.to_datetime(features_engineered['sun__outdoor__set'], unit='s')
        features_engineered['day_length'] = features_engineered['sun_set_at'] - features_engineered['sun_rise_at']
        features_engineered['day_length_sec'] = features_engineered['sun__outdoor__set'] - features_engineered['sun__outdoor__rise']
        features_engineered_renamed = features_engineered[FEATURES_ORIGINAL]
        features_engineered_renamed = features_engineered_renamed.rename(columns=FEATURES_RENAME)
        return features_engineered_renamed
    elif prediction:
        return model['pipeline'].predict(model['vectorizer'].transform(features[FEATURES].to_dict(orient='record')))


def pipeline():
    remote_data_path = sys.argv[1] if len(sys.argv) > 1 else "s3a://asystem-amodel/asystem/amodel/energyforecast"
    remote_model_path = sys.argv[2] if len(sys.argv) > 2 else "s3a://asystem-amodel/asystem/amodel/energyforecast"
    local_model_path = sys.argv[3] if len(sys.argv) > 3 else tempfile.mkdtemp()

    spark = SparkSession.builder.appName("asystem-amodel-energyforecast").getOrCreate()

    # # Exploratory analysis before building predictive models

    # ## Load CSV
    df = spark.read.csv(
        hdfs_make_qualified(remote_data_path + "/training/text/csv/none/" +
                            "amodel_version=10.000.0019-SNAPSHOT/amodel_model=1003"),
        header=True).toPandas().apply(pd.to_numeric, errors='ignore')
    df2 = execute(features=df, engineering=True)
    dfv = spark.read.csv(
        hdfs_make_qualified(remote_data_path + "/validation/text/csv/none/" +
                            "amodel_version=10.000.0019-SNAPSHOT/amodel_model=1003"),
        header=True).toPandas().apply(pd.to_numeric, errors='ignore')
    dfv2 = execute(features=dfv, engineering=True)

    # Plot the pairplot to discover correlation between power generation and other variables.
    # Plot# IGNORE SCRIPT BOILERPLATE #    sns.set(style="ticks")
    # Plot# IGNORE SCRIPT BOILERPLATE #    sns.pairplot(df2, hue="condition")
    # Plot# IGNORE SCRIPT BOILERPLATE #    plt.show(block=False)

    df2.describe()

    def rmse(actual, predicted):
        from sklearn.metrics import mean_squared_error
        from math import sqrt

        return sqrt(mean_squared_error(actual, predicted))

    def train_model(_regr, train_dict, target):
        estimators = [('vectorizer', DictVectorizer(sparse=False)), ('regr', _regr)]
        _pl = Pipeline(estimators)
        _pl.fit(train_dict, target)

        return _pl

    def prepare_data(raw_df, predictor_columns=execute(labels=True)[0]):
        predictors = raw_df[predictor_columns]
        target = None
        if 'energy' in raw_df.columns:
            target = raw_df.energy

        return predictors, target

    def predict_power_generation(_regr, input_df, predictor_columns=execute(labels=True)[0]):
        _predictors, _target = prepare_data(input_df, predictor_columns)
        input_dict = _predictors.to_dict(orient='record')
        return _regr.predict(input_dict)

    # # Build predictive models with linear regression
    #
    # 1. Split train and test data set
    # 2. Filter predictor columns
    # 3. Create dummy variables for categorical varibales
    # 4. Build model with Linear Regression
    # 5. Evaluate
    #
    # From 1 to 4 should be handle with pipeline model.

    # Split development data and test data
    # Training data is the range of the first data

    dev_data = df2
    model_test_data = dfv2

    energies_train, energies_target = prepare_data(dev_data)

    # ## Encode condition from category to dummy variable
    vectorizer = DictVectorizer(sparse=False)
    energies_cat_train = vectorizer.fit_transform(energies_train.to_dict(orient='record'))

    # ## Build a model with linear regression

    def evaluate_by_loo(energies_train, energies_target, regr=LinearRegression()):
        loo = LeaveOneOut()
        loo.get_n_splits(energies_train)

        train_r2_scores = np.array([])
        test_r2_scores = np.array([])
        train_rmse_scores = np.array([])
        test_rmse_scores = np.array([])
        predicted_powers = np.array([])
        actual_powers = np.array([])

        # Train Linear Regression model
        # It is small data, so
        for train_index, test_index in loo.split(energies_train):
            print("Test index:{}".format(test_index))
            # print("TRAIN:", train_index, "TEST:", test_index)
            # regr = LinearRegression()

            X_train, X_test = energies_train[train_index], energies_train[test_index]
            y_train, y_test = energies_target.iloc[train_index], energies_target.iloc[test_index]
            regr.fit(X_train, y_train)
            # print(X_test, y_test)
            y_train_pred = regr.predict(X_train)
            y_test_pred = regr.predict(X_test)
            # print(y_test.values, y_test_pred)

            train_r2_score = regr.score(X_train, y_train)
            train_r2_scores = np.append(train_r2_scores, train_r2_score)
            test_r2_score = r2_score(y_test.values, y_test_pred)
            test_r2_scores = np.append(test_r2_scores, test_r2_score)

            train_rmse_score = rmse(y_train, y_train_pred)
            train_rmse_scores = np.append(train_rmse_scores, train_rmse_score)
            test_rmse_score = rmse(y_test.values, y_test_pred)
            test_rmse_scores = np.append(test_rmse_scores, test_rmse_score)

            actual_powers = np.append(actual_powers, y_test.values[0])
            predicted_powers = np.append(predicted_powers, y_test_pred[0])
            print("Actual energy generation: {}\tPredicted energy generation: {}".
                  format(y_test.values[0], y_test_pred[0]))

            print("Train R^2 score: {}\tTest R^2 score:{}".format(train_r2_score, test_r2_score))
            print("Train RMSE: {}\tTest RMSE:{}\n".format(train_rmse_score, test_rmse_score))

        # Standard deviation of training data is base line of RMSE
        # print("Standard deviation: {}".format(pd.DataFrame.std(energies_target)))

        print("Train average RMSE: {}\tTest average RMSE:{}".
              format(np.average(train_rmse_scores), np.average(test_rmse_scores)))
        print("Train average R2: {}\tTest average R2:{}".
              format(np.average(train_r2_scores), np.average(test_r2_scores)))

        return actual_powers, predicted_powers

    # Plotting LOO predictions
    # http://scikit-learn.org/stable/auto_examples/plot_cv_predict.html

    actual_powers, predicted_powers = evaluate_by_loo(energies_cat_train, energies_target)

    def plot_predict_actual(actual, predicted):
        fig, ax = plt.subplots()
        ax.scatter(actual, predicted, edgecolors=(0, 0, 0))
        ax.plot([actual.min(), actual.max()], [actual.min(), actual.max()], 'k--', lw=4)
        ax.set_xlabel('Measured')
        ax.set_ylabel('Predicted')
        plt.show(block=False)

    # Plot# IGNORE SCRIPT BOILERPLATE #    plot_predict_actual(actual_powers, predicted_powers)

    # Create model with dev data

    def train_and_predict(regr, cat_train, target, cat_test, test_target):
        regr.fit(cat_train, target)

        pred_train = regr.predict(cat_train)
        pred = regr.predict(cat_test)

        dev_rmse = rmse(target.values, pred_train)
        test_rmse = rmse(test_target.values, pred)
        print("Dev RMSE: {}\tDev R2 score: {}".
              format(dev_rmse, r2_score(target.values, pred_train)))
        print("Test RMSE: {}\tTest R2 score: {}".
              format(test_rmse, r2_score(test_target.values, pred)))
        print('Coefficients: \n', regr.coef_)
        # print(test.columns)
        print('Intercepts: \n', regr.intercept_)

        # plot_predict_actual(test_target, pred)

        return regr, dev_rmse, test_rmse

    energies_test, energies_test_target = prepare_data(model_test_data)
    energies_cat_test = vectorizer.transform(energies_test.to_dict(orient='record'))

    min_rmse = 10000000000
    best_model_test_rmse = 10000000000
    best_model = None

    for _regr in [LinearRegression(), ElasticNetCV(cv=4), RidgeCV(cv=4), LassoCV(cv=4)]:
        print(type(_regr).__name__)
        _model, _rmse, _test_rmse = train_and_predict(
            _regr, energies_cat_train, energies_target, energies_cat_test, energies_test_target)
        if min_rmse > _rmse:
            best_model = _model
            min_rmse = _rmse
            best_model_test_rmse = _test_rmse

    print("Best model: {}\tMin Dev RMSE: {}\tTest RMSE: {}"
          .format(type(best_model).__name__, min_rmse, best_model_test_rmse))

    model_file = '/model/pickle/joblib/none/' \
                 'amodel_version=10.000.0019-SNAPSHOT/amodel_model=1003/model.pkl'
    local_model_file = local_model_path + model_file
    remote_model_file = remote_model_path + model_file
    if os.path.exists(os.path.dirname(local_model_file)): shutil.rmtree(os.path.dirname(local_model_file))
    os.makedirs(os.path.dirname(local_model_file))

    import dill
    from StringIO import StringIO
    from sklearn.externals import joblib

    pickled_execute = StringIO()
    dill.dump(execute, pickled_execute)
    pickled_execute.flush()

    joblib.dump({'vectorizer': vectorizer, 'pipeline': best_model, 'execute': pickled_execute}, local_model_file, compress=True)

    # Example of serialized model usage
    model = joblib.load(local_model_file)
    model['execute'] = dill.load(StringIO(model['execute'].getvalue()))
    print(dfv)
    print(dfv.dtypes)
    energy_production_actual = dfv['energy__production__inverter'].iloc[0]
    energy_production_prediction = round(
        model['execute'](model=model, features=model['execute'](features=dfv, engineering=True), prediction=True)[0], 1)
    energy_production_accuracy = int(round(energy_production_prediction / energy_production_actual * 100))
    print("Model prediction [{}] versus actual [{}] at accuracy [{}%]"
          .format(energy_production_prediction, energy_production_actual, energy_production_accuracy))

    print("Model copy: {} -> {}".format(local_model_file, remote_model_file))
    publish(local_model_file, remote_model_file)
    shutil.rmtree(local_model_path)

# Run pipeline# IGNORE SCRIPT BOILERPLATE #pipeline()

# Main function
if __name__ == "__main__":
# Run pipeline
    pipeline()
