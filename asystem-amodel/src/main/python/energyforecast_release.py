#!/usr/local/bin/python -u
"""
Provide a release gateway script
Usage: %s [options]
Options:
-h --help                                Show help
--connection_jar=<path_to_jar>           Path to the connection jar file
--transaction_id=<alpha-numeric-string>  Transaction ID
--job_group=<alpha-numeric-string>       Job group
--job_name=<alpha-numeric-string>        Job name
--job_version=<alpha-numeric-string>     Job version
--job_properties=<path_to_properties>    Path to the job properties file
--job_validation=<path_to_csv>           Path to the Job validation CSV
"""

ENERGYFORECAST_ACCEPTABLE_RMSE = 5000

import getopt
import inspect
import logging
import os
import shutil
import textwrap
from StringIO import StringIO

import dill
import pandas as pd
import sys
from sklearn.externals import joblib

import repo_util
import script_util

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/../manager/python')

import metadata
from metadata import METADATA_NAMESPACE

S3_URL = "s3a://asystem-amodel-staging/asystem/amodel/"


def do_call(connection_jar, transaction_id, job_group, job_name,
            job_version, job_properties, job_validation):
    job_properties_dict = dict(line.strip().split("=") for line in open(job_properties)
                               if not line.startswith("#") and not line.startswith("\n"))
    model_interday_s3 = S3_URL + \
                        "energyforecastinterday/model/pickle/joblib/none/amodel_version=" + \
                        job_version + "/amodel_model=" + job_properties_dict[
                            "MODEL_ENERGYFORECAST_INTERDAY_BUILD_VERSION"]
    if model_interday_s3 != repo_util.nearest(script_util.qualify(model_interday_s3), "model.pkl"):
        print("Required model [{}] not found".format(model_interday_s3))
        return 1
    model_interday_s3 = model_interday_s3 + "/model.pkl"
    model_interday_file = repo_util.get(model_interday_s3)
    model_intraday_s3 = S3_URL + \
                        "energyforecastintraday/model/pickle/joblib/none/amodel_version=" + \
                        job_version + "/amodel_model=" + job_properties_dict[
                            "MODEL_ENERGYFORECAST_INTRADAY_BUILD_VERSION"]
    if model_intraday_s3 != repo_util.nearest(script_util.qualify(model_intraday_s3), "model.pkl"):
        print("Required model [{}] not found".format(model_intraday_s3))
        return 1
    model_intraday_s3 = model_intraday_s3 + "/model.pkl"
    model_intraday_file = repo_util.get(model_intraday_s3)
    model_interday = joblib.load(model_interday_file)
    model_interday['execute'] = dill.load(StringIO(model_interday['execute'].getvalue()))
    shutil.rmtree(os.path.dirname(model_interday_file))
    if model_interday_file is None or model_intraday_file is None:
        print("Could not download models to local file system".format(model_intraday_s3))
        return 1
    print("\nEnergy forecast models downloaded:\n{}\n-> {}\n{}\n-> {}\n"
          .format(model_intraday_s3, model_interday_file, model_interday_s3, model_intraday_file))
    model_intraday = joblib.load(model_intraday_file)
    model_intraday['execute'] = dill.load(StringIO(model_intraday['execute'].getvalue()))
    shutil.rmtree(os.path.dirname(model_intraday_file))
    validation = pd.read_csv(job_validation).apply(pd.to_numeric, errors='ignore')
    model_intraday_prediction = model_intraday['execute'] \
        (model=model_intraday, features=validation, prediction=True)
    model_interday_prediction = model_interday['execute'] \
        (model=model_interday, features=model_interday['execute']
        (features=validation, engineering=True), prediction=True)
    validation["energy__production_Dforecast__inverter"] = \
        model_intraday_prediction.apply(lambda model_intraday_prediction:
                                        model_intraday_prediction *
                                        model_interday_prediction).iloc[:, 0]
    validation["energy__production_Dforecast_Dactual__inverter"] = \
        (validation["energy__production_Dforecast__inverter"] /
         validation["energy__production__inverter"]).fillna(1) * 100
    validation_exit = 0
    validation_rows = validation.shape[0]
    validation_mean_accuracy = validation["energy__production_Dforecast_Dactual__inverter"].mean()
    validation_rmse = ((validation["energy__production_Dforecast__inverter"] -
                        validation["energy__production__inverter"]) ** 2).mean() ** 0.5
    print("Energy validation summary:\n{}\n".format(validation[[
        "energy__production__inverter",
        "energy__production_Dforecast__inverter",
        "energy__production_Dforecast_Dactual__inverter"
    ]].copy().rename(columns={
        "energy__production__inverter": "Actual",
        "energy__production_Dforecast__inverter": "Prediction",
        "energy__production_Dforecast_Dactual__inverter": "Accuracy"
    }).describe()))
    print("Energy validation RMSE: {}\n".format(validation_rmse))
    metadata_bodies = dict([(metadata_body['originalName']
                             if 'originalName' in metadata_body and
                                metadata_body['originalName'] is not None else '', metadata_body)
                            for metadata_body in metadata.put(connection_jar, transaction_id, {
            "VALIDATION_INSTANCES": str(validation_rows),
            "VALIDATION_MEAN_ACCURACY": str(int(validation_mean_accuracy)),
            "VALIDATION_RMS_ERROR": str(int(validation_rmse))
        }, {"Exit": 0 if validation_rmse > ENERGYFORECAST_ACCEPTABLE_RMSE else 1}, ["production"])])
    if len(metadata_bodies) == 0:
        print("Energy forecast metadata not found")
        return 1
    print("Found job metadata:")
    for name, metadata_body in metadata_bodies.iteritems():
        print("\t{}: {}".format(name, metadata_body['navigatorUrl']))
        validation_exit = validation_exit + (
            metadata_body['customProperties'][METADATA_NAMESPACE]["Exit"]
            if 'customProperties' in metadata_body and metadata_body['customProperties'] is not None and
               METADATA_NAMESPACE in metadata_body['customProperties'] and
               metadata_body['customProperties'][METADATA_NAMESPACE] is not None
               and "Exit" in metadata_body['customProperties'][METADATA_NAMESPACE] else 1)
    if validation_rmse > ENERGYFORECAST_ACCEPTABLE_RMSE:
        print("Energy validation RMSE [{}] greater than releasable (ie historic) threshold [{}]"
              .format(validation_rmse, ENERGYFORECAST_ACCEPTABLE_RMSE))
        return 1
    if validation_exit != 0:
        print("Energy validation processes have returned non-zero exit [{}]"
              .format(validation_exit))
        return 1
    return 0


def usage():
    doc = inspect.getmodule(usage).__doc__
    print >> sys.stderr, textwrap.dedent(doc % (sys.argv[0],))


def setup_logging(level):
    logging.basicConfig()
    logging.getLogger().setLevel(level)
    logging.getLogger("requests").setLevel(logging.WARNING)


def main():
    setup_logging(logging.INFO)
    connection_jar = None
    transaction_id = None
    job_group = None
    job_name = None
    job_version = None
    job_properties = None
    job_validation = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h', ['help'
            , 'connection_jar=', 'transaction_id=', 'job_group='
            , 'job_name=', 'job_version=', 'job_properties='
            , 'job_validation='])
    except getopt.GetoptError, err:
        print >> sys.stderr, err
        usage()
        return -1
    for option, value in opts:
        if option in ('-h', '--help'):
            usage()
            return -1
        elif option in '--connection_jar':
            connection_jar = value
        elif option in '--transaction_id':
            transaction_id = value
        elif option in '--job_group':
            job_group = value
        elif option in '--job_name':
            job_name = value
        elif option in '--job_version':
            job_version = value
        elif option in '--job_properties':
            job_properties = value
        elif option in '--job_validation':
            job_validation = value
        else:
            print >> sys.stderr, 'Unknown option or flag: ' + option
            usage()
            return -1
    if not all([connection_jar, transaction_id, job_group, job_name
                   , job_version, job_properties, job_validation]):
        print >> sys.stderr, \
            "Required parameters [connection_jar, transaction_id, " \
            "job_group, job_name, job_version, job_properties, job_validation]" \
            " not passed on command line"
        usage()
        return -1
    return do_call(connection_jar, transaction_id, job_group
                   , job_name, job_version, job_properties, job_validation)


if __name__ == '__main__':
    sys.exit(main())
