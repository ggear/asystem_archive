###############################################################################
#
# PRE-PROCESSED SCRIPT - EDITS WILL BE CLOBBERED BY MAVEN BUILD
#
# This file is in the SCRIPT pre-processed state with template available by the
# same package and file name under the modules src/main/template directory.
#
# If editing as a SCRIPT or LIBRARY, all changes must be merged to the TEMPLATE,
# else they will be locally clobbered by the maven build. Am automated merge
# process (revealing template diffs) is provided by the bootstrap script.
#
# When editing as a TEMPLATE directly (as indicated by the presence of the
# TEMPLATE.PRE-PROCESSOR.RAW_TEMPLATE tag at the top of this file), care should
# be taken to ensure the maven-resources-plugin generate-sources filtering of the
# TEMPLATE.PRE-PROCESSOR tags, which comment and or uncomment blocks of the
# template, leave the file in a consistent state, as a script or library,
# post filtering.
#
# The SCRIPT can be tested from within CDSW, the LIBRARY as part of  the maven
# test phase.
#
###############################################################################

import dill
import datetime
import numpy as np
import os.path
import pandas as pd
import shutil
import tempfile
import time
import sys

# Add working directory to the system path
sys.path.insert(0, 'asystem-amodel/src/main/script/python')

# Add plotting libraries
import matplotlib.pyplot as plt

from StringIO import StringIO
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from sklearn.externals import joblib

from repo_util import publish
from script_util import qualify

DAYS_BLACK_LIST = {
    '2017/10/08', '2017/10/09', '2017/10/12', '2017/10/13',
    '2017/10/23', '2017/10/24', '2017/10/25', '2017/10/28',
    '2017/10/29', '2017/10/30', '2017/11/01', '2017/11/07',
    '2017/11/10', '2017/11/16', '2017/11/20', '2017/11/24',
    '2017/11/28', '2017/11/29', '2017/12/01', '2017/12/02',
    '2017/12/03', '2017/12/06', '2017/12/09', '2017/12/10',
    '2017/12/11', '2017/12/15', '2017/12/16', '2017/12/17',
    '2017/12/20', '2017/12/24', '2017/12/26', '2017/12/31',
    '2018/01/23', '2018/01/31', '2018/02/14', '2018/02/20',
    '2018/02/21', '2018/02/22', '2018/02/23', '2018/03/04',
    '2018/03/28', '2018/04/05', '2018/04/05', '2018/04/06',
    '2018/04/07', '2018/04/08', '2018/04/10', '2018/04/22',
    '2018/05/17', '2018/05/18', '2018/05/21', '2018/05/25',
    '2018/05/30', '2018/06/14', '2018/06/15'
}

DAYS_PLOT = False
DAYS_PLOT_DEBUG = False

# noinspection PyRedeclaration
# Enable plotting
DAYS_PLOT = True

pd.set_option('display.height', 1000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def execute(model=None, features=None, labels=False,
            engineering=False, prediction=False):
    if prediction:
        return model['pipeline'].loc[features[
            'energy__production_Dforecast_Ddaylight__inverter']] \
            .reset_index().drop('standardised', axis=1) \
            .rename(columns={
            'normalised': 'energy__production_Dforecast_Dintraday_Dscale__inverter'})


# noinspection PyStatementEffect
def pipeline():
    remote_data_path = sys.argv[1] if len(sys.argv) > 1 else \
        "s3a://asystem-astore-staging"
    remote_model_path = sys.argv[2] if len(sys.argv) > 2 else \
        "s3a://asystem-amodel-staging/asystem/amodel/energyforecastintraday"
    local_model_path = sys.argv[3] if len(sys.argv) > 3 else \
        tempfile.mkdtemp()

    time_start = int(round(time.time()))
    spark = SparkSession.builder \
        .appName("asystem-amodel-energyforecastintraday").getOrCreate()

    datasets = []
    timezone = 'Australia/Perth'
    print("Loading data:")
    for path in [os.path.join(remote_data_path, str(i),
                              "asystem/astore/processed/canonical/parquet/dict/snappy"
                              ) for i in range(10)]:
        try:
            path_uri = qualify(path)
            datasets.append(spark.read.parquet(path_uri))
            print("Loaded path [{}]".format(path_uri))
        except AnalysisException:
            continue
    ds = reduce(lambda x, y: x.union(y), datasets)
    ds.createOrReplaceTempView('dataset')
    df_energy = spark.sql("""
        SELECT
          bin_timestamp,
          data_value / data_scale AS bin_energy
        FROM dataset
        WHERE
          astore_metric='energy' AND
          data_metric='energy__production__inverter' AND 
          data_type='integral' AND
          bin_width=1 AND
          bin_unit='day'
        ORDER BY bin_timestamp ASC
    """).toPandas()
    df_sun_rise = spark.sql("""
        SELECT
          bin_timestamp,
          data_value / data_scale AS bin_sunrise
        FROM dataset
        WHERE          
          astore_metric='sun' AND
          data_metric='sun__outdoor__rise' AND
          data_type='epoch' AND
          bin_width=1 AND
          bin_unit='day'
        ORDER BY bin_timestamp ASC
    """).toPandas()
    df_sun_set = spark.sql("""
        SELECT
          bin_timestamp,
          data_value / data_scale AS bin_sunset
        FROM dataset
        WHERE          
          astore_metric='sun' AND
          data_metric='sun__outdoor__set' AND
          data_type='epoch' AND
          bin_width=1 AND
          bin_unit='day'
        ORDER BY bin_timestamp ASC
    """).toPandas()
    print("Data loaded and filtered\n")

    df = df_energy.set_index(pd.to_datetime(df_energy['bin_timestamp'], unit='s')
                            .dt.tz_localize('UTC').dt.tz_convert(timezone))
    df['bin_date'] = df.index.date
    df.set_index('bin_date', inplace=True)
    df_energy_day = df.groupby(df.index)['bin_energy'].max().to_frame() \
        .rename(columns={'bin_energy': 'bin_energy_day'})
    df = df.merge(df_energy_day, how='inner', left_index=True, right_index=True)
    df_sun_rise.set_index(pd.to_datetime(df_sun_rise['bin_timestamp'], unit='s')
                        .dt.tz_localize('UTC').dt.tz_convert(timezone), inplace=True)
    df_sun_rise['bin_date'] = df_sun_rise.index.date
    df_sun_rise.set_index('bin_date', inplace=True)
    df = df.merge(df_sun_rise.groupby(df_sun_rise.index)['bin_sunrise'].max()
                  .to_frame(), how='inner', left_index=True, right_index=True)
    df_sun_set.set_index(
        pd.to_datetime(df_sun_set['bin_timestamp'], unit='s')
            .dt.tz_localize('UTC').dt.tz_convert(timezone), inplace=True)
    df_sun_set['bin_date'] = df_sun_set.index.date
    df_sun_set.set_index('bin_date', inplace=True)
    df = df.merge(df_sun_set.groupby(df_sun_set.index)['bin_sunset'].max()
                  .to_frame(), how='inner', left_index=True, right_index=True)
    df.set_index(pd.to_datetime(df['bin_timestamp'], unit='s')
                 .dt.tz_localize('UTC').dt.tz_convert(timezone), inplace=True)
    df.sort_index(inplace=True)

    print("Training data:\n{}\n\n".format(df.describe()))

    dfvs = {'VETTED': {}, 'PURGED': {}, 'TOVETT': {}}
    for dfs in df.groupby(df.index.date):
        day = dfs[0].strftime('%Y/%m/%d')
        dfvs[('PURGED' if day in DAYS_BLACK_LIST else
              ('TOVETT' if day >= datetime.datetime.now().strftime("%Y/%m/%d")
                   else 'VETTED'))][day] = dfs[1]

    for vetting in dfvs:
        for day, dfv in sorted(dfvs[vetting].iteritems()):
            dfv.set_index(
                pd.to_datetime(dfv['bin_timestamp'], unit='s')
                    .dt.tz_localize('UTC').dt.tz_convert(timezone), inplace=True)
            if DAYS_PLOT and DAYS_PLOT_DEBUG:
                dfv.plot(title="Energy ({}) - {}"
                         .format(day, vetting), y=['bin_energy', 'bin_energy_day'])

    for vetting in dfvs: print("{} [{}] days".format(vetting, len(dfvs[vetting])))

    dfnss = []
    bins = 1000
    for day, dfv in sorted(dfvs['VETTED'].iteritems()):
        dfv['normalised'] = dfv['bin_energy'] / dfv['bin_energy_day']
        dfv['standardised'] = bins * (
                dfv['bin_timestamp'] - dfv['bin_sunrise']) / \
                              (dfv['bin_sunset'] - dfv['bin_sunrise'])
        dfv['standardised'] = dfv['standardised'].clip(0, bins).astype(int)
        dfns = dfv.drop(['bin_timestamp', 'bin_energy',
                         'bin_energy_day', 'bin_sunrise', 'bin_sunset'],
                        axis=1).drop_duplicates()
        dfns.set_index('standardised', inplace=True)
        dfns.sort_index(inplace=True)
        dfns = dfns[~dfns.index.duplicated(keep='first')]
        dfns = dfns.reindex(np.arange(0, bins + 1)).ffill()
        dfns.loc[0:10] = 0
        dfns.loc[990:1000] = 1
        dfnss.append(dfns)
        if DAYS_PLOT and DAYS_PLOT_DEBUG:
            dfns.plot(title="Energy ({}) - VETTED".format(day))

    dfnsa = pd.concat(dfnss, axis=1, ignore_index=True)
    if DAYS_PLOT:
        dfnsa.plot(title="Energy Normalised/Standardised (All) - VETTED", legend=False)
    dfnsa = pd.concat(dfnss)
    dfnsa = dfnsa.groupby(dfnsa.index).mean()
    if DAYS_PLOT:
        dfnsa.plot(title="Energy Normalised/Standardised (Mean) - VETTED", legend=False)

    model_file = '/model/pickle/joblib/none/' \
                 'amodel_version=10.000.0113/amodel_model=1003/model.pkl'
    local_model_file = local_model_path + model_file
    remote_model_file = remote_model_path + model_file
    if os.path.exists(os.path.dirname(local_model_file)):
        shutil.rmtree(os.path.dirname(local_model_file))
    os.makedirs(os.path.dirname(local_model_file))
    pickled_execute = StringIO()
    dill.dump(execute, pickled_execute)
    pickled_execute.flush()
    joblib.dump({'pipeline': dfnsa, 'execute': pickled_execute},
                local_model_file, compress=True)

    model = joblib.load(local_model_file)
    dfi = pd.DataFrame([
        {"energy__production_Dforecast_Ddaylight__inverter": 0},
        {"energy__production_Dforecast_Ddaylight__inverter": 250},
        {"energy__production_Dforecast_Ddaylight__inverter": 500},
        {"energy__production_Dforecast_Ddaylight__inverter": 750},
        {"energy__production_Dforecast_Ddaylight__inverter": 1000}
    ]).apply(pd.to_numeric, errors='ignore')
    dfo = dill.load(StringIO(model['execute'].getvalue())) \
        (model=model, features=dfi, prediction=True)
    print("\nEnergy Mean Input:\n{}\n\nEnergy Mean Output:\n{}\n".format(dfi, dfo))
    publish(local_model_file, remote_model_file)
    shutil.rmtree(local_model_path)

    print("Pipeline finished in [{}] s".format(int(round(time.time())) - time_start))

# Run pipeline
pipeline()

# Main function# IGNORE LIBRARY BOILERPLATE #if __name__ == "__main__":
# Run pipeline# IGNORE LIBRARY BOILERPLATE #    pipeline()
