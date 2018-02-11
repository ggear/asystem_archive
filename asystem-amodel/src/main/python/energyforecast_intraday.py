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
###############################################################################

# Add working directory to the system path# IGNORE SCRIPT BOILERPLATE #sys.path.insert(0, 'asystem-amodel/src/main/script/python')

# Add plotting libraries# IGNORE SCRIPT BOILERPLATE #import matplotlib.pyplot as plt

import numpy as np
import os.path
import pandas as pd
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from script_util import hdfs_make_qualified

DAYS_VETTED = '2018/02/08'

DAYS_BLACK_LIST = set([
    '2017/10/08',
    '2017/10/09',
    '2017/10/12',
    '2017/10/13',
    '2017/10/23',
    '2017/10/24',
    '2017/10/25',
    '2017/10/28',
    '2017/10/29',
    '2017/10/30',
    '2017/11/07',
    '2017/11/10',
    '2017/11/16',
    '2017/11/20',
    '2017/11/24',
    '2017/11/28',
    '2017/11/29',
    '2017/12/02',
    '2017/12/03',
    '2017/12/06',
    '2017/12/09',
    '2017/12/10',
    '2017/12/11',
    '2017/12/15',
    '2017/12/16',
    '2017/12/17',
    '2017/12/20',
    '2017/12/24',
    '2017/12/26',
    '2017/12/31',
    '2018/01/23',
    '2018/01/31',
])

DAYS_PLOT = False


# noinspection PyRedeclaration
# Enable plotting# IGNORE SCRIPT BOILERPLATE #DAYS_PLOT = True


# noinspection PyStatementEffect
def pipeline():
    remote_data_path = sys.argv[1] if len(sys.argv) > 1 else "s3a://asystem-astore"

    print("Pipeline started")
    time_start = int(round(time.time()))
    spark = SparkSession.builder.appName("asystem-amodel-energyforecastintraday").getOrCreate()

    # TODO: test to see if version/data means we should load or build

    datasets = []
    timezone = 'Australia/Perth'
    for path in [os.path.join(remote_data_path, str(i), "asystem/astore/processed/canonical/parquet/dict/snappy") for i in range(10)]:
        try:
            path_uri = hdfs_make_qualified(path)
            datasets.append(spark.read.parquet(path_uri))
            print("Loaded path [{}]".format(path_uri))
        except AnalysisException:
            continue
    ds = reduce(lambda x, y: x.union(y), datasets)
    ds.createOrReplaceTempView('dataset')
    dfEnergy = spark.sql("""
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
    dfSunRise = spark.sql("""
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
    dfSunSet = spark.sql("""
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

    df = dfEnergy.set_index(pd.to_datetime(dfEnergy['bin_timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert(timezone))
    df['bin_date'] = df.index.date
    df.set_index('bin_date', inplace=True)
    dfEnergyDay = df.groupby(df.index)['bin_energy'].max().to_frame().rename(columns={'bin_energy': 'bin_energy_day'})
    df = df.merge(dfEnergyDay, how='inner', left_index=True, right_index=True)
    dfSunRise.set_index(pd.to_datetime(dfSunRise['bin_timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert(timezone), inplace=True)
    dfSunRise['bin_date'] = dfSunRise.index.date
    dfSunRise.set_index('bin_date', inplace=True)
    df = df.merge(dfSunRise.groupby(dfSunRise.index)['bin_sunrise'].max().to_frame(), how='inner', left_index=True, right_index=True)
    dfSunSet.set_index(pd.to_datetime(dfSunSet['bin_timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert(timezone), inplace=True)
    dfSunSet['bin_date'] = dfSunSet.index.date
    dfSunSet.set_index('bin_date', inplace=True)
    df = df.merge(dfSunSet.groupby(dfSunSet.index)['bin_sunset'].max().to_frame(), how='inner', left_index=True, right_index=True)
    df.set_index(pd.to_datetime(df['bin_timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert(timezone), inplace=True)
    df.sort_index(inplace=True)

    # TODO: write out data as CSV, generalise from energyforecast

    dfvs = {'VETTED': {}, 'PURGED': {}, 'TOVETT': {}}
    for dfs in df.groupby(df.index.date):
        day = dfs[0].strftime('%Y/%m/%d')
        dfvs[('PURGED' if day in DAYS_BLACK_LIST else ('TOVETT' if day >= DAYS_VETTED else 'VETTED'))][day] = dfs[1]
    for vetting in dfvs:
        for day, dfv in dfvs[vetting].iteritems():
            dfv.set_index(pd.to_datetime(dfv['bin_timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert(timezone), inplace=True)
            if DAYS_PLOT: dfv.plot(title="Energy ({}) - {}".format(day, vetting), y=['bin_energy', 'bin_energy_day'])
    for vetting in dfvs: print("{} [{}] days".format(vetting, len(dfvs[vetting])))

    dfnss = []
    bins = 1000
    for day, dfv in dfvs['VETTED'].iteritems():
        dfv['normalised'] = dfv['bin_energy'] / dfv['bin_energy_day']
        dfv['standardised'] = bins * (dfv['bin_timestamp'] - dfv['bin_sunrise']) / (dfv['bin_sunset'] - dfv['bin_sunrise'])
        dfv['standardised'] = dfv['standardised'].clip(0, bins).astype(int)
        dfns = dfv.drop(['bin_timestamp', 'bin_energy', 'bin_energy_day', 'bin_sunrise', 'bin_sunset'], axis=1).drop_duplicates()
        dfns.set_index('standardised', inplace=True)
        dfns.sort_index(inplace=True)
        dfns = dfns[~dfns.index.duplicated(keep='first')]
        dfns = dfns.reindex(np.arange(0, bins + 1)).ffill()
        dfns.loc[0:10] = 0
        dfns.loc[990:1000] = 1
        dfnss.append(dfns)
        if DAYS_PLOT: dfns.plot(title="Energy ({}) - VETTED".format(day))

    dfnsa = pd.concat(dfnss, axis=1, ignore_index=True)
    if DAYS_PLOT: dfnsa.plot(title="Energy Normalised/Standardised (All) - VETTED", legend=False)
    dfnsa = pd.concat(dfnss)
    dfnsa = dfnsa.groupby(dfnsa.index).mean()
    print("Energy Mean Distribution:\n{}".format(dfnsa))
    if DAYS_PLOT: dfnsa.plot(title="Energy Normalised/Standardised (Mean) - VETTED", legend=False)

    dfnsa.loc[500]['normalised']

    # TODO: write out pipeline, generalise from energyforecast

    print("Pipeline finished in [{}] s".format(int(round(time.time())) - time_start))

# Run pipeline# IGNORE SCRIPT BOILERPLATE #pipeline()

# Main function
if __name__ == "__main__":
# Run pipeline
    pipeline()
