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

import os.path
import tempfile
import pandas as pd
import time
import sys

# Add working directory to the system path
sys.path.insert(0, 'asystem-amodel/src/main/script/python')

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from script_util import hdfs_make_qualified


def pipeline():
    # Set remote path based on passed parameters or default
    remote_data_path = sys.argv[1] if len(sys.argv) > 1 else "s3a://asystem-astore"

    # Start the pipeline session
    print("Pipeline started")
    time_start = int(round(time.time()))
    spark = SparkSession.builder.appName("asystem-amodel-dataset").getOrCreate()

    # Marshall the Spark dataset
    datasets = []
    for path in [os.path.join(remote_data_path, str(i),
                              "asystem/astore/processed/canonical/parquet/dict/snappy"
                              ) for i in range(10)]:
        try:
            path_uri = hdfs_make_qualified(path)
            datasets.append(spark.read.parquet(path_uri))
            print("Loaded path [{}]".format(path_uri))
        except AnalysisException:
            continue
    dataset = reduce(lambda x, y: x.union(y), datasets)
    dataset.createOrReplaceTempView('dataset')

    # Filter/aggregate to a Spark dataframe, converting to pandas
    dataframe = spark.sql("""
        SELECT
          bin_timestamp AS timestamp,
          data_metric AS metric,
          data_temporal AS temporal,
          data_value / data_scale AS temperature
        FROM dataset
        WHERE
          astore_metric='temperature' AND
          data_temporal='current' AND
          data_type='point' AND
          data_metric NOT LIKE '%forecast%'
        ORDER BY timestamp
    """).toPandas()

    # Transform the dataframe using pandas
    dataframe = dataframe.pivot_table(
        values='temperature', index='timestamp', columns='metric')
    dataframe = dataframe.set_index(pd.to_datetime(dataframe.index, unit='s')
                                    .tz_localize('UTC').tz_convert('Australia/Perth'))
    dataframe = dataframe.fillna(method='bfill')
    dataframe = dataframe.fillna(method='ffill')
    dataframe = dataframe.resample('300S').mean()
    dataframe = dataframe.fillna(method='bfill')
    dataframe = dataframe.fillna(method='ffill')

    # Write the data out to CSV
    output = tempfile.NamedTemporaryFile(
        prefix='asystem-temperature', suffix='.csv', delete=False).name
    print("Writing output to [{}]".format(output))
    dataframe.to_csv(output)

    print("Pipeline finished in [{}] s".format(int(round(time.time())) - time_start))

# Run pipeline
pipeline()

# Main function# IGNORE LIBRARY BOILERPLATE #if __name__ == "__main__":
# Run pipeline# IGNORE LIBRARY BOILERPLATE #    pipeline()
