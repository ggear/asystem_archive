###############################################################################
#
# PRE-PROCESSED SCRIPT - EDITS WILL BE CLOBBERED BY MAVEN BUILD
#
# This file is in the SCRIPT pre-processed state with template available by the
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

# Add working directory to the system path
sys.path.insert(0, 'asystem-amodel/src/main/script/python')

import os.path
import sys
import time
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
    for path in [os.path.join(remote_data_path, str(i), "asystem/astore/processed/canonical/parquet/dict/snappy") for i in range(10)]:
        try:
            path_uri = hdfs_make_qualified(path)
            datasets.append(spark.read.parquet(path_uri))
            print("Loaded path [{}]".format(path_uri))
        except AnalysisException:
            continue
    dataset = reduce(lambda x, y: x.union(y), datasets)
    dataset.createOrReplaceTempView('dataset')

    # Filter/aggregate to a Spark dataframe
    dataframe = spark.sql("""
        SELECT data_metric, count(data_metric) AS data_metric_count
        FROM dataset
        GROUP BY data_metric
        ORDER BY data_metric_count DESC
    """).toPandas()
    print("Datums summary:\n" + str(dataframe))

    print("Pipeline finished in [{}] s".format(int(round(time.time())) - time_start))

# Run pipeline
pipeline()

# Main function# IGNORE LIBRARY BOILERPLATE #if __name__ == "__main__":
# Run pipeline# IGNORE LIBRARY BOILERPLATE #    pipeline()
