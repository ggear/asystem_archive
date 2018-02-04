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

import sys
from pyspark.sql import SparkSession


def pipeline():
    remote_data_path = sys.argv[1] if len(sys.argv) > 1 else "s3a://asystem-astore"

    spark = SparkSession.builder.appName("asystem-amodel-datums").getOrCreate()

    spark.stop()

# Run pipeline
pipeline()

# Main function# IGNORE LIBRARY BOILERPLATE #if __name__ == "__main__":
# Run pipeline# IGNORE LIBRARY BOILERPLATE #    pipeline()
