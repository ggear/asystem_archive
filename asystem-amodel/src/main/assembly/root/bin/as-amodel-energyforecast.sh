#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

WAIT_TASK=${1:-"false"}
DELETE_CLUSTER=${2:-"false"}
DO_PREPARATION=${3:-"true"}
DO_TRAINING=${4:-"true"}

[[ "$DELETE_CLUSTER" = "true" ]] && WAIT_TASK="true"

$ROOT_DIR/bin/cldr-provision.sh "$WAIT_TASK"

[[ "$DO_PREPARATION" = "true" ]] && $ROOT_DIR/bin/cldr-shell-spark2.sh \
  "false" \
  "asystem-energyforecast-preparation" \
  "com.jag.asystem.amodel.EnergyForecastPreparation" \
  "$S3_URL_ASTORE/ $S3_URL_AMODEL/asystem/amodel/energyforecast/" \
  "--num-executors ""$SPARK_EXEC_NUM"" --executor-cores ""$SPARK_EXEC_CORES"" --executor-memory ""$SPARK_EXEC_MEMORY""" \
  "$S3_URL_ALIB/jar/"

[[ "$DO_TRAINING" = "true" ]] && $ROOT_DIR/bin/cldr-shell-pyspark2.sh \
  "$WAIT_TASK" \
  "asystem-energyforecast-training" \
  "energyforecast.py" \
  "" \
  "--num-executors 1 --executor-cores 1 --executor-memory 1g" \
  "$S3_URL_ALIB/py/"

[[ "$DELETE_CLUSTER" = "true" ]] && $ROOT_DIR/bin/cldr-provision.sh "true" "true"
