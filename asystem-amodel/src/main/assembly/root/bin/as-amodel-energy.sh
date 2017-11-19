#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

DO_PREPARATION=${1:-"true"}
DO_TRAINING=${2:-"true"}

$ROOT_DIR/bin/cldr-provision.sh

[[ "$DO_PREPARATION" = "true" ]] && $ROOT_DIR/bin/cldr-shell-spark2.sh \
  "false" \
  "asystem-energy-model-preparation" \
  "com.jag.asystem.amodel.EnergyDriver" \
  "$S3_URL_ASTORE/ $S3_URL_AMODEL/asystem/$APP_VERSION/amodel/$MODEL_VERSION/energy/" \
  "--num-executors ""$SPARK_EXEC_NUM"" --executor-cores ""$SPARK_EXEC_CORES"" --executor-memory ""$SPARK_EXEC_MEMORY""" \
  "$S3_URL_ALIB/jar/"

[[ "$DO_TRAINING" = "true" ]] && $ROOT_DIR/bin/cldr-shell-pyspark2.sh \
  "true" \
  "asystem-energy-model-training" \
  "energy.py" \
  "" \
  "--num-executors 1 --executor-cores 1 --executor-memory 1g" \
  "$S3_URL_ALIB/py/"
