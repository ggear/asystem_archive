#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

WAIT_TASK=${1:-"false"}
DELETE_CLUSTER=${2:-"false"}
PROCESS_STAGES=${3:-"repair,batch,stats"}

[[ "$DELETE_CLUSTER" = "true" ]] && WAIT_TASK="true"

$ROOT_DIR/bin/cldr-provision.sh "$WAIT_TASK"

PROCESS_STAGES_ARRAY=(${PROCESS_STAGES//,/ })

for PROCESS_STAGE in "${PROCESS_STAGES_ARRAY[@]}"; do
  $ROOT_DIR/bin/cldr-shell-spark2.sh \
    "$WAIT_TASK" \
    "asystem-astore-process" \
    "com.jag.asystem.astore.Process" \
    "$PROCESS_STAGE $S3_URL_ASTORE/" \
    "--num-executors ""$SPARK_EXEC_NUM"" --executor-cores ""$SPARK_EXEC_CORES"" --executor-memory ""$SPARK_EXEC_MEMORY""" \
    "$S3_URL_ALIB/jar/"
done

[[ "$DELETE_CLUSTER" = "true" ]] && $ROOT_DIR/bin/cldr-provision.sh "true" "true"
