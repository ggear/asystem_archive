#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

WAIT_TASK=${1:-"true"}
DO_RELEASE=${2:-"true"}
PROCESS_STAGES=${3:-"repair,batch,stats"}
DELETE_CLUSTER=${4:-"false"}

PROCESS_TX=$(uuidgen)
PROCESS_GROUP="asystem-astore-process"
PROCESS_STAGES_ARRAY=(${PROCESS_STAGES//,/ })
PROCESS_JAR="$ROOT_DIR/lib/jar/$(basename $(dirname $(dirname $ROOT_DIR))).jar"

[[ "$DELETE_CLUSTER" = "true" ]] && WAIT_TASK="true"
[[ "$APP_VERSION" = *-SNAPSHOT ]] && DO_RELEASE="false"

$ROOT_DIR/bin/cldr-provision.sh "$WAIT_TASK"

$ROOT_DIR/bin/cldr-sync-s3.sh "$S3_URL_ASTORE" "$S3_URL_ASTORE""$S3_URL_ATEMP" "true" "false"

for PROCESS_STAGE in "${PROCESS_STAGES_ARRAY[@]}"; do
  $ROOT_DIR/bin/cldr-shell-spark2.sh \
    "$WAIT_TASK" \
    "$PROCESS_GROUP-$PROCESS_STAGE" \
    "com.jag.asystem.astore.Process" \
    " --cldr.job.group=$PROCESS_GROUP --cldr.job.name=$PROCESS_GROUP-$PROCESS_STAGE --cldr.job.version=$APP_VERSION --cldr.job.transaction=$PROCESS_TX $PROCESS_STAGE $S3_URL_ASTORE$S3_URL_ATEMP/" \
    "--num-executors ""$SPARK_EXEC_NUM"" --executor-cores ""$SPARK_EXEC_CORES"" --executor-memory ""$SPARK_EXEC_MEMORY""" \
    "$S3_URL_ALIB/jar/"
done

if $ROOT_DIR/lib/py/process.py --connection_jar=$PROCESS_JAR --transaction_id=$PROCESS_TX; then
  echo "SUCCESS!"
fi

[[ "$DELETE_CLUSTER" = "true" ]] && $ROOT_DIR/bin/cldr-provision.sh "true" "true"
