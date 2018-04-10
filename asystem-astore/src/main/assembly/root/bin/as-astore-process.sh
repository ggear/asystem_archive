#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

WAIT_TASK=${1:-"true"}
DO_RELEASE=${2:-"true"}
PROCESS_STAGES=${3:-"repair,batch,stats"}
DELETE_CLUSTER=${4:-"false"}

[[ "$DELETE_CLUSTER" = "true" ]] && WAIT_TASK="true"
[[ "$APP_VERSION" = *-SNAPSHOT ]] && DO_RELEASE="false"

$ROOT_DIR/bin/cldr-provision.sh "$WAIT_TASK"

$ROOT_DIR/bin/cldr-sync-s3.sh "$S3_URL_ASTORE" "$S3_URL_ASTORE""$S3_URL_ATEMP" "true" "false"

PROCESS_STAGES_ARRAY=(${PROCESS_STAGES//,/ })

for PROCESS_STAGE in "${PROCESS_STAGES_ARRAY[@]}"; do
  $ROOT_DIR/bin/cldr-shell-spark2.sh \
    "$WAIT_TASK" \
    "asystem-astore-process-""$PROCESS_STAGE" \
    "com.jag.asystem.astore.Process" \
    "$PROCESS_STAGE $S3_URL_ASTORE$S3_URL_ATEMP/" \
    "--num-executors ""$SPARK_EXEC_NUM"" --executor-cores ""$SPARK_EXEC_CORES"" --executor-memory ""$SPARK_EXEC_MEMORY""" \
    "$S3_URL_ALIB/jar/"
done

$ROOT_DIR/bin/cldr-sync-s3.sh "$S3_URL_ASTORE""$S3_URL_ATEMP" "$S3_URL_ASTORE" "$DO_RELEASE"
$ROOT_DIR/bin/cldr-sync-s3.sh "$S3_URL_ASTORE""$S3_URL_ATEMP" "$S3_URL_ASTORE" "$DO_RELEASE" "false" "*/processed/*" "*"
$ROOT_DIR/bin/cldr-sync-s3.sh "$S3_URL_ASTORE""$S3_URL_ATEMP" "$S3_URL_ASTORE" "$DO_RELEASE" "false" "*.avro.tmp" "*"

[[ "$DELETE_CLUSTER" = "true" ]] && $ROOT_DIR/bin/cldr-provision.sh "true" "true"
