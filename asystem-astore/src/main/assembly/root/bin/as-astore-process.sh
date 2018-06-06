#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

WAIT_TASK=${1:-"true"}
DO_RELEASE=${2:-"true"}
PROCESS_STAGES=${3:-"repair,batch,stats"}
DELETE_CLUSTER=${4:-"false"}
DO_PRODUCTION=${5:-"false"}

PROCESS_RETURN=0
PROCESS_TX=$(echo $(uuidgen) | sed "s/[[:alpha:].-]//g")
PROCESS_S3="$S3_URL_ASTAGING"
PROCESS_TAGS="staging"
PROCESS_GROUP="asystem-astore-process"
PROCESS_STAGES_ARRAY=(${PROCESS_STAGES//,/ })
PROCESS_JAR="$ROOT_DIR/lib/jar/$(basename $(dirname $(dirname $ROOT_DIR))).jar"

[[ "$DELETE_CLUSTER" = "true" ]] && WAIT_TASK="true"
[[ "$APP_VERSION" = *-SNAPSHOT ]] && DO_RELEASE="false" && DO_PRODUCTION="false"
[[ "$DO_PRODUCTION" = "true" ]] && PROCESS_TAGS="production"
[[ "$DO_RELEASE" = "true" && "$DO_PRODUCTION" = "true" ]] && PROCESS_S3=""
[[ "$DO_RELEASE" = "true" && "$DO_PRODUCTION" = "true" ]] && WAIT_TASK="true"

$ROOT_DIR/bin/cldr-provision.sh "$WAIT_TASK"

[[ "$DO_RELEASE" != "true" || "$DO_PRODUCTION" != "true" ]] && $ROOT_DIR/bin/cldr-sync-s3.sh "$S3_URL_ASTORE" "$S3_URL_ASTORE""$S3_URL_ASTAGING" "true" "false"

for PROCESS_STAGE in "${PROCESS_STAGES_ARRAY[@]}"; do
  sleep 5
  $ROOT_DIR/bin/cldr-shell-spark2.sh \
    "$WAIT_TASK" \
    "$PROCESS_GROUP-$PROCESS_STAGE" \
    "com.jag.asystem.astore.Process" \
    " --cldr.job.group=$PROCESS_GROUP --cldr.job.name=$PROCESS_GROUP-$PROCESS_STAGE --cldr.job.version=$APP_VERSION --cldr.job.transaction=$PROCESS_TX --com.jag.metadata.tags=$PROCESS_TAGS --cldr.job.metadata=true $PROCESS_STAGE $S3_URL_ASTORE$PROCESS_S3/" \
    "--num-executors ""$SPARK_EXEC_NUM"" --executor-cores ""$SPARK_EXEC_CORES"" --executor-memory ""$SPARK_EXEC_MEMORY""" \
    "$S3_URL_ALIB/jar/"
done

if [[ "$DO_RELEASE" = "true" ]] && [[ "$DO_PRODUCTION" = "false" ]]; then
  if $ROOT_DIR/lib/py/process_release.py --connection_jar=$PROCESS_JAR --transaction_id=$PROCESS_TX; then
    $ROOT_DIR/bin/as-astore-process.sh "$WAIT_TASK" "true" "$PROCESS_STAGES" "false" "true"
    $ROOT_DIR/bin/cldr-sync-s3.sh "$S3_URL_ASTORE" "$S3_URL_ASTORE""$S3_URL_ASTAGING" "true" "false"
  else
    echo "Release quality script failed, halting release"
    PROCESS_RETURN=1
  fi
fi

[[ "$DELETE_CLUSTER" = "true" ]] && $ROOT_DIR/bin/cldr-provision.sh "true" "true"

exit PROCESS_RETURN
