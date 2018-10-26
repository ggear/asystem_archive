#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

WAIT_TASK=${1:-"true"}
DO_RELEASE=${2:-"true"}
PROCESS_STAGES=${3:-"interday-preparation,interday-training,intraday-all"}
DELETE_CLUSTER=${6:-"false"}
DO_PRODUCTION=${7:-"true"}

PROCESS_RETURN=0
PROCESS_TX=$(echo $(uuidgen) | sed "s/[[:alpha:].-]//g")
PROCESS_S3="$S3_URL_ASTAGING"
PROCESS_TAGS="production"
PROCESS_GROUP="asystem-amodel-energyforecast"
PROCESS_STAGES_ARRAY=(${PROCESS_STAGES//,/ })
PROCESS_JAR="$ROOT_DIR/lib/jar/$(basename $(dirname $(dirname $ROOT_DIR))).jar"
PROCESS_PROPERTIES="$ROOT_DIR/lib/avro/model.properties"
PROCESS_VALIDATION="$ROOT_DIR/lib/data/energyforecast_validate.csv"

[[ "$DELETE_CLUSTER" = "true" ]] && WAIT_TASK="true"
[[ "$APP_VERSION" = *-SNAPSHOT ]] && DO_RELEASE="false" && DO_PRODUCTION="false"
[[ "$DO_PRODUCTION" = "true" ]] && PROCESS_TAGS="production"
[[ "$DO_RELEASE" = "true" && "$DO_PRODUCTION" = "true" ]] && PROCESS_S3=""
[[ "$DO_RELEASE" = "true" && "$DO_PRODUCTION" = "true" ]] && WAIT_TASK="true"

$ROOT_DIR/bin/cldr-provision.sh "$WAIT_TASK"

$ROOT_DIR/bin/cldr-sync-s3.sh "$S3_URL_AMODEL" "$S3_URL_AMODEL""$S3_URL_ASTAGING" "true" "false"

for PROCESS_STAGE in "${PROCESS_STAGES_ARRAY[@]}"; do
  sleep 5
  if [ "$PROCESS_STAGE" = "interday-preparation" ]; then
  $ROOT_DIR/bin/cldr-shell-spark2.sh \
    "$WAIT_TASK" \
    "$PROCESS_GROUP-interday-preparation" \
    "com.jag.asystem.amodel.EnergyForecastInterday " \
    "--cldr.job.group=$PROCESS_GROUP --cldr.job.name=$PROCESS_GROUP-validation --cldr.job.version=$APP_VERSION --cldr.job.transaction=$PROCESS_TX --com.jag.metadata.tags=$PROCESS_TAGS --cldr.job.metadata=true $S3_URL_ASTORE/ $S3_URL_AMODEL$S3_URL_ASTAGING/asystem/amodel/energyforecastinterday/" \
    "--num-executors ""$SPARK_EXEC_NUM"" --executor-cores ""$SPARK_EXEC_CORES"" --executor-memory ""$SPARK_EXEC_MEMORY""" \
    "$S3_URL_ALIB/jar/"
  elif [ "$PROCESS_STAGE" = "interday-training" ]; then
  $ROOT_DIR/bin/cldr-shell-pyspark2.sh \
    "$WAIT_TASK" \
    "$PROCESS_GROUP-interday-training" \
    "energyforecast_interday.py" \
    "" \
    "--num-executors ""$SPARK_EXEC_NUM"" --executor-cores ""$SPARK_EXEC_CORES"" --executor-memory ""$SPARK_EXEC_MEMORY""" \
    "$S3_URL_ALIB/py/"
  elif [ "$PROCESS_STAGE" = "intraday-all" ]; then
  $ROOT_DIR/bin/cldr-shell-pyspark2.sh \
    "$WAIT_TASK" \
    "$PROCESS_GROUP-intraday-training" \
    "energyforecast_intraday.py" \
    "" \
    "--num-executors ""$SPARK_EXEC_NUM"" --executor-cores ""$SPARK_EXEC_CORES"" --executor-memory ""$SPARK_EXEC_MEMORY""" \
    "$S3_URL_ALIB/py/"
  fi
done

if [[ "$WAIT_TASK" = "true" ]]; then
  if $ROOT_DIR/lib/py/energyforecast_release.py --connection_jar=$PROCESS_JAR --transaction_id=$PROCESS_TX --job_group=$PROCESS_GROUP --job_name=$PROCESS_GROUP-validation --job_version=$APP_VERSION --job_properties=$PROCESS_PROPERTIES --job_validation=$PROCESS_VALIDATION; then
    if [[ "$DO_RELEASE" = "true" ]] && [[ "$DO_PRODUCTION" = "true" ]]; then
      $ROOT_DIR/bin/cldr-sync-s3.sh "$S3_URL_AMODEL""$S3_URL_ASTAGING" "$S3_URL_AMODEL" "true"
    fi
  else
    echo "Release quality script failed, halting release"
    PROCESS_RETURN=1
  fi
fi

[[ "$DELETE_CLUSTER" = "true" ]] && $ROOT_DIR/bin/cldr-provision.sh "true" "true"

exit ${PROCESS_RETURN}
