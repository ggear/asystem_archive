#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/../..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

export FLUME_AGENT_CONFIG=$(echo "$FLUME_AGENT_CONFIG" | \
	sed -e "s|\$APP_VERSION|$APP_VERSION|g" | \
	sed -e "s|\$S3_URL_ASTORE|$S3_URL_ASTORE|g" | \
	sed -e "s|\$MQTT_ACCESS_KEY|$MQTT_ACCESS_KEY|g" | \
	sed -e "s|\$MQTT_SECRET_KEY|$MQTT_SECRET_KEY|g" | \
	sed -e "s|\$MQTT_BROKER_HOST|$MQTT_BROKER_HOST|g" | \
	sed -e "s|\$MQTT_BROKER_PORT|$MQTT_BROKER_PORT|g" | \
	sed -e "s|\$MQTT_DROP_SNAPSHOTS|$MQTT_DROP_SNAPSHOTS|g" | \
	sed -e "s|\$MQTT_BATCHSIZE|$MQTT_BATCHSIZE|g" | \
	sed -e "s|\$HDFS_BATCHSIZE|$HDFS_BATCHSIZE|g" | \
	sed -e "s|\$AVRO_SCHEMA_URL|$AVRO_SCHEMA_URL|g" | \
	sed -e "s|\$FLUME_MQTT_JOURNAL_DIR|$FLUME_MQTT_JOURNAL_DIR|g" | \
	sed -e "s|\$FLUME_MQTT_CHECKPOINT_DIR|$FLUME_MQTT_CHECKPOINT_DIR|g" | \
	sed -e "s|\$FLUME_MQTT_DATA_DIRS|$FLUME_MQTT_DATA_DIRS|g" \
)
