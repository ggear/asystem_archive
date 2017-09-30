#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/../..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

echo ""
echo ""
echo ""
echo $PARCEL_NAMESPACE
echo ""
echo ""
echo ""
echo ""

export FLUME_AGENT_CONFIG=$(echo "$FLUME_AGENT_CONFIG" | \
	sed -e "s|\$APP_VERSION|$APP_VERSION|g" | \
	sed -e "s|\$S3_URL|$S3_URL|g" | \
	sed -e "s|\$S3_APP|$S3_APP|g" | \
	sed -e "s|\$MQTT_ACCESS|$MQTT_ACCESS|g" | \
	sed -e "s|\$MQTT_SECRET|$MQTT_SECRET|g" | \
	sed -e "s|\$MQTT_BROKER_HOST|$MQTT_BROKER_HOST|g" | \
	sed -e "s|\$MQTT_BROKER_PORT|$MQTT_BROKER_PORT|g" | \
	sed -e "s|\$MQTT_BACK_OFF|$MQTT_BACK_OFF|g" | \
	sed -e "s|\$MQTT_MAX_BACK_OFF|$MQTT_MAX_BACK_OFF|g" | \
	sed -e "s|\$MQTT_BATCHSIZE|$MQTT_BATCHSIZE|g" | \
	sed -e "s|\$HDFS_BATCHSIZE|$HDFS_BATCHSIZE|g" | \
	sed -e "s|\$AVRO_SCHEMA_URL|$AVRO_SCHEMA_URL|g" | \
	sed -e "s|\$FLUME_MQTT_CHECKPOINT_DIR|$FLUME_MQTT_CHECKPOINT_DIR|g" | \
	sed -e "s|\$FLUME_MQTT_DATA_DIRS|$FLUME_MQTT_DATA_DIRS|g" \
)
