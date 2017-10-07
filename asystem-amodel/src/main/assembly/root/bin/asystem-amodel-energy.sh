#!/bin/bash
#[[#
# Driver script
#

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env
  do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

$ROOT_DIR/bin/cloudera-framework-spark.sh "\
  jar $ROOT_DIR/lib/jar/*.jar \
  com.jag.asystem.amodel.EnergyDriver \
  -libjars $LIBJARS \
  $HDFS_APP
