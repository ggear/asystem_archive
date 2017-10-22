#!/bin/bash
#[[#
# Driver script
#

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env
  do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

#$ROOT_DIR/bin/cloudera-framework-spark.sh \
#  jar $ROOT_DIR/lib/jar/*.jar \
#  com.jag.asystem.amodel.EnergyDriver \
#  -libjars $LIBJARS \
#  $HDFS_APP

aws s3 cp /Users/graham/_/dev/graham/asystem/asystem-amodel/target/assembly/asystem-amodel-10.000.0001-SNAPSHOT/lib/jar/asystem-amodel-10.000.0001-SNAPSHOT.jar s3://asystem-astore/tmp/jar/asystem-amodel-10.000.0001-SNAPSHOT.jar
aws s3 cp /Users/graham/_/dev/graham/asystem/asystem-amodel/target/assembly/asystem-amodel-10.000.0001-SNAPSHOT/lib/jar/dep/spark-avro_2.11-3.2.0.jar s3://asystem-astore/tmp/jar/spark-avro_2.11-3.2.0.jar
aws s3 cp /Users/graham/_/dev/graham/asystem/asystem-amodel/target/assembly/asystem-amodel-10.000.0001-SNAPSHOT/lib/jar/dep/cloudera-framework-common-1.6.0-cdh5.12.1-SNAPSHOT.jar s3://asystem-astore/tmp/jar/cloudera-framework-common-1.6.0-cdh5.12.1-SNAPSHOT.jar

altus dataeng submit-jobs \
--cluster-name graham-test \
--jobs '{ "sparkJob": {
                "jars": [
                    "s3a://asystem-astore/tmp/jar/asystem-amodel-10.000.0001-SNAPSHOT.jar",
                    "s3a://asystem-astore/tmp/jar/spark-avro_2.11-3.2.0.jar",
                    "s3a://asystem-astore/tmp/jar/cloudera-framework-common-1.6.0-cdh5.12.1-SNAPSHOT.jar"
                ],
                "mainClass": "com.jag.asystem.amodel.EnergyDriver",
                "applicationArguments": [
                   "s3a://asystem-astore/",
                   "s3a://asystem-astore/tmp/"
                ]
            }}'
