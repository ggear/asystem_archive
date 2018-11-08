#!/Users/graham/.conda/envs/python2/bin/python

# @formatter:off
#
# TODO:
#
# 1. Test performance improvements
# 2. Implement dir flattening on avro/parquet/model for local/S3
# 3. Implement router/store/model/anode on new flattening
# 4. Deploy router
# 5. Run dir flattening on store/model (50/1 min)
# 6. Run test all
# 7. Delete rewrite scripts
#
# 1.1 Time with current dir structure (373)
# 1.2. Time with flattened dir structure (343)
# 1.2. Time with S3Guard enabled
# 1.4. Time with partition dirs (via spark.parallelize(arrow/other) or as spark.load(list))
# 1.5. Time with (1) with pre-pruned parquet dirs (via spark.parallelize(arrow/other) or as spark.load(list))
#
# Custom pruning, partition collapsing, load(list)
# Arrow/S3FS?
# S3Gard?
# Impala?
# Hive?
#
# 5/asystem/astore/processed/canonical/parquet/dict/snappy/astore_version=10.000.0126/astore_year=2018/astore_month=9/astore_model=1001/astore_metric=upload/part-00001-056fe324-89e5-489a-afb5-249636d2c1ec.c000.snappy.parquet
# 5_asystem_astore_processed_canonical_parquet_dict_snappy/astore_version=10.000.0126_1001/astore_month=2018-09/astore_metric=upload/part-00001-056fe324-89e5-489a-afb5-249636d2c1ec.c000.snappy.parquet
# 13 dirs -> 4 dirs
#
# 5/asystem/astore/staged/canonical/avro/binary/snappy/arouter_version=10.000.0107/arouter_id=flume-c6cfd8dc-2838-4122-b00c-9b1cf66cdf44/arouter_ingest=1531180803/arouter_start=1531180800/arouter_finish=1531195200/arouter_model=1000/datum.1531187955988.avro
# 5_asystem_astore_staged_canonical_avro_binary_snappy/arouter_version=10.000.0107_1000/arouter_id=flume-c6cfd8dc-2838-4122-b00c-9b1cf66cdf44/arouter_ingest=1531180803_1531180800_1531195200/datum.1531187955988.avro
# 14 dirs -> 4 dirs
#
# https://stackoverflow.com/questions/41710053/can-i-read-multiple-files-into-a-spark-dataframe-from-s3-passing-over-nonexiste
# https://forums.databricks.com/questions/480/how-do-i-ingest-a-large-number-of-files-from-s3-my.html
# http://apache-spark-user-list.1001560.n3.nabble.com/Strategies-for-reading-large-numbers-of-files-td15644.html
# https://tech.kinja.com/how-not-to-pull-from-s3-using-apache-spark-1704509219
# https://gist.githubusercontent.com/pjrt/f1cad93b154ac8958e65/raw/7b0b764408f145f51477dc05ef1a99e8448bce6d/S3Puller.scala
# https://arrow.apache.org/blog/2017/07/26/spark-arrow
# https://medium.com/@subhojit20_27731/apache-spark-and-amazon-s3-gotchas-and-best-practices-a767242f3d98
# https://docs.aws.amazon.com/AmazonS3/latest/dev/request-rate-perf-considerations.html
#
# @formatter:on

import re

from boto.s3.connection import S3Connection

re_staged_original = re.compile(
    "(.*)/asystem/astore/staged/canonical/avro/binary/snappy"
    "/arouter_version=(.*)"
    "/arouter_id=(.*)"
    "/arouter_ingest=(.*)"
    "/arouter_start=(.*)"
    "/arouter_finish=(.*)"
    "/arouter_model=(.*)/(.*)")

re_processed_original = re.compile(
    "(.*)/asystem/astore/processed/canonical/parquet/dict/snappy"
    "/astore_version=(.*)"
    "/astore_year=(.*)"
    "/astore_month=(.*)"
    "/astore_model=(.*)"
    "/astore_metric=(.*)/(.*)")


def list_all(path):
    return (False, None)

def drop_staged(path):
    return (re_staged_original.match(path) is not None, None)


def run(refactor):
    bucket_name = "asystem-astore-staging"
    connection = S3Connection()
    bucket = connection.get_bucket(bucket_name)
    for key in list(bucket.list()):
        refactored = refactor(key.name)
        if refactored:
            if refactored[1]: bucket.copy_key(refactored[1], bucket_name, key.name)
            if refactored[0]: bucket.delete_key(key.name)
            print("{} =>\n  {}".format(key.name, refactored[1]))

run(list_all)
#run(drop_staged)