package com.jag.asystem.amodel

import java.util.TimeZone

import com.cloudera.framework.common.Driver.Counter.{RECORDS_IN, RECORDS_OUT}
import com.cloudera.framework.common.Driver.{FAILURE_ARGUMENTS, SUCCESS, getApplicationProperty}
import com.cloudera.framework.common.{Driver, DriverSpark}
import com.jag.asystem.amodel.Counter.{RECORDS_TRAINING, RECORDS_VALIDATION}
import com.jag.asystem.amodel.DatumFactory.getModelProperty
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions._

class EnergyForecastPreparation(configuration: Configuration) extends DriverSpark(configuration) {

  var outputPath: Path = _
  var inputPaths: Set[String] = Set()
  val outputPathSuffix = "/text/csv/none/amodel_version=" + getApplicationProperty("APP_VERSION") +
    "/amodel_model=" + getModelProperty("MODEL_ENERGYFORECAST_VERSION")

  val Log = LoggerFactory.getLogger(classOf[EnergyForecastPreparation])

  override def prepare(arguments: String*): Int = {
    if (arguments == null || arguments.length != parameters().length) return FAILURE_ARGUMENTS
    outputPath = new Path(arguments(1))
    var dfs = outputPath.getFileSystem(getConf)
    outputPath = dfs.makeQualified(outputPath)
    for (path <- List(new Path(outputPath, "training" + outputPathSuffix), new Path(outputPath, "validation" + outputPathSuffix))) {
      if (dfs.exists(path)) if (getApplicationProperty("APP_VERSION").endsWith("-SNAPSHOT")) dfs.delete(path, true) else {
        if (Log.isErrorEnabled()) Log.error("Driver [" + classOf[EnergyForecastPreparation].getSimpleName +
          "] cannot write to pre-existing non-SNAPSHOT directory [" + path + "]")
        return FAILURE_ARGUMENTS
      }
    }
    var inputPath = new Path(arguments(0))
    dfs = inputPath.getFileSystem(getConf)
    inputPath = dfs.makeQualified(inputPath)
    val files = dfs.listFiles(inputPath, true)
    while (files.hasNext) {
      val fileUri = files.next().getPath.toString
      val fileRewritePattern = "(.*/asystem/astore/processed/canonical/parquet/dict/snappy)/.*\\.parquet".r
      fileUri match {
        case fileRewritePattern(fileRoot) => inputPaths += fileRoot
        case _ =>
      }
    }
    if (Log.isInfoEnabled()) {
      Log.info("Driver [" + classOf[EnergyForecastPreparation].getSimpleName +
        "] prepared with input [" + inputPath.toString + "] and inputs:")
      for (inputPath <- inputPaths) Log.info("  " + inputPath)
    }
    SUCCESS
  }

  override def parameters(): Array[String] = {
    Array("input-path", "output-path")
  }

  override def execute(): Int = {
    if (inputPaths.nonEmpty) {
      val spark = SparkSession.builder.config(new SparkConf).appName("asystem-energyforecast-preparation").getOrCreate()
      val defaultTimeZone = TimeZone.getDefault.getID
      val input = inputPaths
        .map(inputPath => spark.read.parquet(inputPath))
        .reduce((left: DataFrame, right: DataFrame) => left.union(right))
      input.createGlobalTempView("datums")
      val outputAll = List(
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$defaultTimeZone'), 'Australia/Perth'), 'YYYY/MM/dd') AS datum__bin__date,
           |   max(data_value) / first(data_scale) AS energy__production__inverter
           | FROM global_temp.datums
           | WHERE
           |   astore_metric='energy' AND data_metric='energy__production__inverter' AND
           |   data_type='integral' AND bin_width=1 AND bin_unit='day'
           | GROUP BY datum__bin__date
        """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$defaultTimeZone'), 'Australia/Perth'), 'YYYY/MM/dd') AS datum__bin__date,
           |   max(data_value) / first(data_scale) AS temperature__forecast__glen_Dforrest
           | FROM global_temp.datums
           | WHERE
           |   astore_metric='temperature' AND data_metric='temperature__forecast__glen_Dforrest' AND
           |   data_type='high' AND bin_width=1 AND bin_unit='day'
           | GROUP BY datum__bin__date
        """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$defaultTimeZone'), 'Australia/Perth'), 'YYYY/MM/dd') AS datum__bin__date,
           |   max(data_value) / first(data_scale) AS rain__forecast__glen_Dforrest
           | FROM global_temp.datums
           | WHERE
           |   astore_metric='rain' AND data_metric='rain__forecast__glen_Dforrest' AND
           |   data_type='integral' AND bin_width=1 AND bin_unit='day_Dtime'
           | GROUP BY datum__bin__date
        """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$defaultTimeZone'), 'Australia/Perth'), 'YYYY/MM/dd') AS datum__bin__date,
           |   max(data_value) / first(data_scale) AS humidity__forecast__glen_Dforrest
           | FROM global_temp.datums
           | WHERE
           |   astore_metric='humidity' AND data_metric='humidity__forecast__glen_Dforrest' AND
           |   data_type='mean' AND bin_width=1 AND bin_unit='day'
           | GROUP BY datum__bin__date
        """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$defaultTimeZone'), 'Australia/Perth'), 'YYYY/MM/dd') AS datum__bin__date,
           |   max(data_value) / first(data_scale) AS wind__forecast__glen_Dforrest
           | FROM global_temp.datums
           | WHERE
           |   astore_metric='wind' AND data_metric='wind__forecast__glen_Dforrest' AND
           |   data_type='mean' AND bin_width=1 AND bin_unit='day'
           | GROUP BY datum__bin__date
        """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$defaultTimeZone'), 'Australia/Perth'), 'YYYY/MM/dd') AS datum__bin__date,
           |   max(data_value) AS sun__outdoor__rise
           | FROM global_temp.datums
           | WHERE
           |   astore_metric='sun' AND data_metric='sun__outdoor__rise' AND
           |   data_type='epoch' AND bin_width=1 AND bin_unit='day'
           | GROUP BY datum__bin__date
        """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$defaultTimeZone'), 'Australia/Perth'), 'YYYY/MM/dd') AS datum__bin__date,
           |   max(data_value) AS sun__outdoor__set
           | FROM global_temp.datums
           | WHERE
           |   astore_metric='sun' AND data_metric='sun__outdoor__set' AND
           |   data_type='epoch' AND bin_width=1 AND bin_unit='day'
           | GROUP BY datum__bin__date
        """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$defaultTimeZone'), 'Australia/Perth'), 'YYYY/MM/dd') AS datum__bin__date,
           |   max(data_value) / first(data_scale) AS sun__outdoor__azimuth
           | FROM global_temp.datums
           | WHERE
           |   astore_metric='sun' AND data_metric='sun__outdoor__azimuth' AND
           |   data_type='point' AND bin_width=2 AND bin_unit='second'
           | GROUP BY datum__bin__date
        """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$defaultTimeZone'), 'Australia/Perth'), 'YYYY/MM/dd') AS datum__bin__date,
           |   max(data_value) / first(data_scale) AS sun__outdoor__altitude
           | FROM global_temp.datums
           | WHERE
           |   astore_metric='sun' AND data_metric='sun__outdoor__altitude'
           |   AND data_type='point' AND bin_width=2 AND bin_unit='second'
           | GROUP BY datum__bin__date
        """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$defaultTimeZone'), 'Australia/Perth'), 'YYYY/MM/dd') AS datum__bin__date,
           |   last(data_string) AS conditions__forecast__glen_Dforrest
           | FROM global_temp.datums
           | WHERE
           |   astore_metric='conditions' AND data_metric='conditions__forecast__glen_Dforrest'
           |   AND data_type='enumeration' AND bin_width=1 AND bin_unit='day'
           | GROUP BY datum__bin__date
        """.stripMargin)
        .map(sql => spark.sql(sql))
        .reduce((left: DataFrame, right: DataFrame) => left.join(right, "datum__bin__date"))
        .orderBy("datum__bin__date")
      addResult("All data:")
      addResult("  " + outputAll.columns.mkString(","))
      outputAll.collect.foreach(row => addResult("  " + row.mkString(",")))
      val outputTrainingDays = outputAll.
        select("datum__bin__date", "temperature__forecast__glen_Dforrest", "conditions__forecast__glen_Dforrest").
        groupBy("conditions__forecast__glen_Dforrest").agg(first("datum__bin__date").as("datum__bin__date")).
        drop("temperature__forecast__glen_Dforrest").drop("conditions__forecast__glen_Dforrest").union(outputAll.
        select("datum__bin__date", "temperature__forecast__glen_Dforrest", "conditions__forecast__glen_Dforrest").
        groupBy("conditions__forecast__glen_Dforrest").agg(last("datum__bin__date").as("datum__bin__date")).
        drop("temperature__forecast__glen_Dforrest").drop("conditions__forecast__glen_Dforrest")
      ).dropDuplicates()
      val outputTraining = outputAll.as("all").join(outputTrainingDays.as("training_days"), Seq("datum__bin__date"), "leftanti").
        sort(asc("datum__bin__date")).coalesce(1)
      outputTraining.write.format("com.databricks.spark.csv").option("header", "true").
        save(outputPath.toString + "/training" + outputPathSuffix)
      addResult("Training data:")
      addResult("  " + outputTraining.columns.mkString(","))
      outputTraining.collect.foreach(row => addResult("  " + row.mkString(",")))
      incrementCounter(RECORDS_TRAINING, outputTraining.count())
      val outputValidation = outputAll.as("all").join(outputTrainingDays.as("training_days"), Seq("datum__bin__date"), "inner").
        sort(asc("datum__bin__date")).coalesce(1)
      outputValidation.write.format("com.databricks.spark.csv").option("header", "true").
        save(outputPath.toString + "/validation" + outputPathSuffix)
      addResult("Validation data:")
      addResult("  " + outputValidation.columns.mkString(","))
      outputValidation.collect.foreach(row => addResult("  " + row.mkString(",")))
      incrementCounter(RECORDS_VALIDATION, outputValidation.count())
      spark.close()
    }
    else {
      incrementCounter(RECORDS_TRAINING, 0)
      incrementCounter(RECORDS_VALIDATION, 0)
    }
    SUCCESS
  }

}

object EnergyForecastPreparation {

  def main(arguments: Array[String]): Unit = {
    new EnergyForecastPreparation(null).runner(arguments: _*)
  }

}
