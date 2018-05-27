package com.jag.asystem.amodel

import java.util.{Calendar, GregorianCalendar, TimeZone}

import com.cloudera.framework.common.Driver.{FAILURE_ARGUMENTS, SUCCESS, getApplicationProperty}
import com.cloudera.framework.common.DriverSpark
import com.jag.asystem.amodel.Counter.{RECORDS_TRAINING, RECORDS_VALIDATION}
import com.jag.asystem.amodel.DatumFactory.getModelProperty
import com.jag.asystem.amodel.EnergyForecastInterday.{DaysBlacklist, DaysVetted}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

object EnergyForecastInterday {

  val DaysVetted = "2018/05/25"

  val DaysBlacklist = List(
    "2017/10/12",
    "2017/10/13",
    "2017/10/29",
    "2017/10/30",
    "2017/11/27",
    "2017/11/28",
    "2017/12/18",
    "2018/05/21",
    "2018/05/25"
  )

  def main(arguments: Array[String]): Unit = {
    new EnergyForecastInterday(null).runner(arguments: _*)
  }

}

class EnergyForecastInterday(configuration: Configuration) extends DriverSpark(configuration) {

  var outputPath: Path = _
  var inputPaths: Set[String] = Set()
  var outputPathSuffix: String = "/text/csv/none/amodel_version=" + getApplicationProperty("APP_VERSION") +
    "/amodel_model=" + getModelProperty("MODEL_ENERGYFORECAST_INTERDAY_BUILD_VERSION")

  val Log: Logger = LoggerFactory.getLogger(classOf[EnergyForecastInterday])

  override def prepare(arguments: String*): Int = {
    if (arguments == null || arguments.length != parameters().length) return FAILURE_ARGUMENTS
    var inputPath = new Path(arguments(0))
    var dfs = inputPath.getFileSystem(getConf)
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
    outputPath = new Path(arguments(1))
    dfs = outputPath.getFileSystem(getConf)
    outputPath = dfs.makeQualified(outputPath)
    for (path <- List(
      new Path(outputPath, "train" + outputPathSuffix),
      new Path(outputPath, "test" + outputPathSuffix))) {
      if (dfs.exists(path)) {
        if (getApplicationProperty("APP_VERSION").endsWith("-SNAPSHOT")) dfs.delete(path.getParent, true) else {
          if (Log.isWarnEnabled()) Log.warn("Driver [" + classOf[EnergyForecastInterday].getSimpleName +
            "] cannot write to pre-existing non-SNAPSHOT directory [" + path.getParent + "], clearing inputs")
          inputPaths = Set.empty
        }
      }
    }
    if (Log.isInfoEnabled()) {
      Log.info("Driver [" + classOf[EnergyForecastInterday].getSimpleName +
        "] prepared with output [" + outputPath.toString + "], input [" + inputPath.toString + "] and inputs:")
      for (inputPath <- inputPaths) Log.info("  " + inputPath)
    }
    SUCCESS
  }

  override def parameters(): Array[String] = {
    Array("input-uri", "output-uri")
  }

  override def execute(): Int = {
    val spark = SparkSession.builder.config(new SparkConf).appName("asystem-energyforecast-preparation").getOrCreate()
    import spark.implicits._
    if (inputPaths.nonEmpty) {
      val dateFormat = "y/MM/dd"
      val timezoneWorking = "Australia/Perth"
      val timezoneDefault = TimeZone.getDefault.getID
      val calendarCurrent = new GregorianCalendar(TimeZone.getTimeZone(timezoneWorking))
      calendarCurrent.setTimeInMillis(Calendar.getInstance.getTimeInMillis)
      val input = inputPaths.map(spark.read.parquet(_)).reduce(_.union(_))
      input.createTempView("datums")
      var outputAll = List(
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
           |   max(data_value) / first(data_scale) AS energy__production__inverter
           | FROM datums
           | WHERE
           |   astore_metric='energy' AND data_metric='energy__production__inverter' AND
           |   data_type='integral' AND bin_width=1 AND bin_unit='day'
           | GROUP BY datum__bin__date
              """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
           |   max(data_value) / first(data_scale) AS temperature__forecast__glen_Dforrest
           | FROM datums
           | WHERE
           |   astore_metric='temperature' AND data_metric='temperature__forecast__glen_Dforrest' AND
           |   data_type='point' AND bin_width=1 AND bin_unit='day'
           | GROUP BY datum__bin__date
              """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
           |   max(data_value) / first(data_scale) AS rain__forecast__glen_Dforrest
           | FROM datums
           | WHERE
           |   astore_metric='rain' AND data_metric='rain__forecast__glen_Dforrest' AND
           |   data_type='integral' AND bin_width=1 AND bin_unit='day_Dtime'
           | GROUP BY datum__bin__date
              """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
           |   max(data_value) / first(data_scale) AS humidity__forecast__glen_Dforrest
           | FROM datums
           | WHERE
           |   astore_metric='humidity' AND data_metric='humidity__forecast__glen_Dforrest' AND
           |   data_type='mean' AND bin_width=1 AND bin_unit='day'
           | GROUP BY datum__bin__date
              """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
           |   max(data_value) / first(data_scale) AS wind__forecast__glen_Dforrest
           | FROM datums
           | WHERE
           |   astore_metric='wind' AND data_metric='wind__forecast__glen_Dforrest' AND
           |   data_type='mean' AND bin_width=1 AND bin_unit='day'
           | GROUP BY datum__bin__date
              """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
           |   max(data_value) AS sun__outdoor__rise
           | FROM datums
           | WHERE
           |   astore_metric='sun' AND data_metric='sun__outdoor__rise' AND
           |   data_type='epoch' AND bin_width=1 AND bin_unit='day'
           | GROUP BY datum__bin__date
              """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
           |   max(data_value) AS sun__outdoor__set
           | FROM datums
           | WHERE
           |   astore_metric='sun' AND data_metric='sun__outdoor__set' AND
           |   data_type='epoch' AND bin_width=1 AND bin_unit='day'
           | GROUP BY datum__bin__date
              """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
           |   max(data_value) / first(data_scale) AS sun__outdoor__azimuth
           | FROM datums
           | WHERE
           |   astore_metric='sun' AND data_metric='sun__outdoor__azimuth' AND
           |   data_type='point' AND bin_width=2 AND bin_unit='second'
           | GROUP BY datum__bin__date
              """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
           |   max(data_value) / first(data_scale) AS sun__outdoor__altitude
           | FROM datums
           | WHERE
           |   astore_metric='sun' AND data_metric='sun__outdoor__altitude'
           |   AND data_type='point' AND bin_width=2 AND bin_unit='second'
           | GROUP BY datum__bin__date
              """.stripMargin,
        s"""
           | SELECT
           |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
           |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
           |   last(data_string) AS conditions__forecast__glen_Dforrest
           | FROM datums
           | WHERE
           |   astore_metric='conditions' AND data_metric='conditions__forecast__glen_Dforrest'
           |   AND data_type='enumeration' AND bin_width=1 AND bin_unit='day'
           | GROUP BY datum__bin__date
              """.stripMargin)
        .map(spark.sql).reduce(_.join(_, "datum__bin__date"))
        .where($"datum__bin__date" < DaysVetted)
      DaysBlacklist.foreach(day => outputAll = outputAll.where($"datum__bin__date" =!= day))
      outputAll = outputAll.repartition(1).orderBy("datum__bin__date")
      addResult("All data:")
      addResult("  " + outputAll.columns.mkString(","))
      outputAll.collect.foreach(row => addResult("  " + row.mkString(",")))
      val outputTrainingDays = outputAll
        .select("datum__bin__date", "temperature__forecast__glen_Dforrest", "conditions__forecast__glen_Dforrest")
        .groupBy("conditions__forecast__glen_Dforrest")
        .agg(first("datum__bin__date").as("datum__bin__date"))
        .drop("temperature__forecast__glen_Dforrest")
        .drop("conditions__forecast__glen_Dforrest")
        .orderBy("datum__bin__date")
        .union(outputAll
          .select("datum__bin__date", "temperature__forecast__glen_Dforrest", "conditions__forecast__glen_Dforrest")
          .groupBy("conditions__forecast__glen_Dforrest")
          .agg(last("datum__bin__date").as("datum__bin__date"))
          .drop("temperature__forecast__glen_Dforrest")
          .drop("conditions__forecast__glen_Dforrest")
          .orderBy("datum__bin__date")
        ).dropDuplicates()
      val outputTraining = outputAll.as("all").join(outputTrainingDays.as("training_days"), Seq("datum__bin__date"), "leftanti").
        sort(asc("datum__bin__date")).coalesce(1)
      outputTraining.write.format("com.databricks.spark.csv").option("header", "true").
        save(outputPath.toString + "/train" + outputPathSuffix)
      addResult("Training data:")
      addResult("  " + outputTraining.columns.mkString(","))
      outputTraining.collect.foreach(row => addResult("  " + row.mkString(",")))
      incrementCounter(RECORDS_TRAINING, outputTraining.count())
      val outputValidation = outputAll.as("all").join(outputTrainingDays.as("training_days"), Seq("datum__bin__date"), "inner").
        sort(asc("datum__bin__date")).coalesce(1)
      outputValidation.write.format("com.databricks.spark.csv").option("header", "true").
        save(outputPath.toString + "/test" + outputPathSuffix)
      addResult("Validation data:")
      addResult("  " + outputValidation.columns.mkString(","))
      outputValidation.collect.foreach(row => addResult("  " + row.mkString(",")))
      incrementCounter(RECORDS_VALIDATION, outputValidation.count())
    }
    else {
      incrementCounter(RECORDS_TRAINING, 0)
      incrementCounter(RECORDS_VALIDATION, 0)
    }
    spark.close()
    SUCCESS
  }

}

