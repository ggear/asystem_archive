package com.jag.asystem.amodel

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import com.cloudera.framework.common.Driver.{CONF_CLDR_JOB_NAME, FAILURE_ARGUMENTS, SUCCESS, getApplicationProperty}
import com.cloudera.framework.common.DriverSpark
import com.jag.asystem.amodel.Counter.{TESTING_INSTANCES, TRAINING_INSTANCES}
import com.jag.asystem.amodel.DatumFactory.getModelProperty
import com.jag.asystem.amodel.EnergyForecastInterday.DaysBlacklist
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.Instant
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object EnergyForecastInterday {

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

  private var inputPath: Path = _
  private var outputPath: Path = _
  private val inputPaths = Map(
    "sun" -> ArrayBuffer[String](),
    "wind" -> ArrayBuffer[String](),
    "rain" -> ArrayBuffer[String](),
    "energy" -> ArrayBuffer[String](),
    "humidity" -> ArrayBuffer[String](),
    "conditions" -> ArrayBuffer[String](),
    "temperature" -> ArrayBuffer[String]()
  )
  private var timestampStart: Instant = _
  private val outputPathSuffix: String = "/text/csv/none/amodel_version=" + getApplicationProperty("APP_VERSION") +
    "/amodel_model=" + getModelProperty("MODEL_ENERGYFORECAST_INTERDAY_BUILD_VERSION")

  val Log: Logger = LoggerFactory.getLogger(classOf[EnergyForecastInterday])

  override def prepare(arguments: String*): Int = {
    var exit = SUCCESS
    timestampStart = Instant.now
    try {
      if (arguments == null || arguments.length != parameters().length) exit = FAILURE_ARGUMENTS else {
        inputPath = new Path(arguments(0))
        var dfs = inputPath.getFileSystem(getConf)
        inputPath = dfs.makeQualified(inputPath)
        var inputPathsRegex: Map[String, Regex] = Map()
        inputPaths.keys.foreach(partition => inputPathsRegex +=
          (partition -> (".*/asystem/astore/processed/canonical/parquet/dict/snappy/.*/astore_metric=" + partition + "/.*\\.parquet").r))
        val files = dfs.listFiles(inputPath, true)
        while (files.hasNext) {
          val fileUri = files.next().getPath.toString
          for ((partition, filePattern) <- inputPathsRegex) {
            fileUri match {
              case filePattern() => inputPaths(partition) += fileUri
              case _ =>
            }
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
              inputPaths.values.foreach(files => files.clear())
            }
          }
        }
      }
    } finally {
      if (Log.isInfoEnabled()) {
        Log.info("Driver [" + classOf[EnergyForecastInterday].getSimpleName +
          "] prepared with output [" + outputPath.toString + "], input [" + inputPath.toString + "] and inputs:")
        inputPaths.values.foreach(paths => paths.foreach(path => Log.info("  " + path)))
      }
      if (exit != SUCCESS) {
        val metaData = getMetaData(exit, timestampStart)
        pushMetaData(metaData)
        if (Log.isInfoEnabled) Log.info("Driver [" + this.getClass.getSimpleName +
          "] metadata: " + pullMetaData(metaData).mkString(" "))
      }
    }
    exit
  }

  override def parameters(): Array[String] = {
    Array("input-uri", "output-uri")
  }

  override def execute(): Int = {
    val exit = SUCCESS
    var recordsTrain = 0L
    var recordsTest = 0L
    val spark = SparkSession.builder.config(new SparkConf).appName(
      getConf.get(CONF_CLDR_JOB_NAME, "asystem-energyforecast-preparation")).getOrCreate()
    import spark.implicits._
    try {
      var outputTest = None: Option[DataFrame]
      var outputTrain = None: Option[DataFrame]
      if (!inputPaths.mapValues(_.size).values.contains(0)) {
        val dateFormat = "y/MM/dd"
        val timezoneWorking = "Australia/Perth"
        val timezoneDefault = TimeZone.getDefault.getID
        for ((partition, paths) <- inputPaths)
          spark.read.parquet(paths: _*).cache().createTempView(partition)
        var outputAll = List(
          s"""
             | SELECT
             |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
             |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
             |   max(data_value) / first(data_scale) AS energy__production__inverter
             | FROM energy
             | WHERE
             |   data_metric='energy__production__inverter' AND
             |   data_type='integral' AND bin_width=1 AND bin_unit='day'
             | GROUP BY datum__bin__date
                  """.stripMargin,
          s"""
             | SELECT
             |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
             |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
             |   max(data_value) / first(data_scale) AS temperature__forecast__glen_Dforrest
             | FROM temperature
             | WHERE
             |   data_metric='temperature__forecast__glen_Dforrest' AND
             |   data_type='point' AND bin_width=1 AND bin_unit='day'
             | GROUP BY datum__bin__date
                  """.stripMargin,
          s"""
             | SELECT
             |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
             |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
             |   max(data_value) / first(data_scale) AS rain__forecast__glen_Dforrest
             | FROM rain
             | WHERE
             |   data_metric='rain__forecast__glen_Dforrest' AND
             |   data_type='integral' AND bin_width=1 AND bin_unit='day_Dtime'
             | GROUP BY datum__bin__date
                  """.stripMargin,
          s"""
             | SELECT
             |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
             |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
             |   max(data_value) / first(data_scale) AS humidity__forecast__glen_Dforrest
             | FROM humidity
             | WHERE
             |   data_metric='humidity__forecast__glen_Dforrest' AND
             |   data_type='mean' AND bin_width=1 AND bin_unit='day'
             | GROUP BY datum__bin__date
                  """.stripMargin,
          s"""
             | SELECT
             |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
             |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
             |   max(data_value) / first(data_scale) AS wind__forecast__glen_Dforrest
             | FROM wind
             | WHERE
             |   data_metric='wind__forecast__glen_Dforrest' AND
             |   data_type='mean' AND bin_width=1 AND bin_unit='day'
             | GROUP BY datum__bin__date
                  """.stripMargin,
          s"""
             | SELECT
             |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
             |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
             |   max(data_value) AS sun__outdoor__rise
             | FROM sun
             | WHERE
             |   data_metric='sun__outdoor__rise' AND
             |   data_type='epoch' AND bin_width=1 AND bin_unit='day'
             | GROUP BY datum__bin__date
                  """.stripMargin,
          s"""
             | SELECT
             |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
             |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
             |   max(data_value) AS sun__outdoor__set
             | FROM sun
             | WHERE
             |   data_metric='sun__outdoor__set' AND
             |   data_type='epoch' AND bin_width=1 AND bin_unit='day'
             | GROUP BY datum__bin__date
                  """.stripMargin,
          s"""
             | SELECT
             |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
             |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
             |   max(data_value) / first(data_scale) AS sun__outdoor__azimuth
             | FROM sun
             | WHERE
             |   data_metric='sun__outdoor__azimuth' AND
             |   data_type='point' AND bin_width=2 AND bin_unit='second'
             | GROUP BY datum__bin__date
                  """.stripMargin,
          s"""
             | SELECT
             |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
             |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
             |   max(data_value) / first(data_scale) AS sun__outdoor__altitude
             | FROM sun
             | WHERE
             |   data_metric='sun__outdoor__altitude'
             |   AND data_type='point' AND bin_width=2 AND bin_unit='second'
             | GROUP BY datum__bin__date
                  """.stripMargin,
          s"""
             | SELECT
             |   date_format(from_utc_timestamp(to_utc_timestamp(cast(bin_timestamp as timestamp),
             |   '$timezoneDefault'), '$timezoneWorking'), '$dateFormat') AS datum__bin__date,
             |   last(data_string) AS conditions__forecast__glen_Dforrest
             | FROM conditions
             | WHERE
             |   data_metric='conditions__forecast__glen_Dforrest'
             |   AND data_type='enumeration' AND bin_width=1 AND bin_unit='day'
             | GROUP BY datum__bin__date
                  """.stripMargin)
          .map(spark.sql).reduce(_.join(_, "datum__bin__date"))
          .where($"datum__bin__date" < new SimpleDateFormat(dateFormat).format(Calendar.getInstance().getTime))
        DaysBlacklist.foreach(day => outputAll = outputAll.where($"datum__bin__date" =!= day))
        outputAll = outputAll.coalesce(1).orderBy("datum__bin__date").cache()
        addResult("All data:")
        addResult("  " + outputAll.columns.mkString(","))
        outputAll.collect.foreach(row => addResult("  " + row.mkString(",")))
        val outputTrainDays = outputAll
          .select("datum__bin__date", "conditions__forecast__glen_Dforrest")
          .groupBy("conditions__forecast__glen_Dforrest")
          .agg(first("datum__bin__date").as("datum__bin__date"))
          .orderBy("datum__bin__date")
          .union(outputAll
            .select("datum__bin__date", "conditions__forecast__glen_Dforrest")
            .groupBy("conditions__forecast__glen_Dforrest")
            .agg(last("datum__bin__date").as("datum__bin__date"))
            .orderBy("datum__bin__date")
          ).drop("conditions__forecast__glen_Dforrest").orderBy("datum__bin__date")
          .dropDuplicates().coalesce(1).cache()
        outputTrain = Some(outputAll.join(outputTrainDays, Seq("datum__bin__date"), "leftanti").
          sort(asc("datum__bin__date")).coalesce(1).cache())
        outputTrain.get.write.option("header", "true").csv(outputPath.toString + "/train" + outputPathSuffix)
        outputTest = Some(outputAll.join(outputTrainDays, Seq("datum__bin__date"), "inner").
          sort(asc("datum__bin__date")).coalesce(1).cache())
        outputTest.get.write.option("header", "true").csv(outputPath.toString + "/test" + outputPathSuffix)
      }
      else {
        outputTrain = Some(spark.read.option("header", "true").csv(outputPath + "/train" + outputPathSuffix))
        outputTest = Some(spark.read.option("header", "true").csv(outputPath + "/test" + outputPathSuffix))
      }
      if (outputTrain.isDefined && outputTest.isDefined) {
        addResult("Train data:")
        addResult("  " + outputTrain.get.columns.mkString(","))
        outputTrain.get.collect.foreach(row => addResult("  " + row.mkString(",")))
        recordsTrain = outputTrain.get.count()
        addResult("Test data:")
        addResult("  " + outputTest.get.columns.mkString(","))
        outputTest.get.collect.foreach(row => addResult("  " + row.mkString(",")))
        recordsTest = outputTest.get.count()
      }
    } finally {
      spark.catalog.clearCache()
      spark.close()
      val metaData = getMetaData(exit, timestampStart)
      addMetaDataCounter(metaData, TRAINING_INSTANCES, recordsTrain)
      addMetaDataCounter(metaData, TESTING_INSTANCES, recordsTest)
      pushMetaData(metaData)
      if (Log.isInfoEnabled()) Log.info("Driver [" + this.getClass.getSimpleName + "] execute metadata: " +
        pullMetaData(metaData).mkString(" "))
    }
    exit
  }

  def getMetaData(exit: Integer, started: Instant = Instant.now, ended: Instant = Instant.now): Execution = {
    new Execution(getConf, new Template(getConf), exit, timestampStart, Instant.now)
  }

}

