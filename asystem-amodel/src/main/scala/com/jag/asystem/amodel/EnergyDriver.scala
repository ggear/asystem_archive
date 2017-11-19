package com.jag.asystem.amodel

import java.util.UUID

import com.cloudera.framework.common.Driver.Counter.{RECORDS_IN, RECORDS_OUT}
import com.cloudera.framework.common.Driver.{Engine, FAILURE_ARGUMENTS, SUCCESS, getApplicationProperty}
import com.cloudera.framework.common.DriverSpark
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RemoteIterator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

class EnergyDriver(configuration: Configuration) extends DriverSpark(configuration) {

  var outputPath: Path = _
  var inputPaths: Set[Path] = Set()

  val Log: Logger = LoggerFactory.getLogger(classOf[EnergyDriver])

  override def prepare(arguments: String*): Int = {
    if (arguments == null || arguments.length != parameters.length) return FAILURE_ARGUMENTS
    outputPath = new Path(arguments(1))
    var dfs = outputPath.getFileSystem(getConf)
    outputPath = dfs.makeQualified(outputPath)
    if (dfs.exists(outputPath)) if (outputPath.toString.contains("-SNAPSHOT/amodel/")) dfs.delete(outputPath, true) else {
      if (Log.isErrorEnabled()) Log.error("Driver [" + classOf[EnergyDriver].getSimpleName +
        "] cannot write to pre-existing non-SNAPSHOT directory [" + outputPath + "]")
      return FAILURE_ARGUMENTS
    }
    var inputPath = new Path(arguments(0))
    dfs = inputPath.getFileSystem(getConf)
    inputPath = dfs.makeQualified(inputPath)
    val files = dfs.listFiles(inputPath, true)
    while (files.hasNext) {
      val file = files.next()
      if (file.getPath.depth > 3) inputPaths += dfs.makeQualified(file.getPath.getParent.getParent.getParent)
    }
    if (Log.isInfoEnabled()) Log.info("Driver [" + classOf[EnergyDriver].getSimpleName + "] prepared with input [" +
      inputPath.toString + "], inputs [" + inputPaths.mkString(", ") + "] and output [" + outputPath + "]")
    SUCCESS
  }

  override def parameters(): Array[String] = {
    Array("input-path", "output-path")
  }

  override def execute(): Int = {
    val spark = SparkSession.builder.config(new SparkConf).appName("asystem-energy-model-preparation").getOrCreate()
    val inputs = new ListBuffer[DataFrame]()
    inputPaths.foreach(inputPath => inputs += spark.read.format("com.databricks.spark.avro").load(inputPath.toString))
    if (inputs.nonEmpty) {
      var input = inputs.head
      for (i <- 1 until inputs.length) input = input.union(inputs(i))
      incrementCounter(RECORDS_IN, input.count())
      input.createGlobalTempView("datums")
      var outputAll = spark.sql(
        """
          SELECT
            ep.epd AS datum__bin__date,
            ep.epv AS energy__production__inverter,
            ft.ftv AS temperature__forecast__glen_Dforrest,
            fr.frv AS rain__forecast__glen_Dforrest,
            fh.fhv AS humidity__forecast__glen_Dforrest,
            fw.fwv AS wind__forecast__glen_Dforrest,
            sr.srv AS sun__outdoor__rise,
            ss.ssv AS sun__outdoor__set,
            sz.szv AS sun__outdoor__azimuth,
            sa.sav AS sun__outdoor__altitude,
            fc.fcv AS conditions__forecast__glen_Dforrest
          FROM
            (SELECT
              date_format(from_utc_timestamp(from_unixtime(bin_timestamp), 'AWST'), 'YYYY/MM/dd') AS epd,
              max(data_value) / first(data_scale) AS epv
            FROM global_temp.datums
            WHERE
              data_metric='energy__production__inverter' AND data_type='integral' AND bin_width=1 AND bin_unit='day'
            GROUP BY epd) AS ep,
            (SELECT
              date_format(from_utc_timestamp(from_unixtime(bin_timestamp), 'AWST'), 'YYYY/MM/dd') AS ftd,
              max(data_value) / first(data_scale) AS ftv
            FROM global_temp.datums
            WHERE
              data_metric='temperature__forecast__glen_Dforrest' AND data_type='high' AND bin_width=1 AND bin_unit='day'
            GROUP BY ftd) AS ft,
            (SELECT
              date_format(from_utc_timestamp(from_unixtime(bin_timestamp), 'AWST'), 'YYYY/MM/dd') AS frd,
              max(data_value) / first(data_scale) AS frv
            FROM global_temp.datums
            WHERE
              data_metric='rain__forecast__glen_Dforrest' AND data_type='integral' AND bin_width=1 AND bin_unit='day_Dtime'
            GROUP BY frd) AS fr,
            (SELECT
              date_format(from_utc_timestamp(from_unixtime(bin_timestamp), 'AWST'), 'YYYY/MM/dd') AS fhd,
              max(data_value) / first(data_scale) AS fhv
            FROM global_temp.datums
            WHERE
              data_metric='humidity__forecast__glen_Dforrest' AND data_type='mean' AND bin_width=1 AND bin_unit='day'
            GROUP BY fhd) AS fh,
            (SELECT
              date_format(from_utc_timestamp(from_unixtime(bin_timestamp), 'AWST'), 'YYYY/MM/dd') AS fwd,
              max(data_value) / first(data_scale) AS fwv
            FROM global_temp.datums
            WHERE
              data_metric='wind__forecast__glen_Dforrest' AND data_type='mean' AND bin_width=1 AND bin_unit='day'
            GROUP BY fwd) AS fw,
            (SELECT
              date_format(from_utc_timestamp(from_unixtime(bin_timestamp), 'AWST'), 'YYYY/MM/dd') AS srd,
              max(data_value) AS srv
            FROM global_temp.datums
            WHERE
              data_metric='sun__outdoor__rise' AND data_type='epoch' AND bin_width=1 AND bin_unit='day'
            GROUP BY srd) AS sr,
            (SELECT
              date_format(from_utc_timestamp(from_unixtime(bin_timestamp), 'AWST'), 'YYYY/MM/dd') AS ssd,
              max(data_value) AS ssv
            FROM global_temp.datums
            WHERE
              data_metric='sun__outdoor__set' AND data_type='epoch' AND bin_width=1 AND bin_unit='day'
            GROUP BY ssd) AS ss,
            (SELECT
              date_format(from_utc_timestamp(from_unixtime(bin_timestamp), 'AWST'), 'YYYY/MM/dd') AS szd,
              max(data_value) / first(data_scale) AS szv
            FROM global_temp.datums
            WHERE
              data_metric='sun__outdoor__azimuth' AND data_type='point' AND bin_width=2 AND bin_unit='second'
            GROUP BY szd) AS sz,
            (SELECT
              date_format(from_utc_timestamp(from_unixtime(bin_timestamp), 'AWST'), 'YYYY/MM/dd') AS sad,
              max(data_value) / first(data_scale) AS sav
            FROM global_temp.datums
            WHERE
              data_metric='sun__outdoor__altitude' AND data_type='point' AND bin_width=2 AND bin_unit='second'
            GROUP BY sad) AS sa,
            (SELECT
              date_format(from_utc_timestamp(from_unixtime(bin_timestamp), 'AWST'), 'YYYY/MM/dd') AS fcd,
              last(data_string) AS fcv
            FROM global_temp.datums
            WHERE
              data_metric='conditions__forecast__glen_Dforrest' AND data_type='enumeration' AND bin_width=1 AND bin_unit='day'
            GROUP BY fcd) AS fc
          WHERE
            ep.epd = ft.ftd AND
            ep.epd = fr.frd AND
            ep.epd = fh.fhd AND
            ep.epd = fw.fwd AND
            ep.epd = sr.srd AND
            ep.epd = ss.ssd AND
            ep.epd = sz.szd AND
            ep.epd = sa.sad AND
            ep.epd = fc.fcd AND
            ep.epd != '2017/10/12' AND
            ep.epd != '2017/10/13' AND
            ep.epd != '2017/10/29' AND
            ep.epd != '2017/10/30' AND
            ep.epd != date_format(from_utc_timestamp(from_unixtime(unix_timestamp()), 'AWST'), 'YYYY/MM/dd')
          ORDER BY
            ep.epd ASC
        """
      )
      var outputAllCount = outputAll.count().toInt
      incrementCounter(RECORDS_OUT, outputAllCount)
      val outputTraining = outputAll.limit(outputAllCount - 1)
      outputTraining.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(outputPath.toString +
        "/training/text/csv/none")
      val outputValidation = outputAll.sort(desc("datum__bin__date")).limit(1)
      outputValidation.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(outputPath.toString +
        "/validation/text/csv/none")
      addResult(outputAll.columns.mkString(","))
      outputAll.collect.foreach(row => addResult(row.mkString(",")))
    }
    else {
      incrementCounter(RECORDS_IN, 0)
      incrementCounter(RECORDS_OUT, 0)
    }
    spark.close()
    SUCCESS
  }

}

object EnergyDriver {

  def main(arguments: Array[String]): Unit = {
    new EnergyDriver(null).runner(arguments: _*)
  }

}
