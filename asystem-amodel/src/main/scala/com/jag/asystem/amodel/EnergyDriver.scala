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

import scala.collection.mutable.ListBuffer

class EnergyDriver(configuration: Configuration) extends DriverSpark(configuration) {

  var outputPath: String = _
  var inputPaths: Set[String] = Set()

  val Log: Logger = LoggerFactory.getLogger(classOf[EnergyDriver])

  override def prepare(arguments: String*): Int = {
    if (arguments == null || arguments.length != parameters.length)
      return FAILURE_ARGUMENTS
    var inputPath = new Path(arguments(0))
    val dfs = inputPath.getFileSystem(getConf)
    inputPath = dfs.makeQualified(inputPath)
    val files = dfs.listFiles(inputPath, true)
    while (files.hasNext) {
      val file = files.next()
      if (file.getPath.depth > 12 && file.getPath.getParent.getParent.getParent.getParent.getParent.getParent.getParent.getParent
        .getParent.getParent.getParent.getParent.getName != new Path(arguments(1)).getName)
        inputPaths += dfs.makeQualified(file.getPath.getParent.getParent.getParent).toString
    }
    outputPath = dfs.makeQualified(new Path(arguments(1), UUID.randomUUID().toString)).toString
    if (Log.isInfoEnabled()) Log.info("Driver [" + classOf[EnergyDriver].getSimpleName + "] " + "prepared with input [" +
      inputPath.toString + "], inputs [" + inputPaths.mkString(", ") + "] and output [" + outputPath + "]")
    SUCCESS
  }

  override def parameters(): Array[String] = {
    Array("input-path", "output-path")
  }

  override def execute(): Int = {
    val spark = SparkSession.builder.config(new SparkConf).appName(getApplicationProperty("APP_NAME")).getOrCreate()
    val inputs = new ListBuffer[DataFrame]()
    inputPaths.foreach(inputPath => inputs += spark.read.format("com.databricks.spark.avro").load(inputPath))
    if (inputs.nonEmpty) {
      var input = inputs.head
      for (i <- 1 until inputs.length) input = input.union(inputs(i))
      incrementCounter(RECORDS_IN, input.count())
      input.createGlobalTempView("datums")
      val outputProduction = spark.sql(
        """
          SELECT
            from_unixtime(ep.epd, 'dd/MM/YYYY') AS datum__bin__date,
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
              bin_timestamp - pmod(bin_timestamp + 28800, 86400) AS epd,
              max(data_value) / first(data_scale) AS epv
            FROM global_temp.datums
            WHERE
              data_metric='energy__production__inverter' AND data_type='integral' AND bin_width=1 AND bin_unit='day'
            GROUP BY epd) AS ep,
            (SELECT
              bin_timestamp - pmod(bin_timestamp + 28800, 86400) AS ftd,
              max(data_value) / first(data_scale) AS ftv
            FROM global_temp.datums
            WHERE
              data_metric='temperature__forecast__glen_Dforrest' AND data_type='high' AND bin_width=1 AND bin_unit='day'
            GROUP BY ftd) AS ft,
            (SELECT
              bin_timestamp - pmod(bin_timestamp + 28800, 86400) AS frd,
              max(data_value) / first(data_scale) AS frv
            FROM global_temp.datums
            WHERE
              data_metric='rain__forecast__glen_Dforrest' AND data_type='integral' AND bin_width=1 AND bin_unit='day_Dtime'
            GROUP BY frd) AS fr,
            (SELECT
              bin_timestamp - pmod(bin_timestamp + 28800, 86400) AS fhd,
              max(data_value) / first(data_scale) AS fhv
            FROM global_temp.datums
            WHERE
              data_metric='humidity__forecast__glen_Dforrest' AND data_type='mean' AND bin_width=1 AND bin_unit='day'
            GROUP BY fhd) AS fh,
            (SELECT
              bin_timestamp - pmod(bin_timestamp + 28800, 86400) AS fwd,
              max(data_value) / first(data_scale) AS fwv
            FROM global_temp.datums
            WHERE
              data_metric='wind__forecast__glen_Dforrest' AND data_type='mean' AND bin_width=1 AND bin_unit='day'
            GROUP BY fwd) AS fw,
            (SELECT
              bin_timestamp - pmod(bin_timestamp + 28800, 86400) AS srd,
              max(data_value) AS srv
            FROM global_temp.datums
            WHERE
              data_metric='sun__outdoor__rise' AND data_type='epoch' AND bin_width=1 AND bin_unit='day'
            GROUP BY srd) AS sr,
            (SELECT
              bin_timestamp - pmod(bin_timestamp + 28800, 86400) AS ssd,
              max(data_value) AS ssv
            FROM global_temp.datums
            WHERE
              data_metric='sun__outdoor__set' AND data_type='epoch' AND bin_width=1 AND bin_unit='day'
            GROUP BY ssd) AS ss,
            (SELECT
              bin_timestamp - pmod(bin_timestamp + 28800, 86400) AS szd,
              max(data_value) / first(data_scale) AS szv
            FROM global_temp.datums
            WHERE
              data_metric='sun__outdoor__azimuth' AND data_type='point' AND bin_width=2 AND bin_unit='second'
            GROUP BY szd) AS sz,
            (SELECT
              bin_timestamp - pmod(bin_timestamp + 28800, 86400) AS sad,
              max(data_value) / first(data_scale) AS sav
            FROM global_temp.datums
            WHERE
              data_metric='sun__outdoor__altitude' AND data_type='point' AND bin_width=2 AND bin_unit='second'
            GROUP BY sad) AS sa,
            (SELECT
              bin_timestamp - pmod(bin_timestamp + 28800, 86400) AS fcd,
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
            ep.epd != 1507737600 AND
            ep.epd != 1507824000 AND
            ep.epd != unix_timestamp() - pmod(unix_timestamp() + 28800, 86400)
          ORDER BY
            ep.epd ASC
        """
      )
      incrementCounter(RECORDS_OUT, outputProduction.count())
      outputProduction.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(outputPath)
      addResult(outputProduction.columns.mkString(","))
      outputProduction.collect.foreach(row => addResult(row.mkString(",")))
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
