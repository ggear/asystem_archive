package com.jag.asystem.astore

import java.util.UUID

import com.cloudera.framework.common.Driver.{FAILURE_ARGUMENTS, FAILURE_RUNTIME, SUCCESS, getApplicationProperty}
import com.cloudera.framework.common.DriverSpark
import com.databricks.spark.avro._
import com.jag.asystem.amodel.DatumFactory.getModelProperty
import com.jag.asystem.astore.Counter._
import com.jag.asystem.astore.Rewrite.{BATCH_WINDOW_SECONDS, PATH_PREFIX_MAX}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.util.Random

class Rewrite(configuration: Configuration) extends DriverSpark(configuration) {

  private var inputOutputPath: Path = _

  private val filesStagedTodo = mutable.SortedSet[String]()
  private val filesStagedSkip = mutable.SortedSet[String]()
  private val filesStagedRoot = mutable.SortedSet[String]()

  private var dfs: FileSystem = _

  private val Log: Logger = LoggerFactory.getLogger(classOf[Process])

  override def prepare(arguments: String*): Int = {
    if (arguments == null || arguments.length != parameters().length) return FAILURE_ARGUMENTS
    inputOutputPath = new Path(arguments(0))
    dfs = inputOutputPath.getFileSystem(getConf)
    inputOutputPath = dfs.makeQualified(inputOutputPath)
    val files = dfs.listFiles(inputOutputPath, true)
    while (files.hasNext) {
      val fileUri = files.next().getPath.toString
      val fileRewritePattern = "(.*/[0-9]/asystem/[1-9][0-9]\\.[0-9][0-9][0-9]\\.[0-9][0-9][0-9][0-9]/).*\\.avro".r
      fileUri match {
        case fileRewritePattern(fileRoot) =>
          filesStagedTodo += new Path(fileUri).getParent.toString
          filesStagedRoot += fileRoot
        case _ => filesStagedSkip += new Path(fileUri).getParent.toString
      }
    }
    if (Log.isInfoEnabled())
      Log.info("Driver [" + this.getClass.getSimpleName + "] prepared with input/output [" + inputOutputPath.toString + "]")
    if (Log.isDebugEnabled()) {
      Log.debug("Driver [" + this.getClass.getSimpleName + "] prepared with input files:")
      logFiles("FILES_STAGED_TODO", filesStagedTodo)
      logFiles("FILES_STAGED_SKIP", filesStagedSkip)
    }
    SUCCESS
  }

  override def parameters(): Array[String] = {
    Array("input-output-path")
  }

  override def execute(): Int = {
    val spark = SparkSession.builder.config(new SparkConf).appName("asystem-astore-process").getOrCreate()
    import spark.implicits._
    spark.conf.set("spark.sql.avro.compression.codec", "snappy")
    incrementCounter(FILES_STAGED_SKIP, filesStagedSkip.size)
    val fileModel = lit(getModelProperty("MODEL_VERSION"))
    val fileVersion = lit(getApplicationProperty("APP_VERSION"))
    val files = filesStagedTodo.toArray
    val filesBatchBase = files.length / PATH_PREFIX_MAX
    var filesBatchModulusCount = 0
    val filesBatchModulusIndexes = new Array[Int](PATH_PREFIX_MAX)
    while (filesBatchModulusIndexes.sum < files.length % PATH_PREFIX_MAX) {
      val index = Random.nextInt(PATH_PREFIX_MAX)
      filesBatchModulusIndexes(index) = if (filesBatchModulusIndexes(index) == 0) 1 else 0
    }
    for (filesPathPrefix <- 0 until PATH_PREFIX_MAX) {
      val filesBatchStart = filesBatchBase * filesPathPrefix + filesBatchModulusCount
      val filesBatchFinish = filesBatchBase * (filesPathPrefix + 1) + filesBatchModulusCount + filesBatchModulusIndexes(filesPathPrefix)
      filesBatchModulusCount += filesBatchModulusIndexes(filesPathPrefix)
      if (filesBatchStart != filesBatchFinish) {
        var filesStagedTodoBatch = files.slice(filesBatchStart, filesBatchFinish)
        val fileId = lit("spark-" + UUID.randomUUID().toString)
        val fileRewriteRoot = s"$inputOutputPath/$filesPathPrefix/asystem/astore/staged/canonical/avro/binary/snappy"
        if (filesStagedTodoBatch.nonEmpty) filesStagedTodoBatch
          .map(fileStagedTodo => spark.read.avro(fileStagedTodo))
          .reduce((dataLeft: DataFrame, dataRight: DataFrame) => dataLeft.union(dataRight))
          .withColumn("arouter_version", fileVersion)
          .withColumn("arouter_id", fileId)
          .withColumn("arouter_ingest", $"bin_timestamp" - $"bin_timestamp" % BATCH_WINDOW_SECONDS + BATCH_WINDOW_SECONDS)
          .withColumn("arouter_start", $"bin_timestamp" - $"bin_timestamp" % BATCH_WINDOW_SECONDS)
          .withColumn("arouter_finish", $"bin_timestamp" - $"bin_timestamp" % BATCH_WINDOW_SECONDS + BATCH_WINDOW_SECONDS)
          .withColumn("arouter_model", fileModel)
          .repartition(2, $"arouter_version", $"arouter_id", $"arouter_ingest", $"arouter_start", $"arouter_finish", $"arouter_model")
          .write.mode("append")
          .partitionBy("arouter_version", "arouter_id", "arouter_ingest", "arouter_start", "arouter_finish", "arouter_model")
          .avro(fileRewriteRoot)
        val fileSuccess = new Path(fileRewriteRoot, "_SUCCESS")
        if (dfs.exists(fileSuccess)) {
          dfs.delete(fileSuccess, true)
          incrementCounter(FILES_STAGED_DONE, filesStagedTodoBatch.size)
        } else {
          if (Log.isErrorEnabled()) Log.error("Driver [" + this.getClass.getSimpleName + "] failed during batch processing files [" +
            filesStagedTodoBatch.mkString(", ") + "]")
          return FAILURE_RUNTIME
        }
      }
    }
    for (fileStageRoot <- filesStagedRoot) dfs.delete(new Path(fileStageRoot), true)
    spark.close()
    SUCCESS
  }

  override def reset(): Unit = {
    filesStagedTodo.clear
    filesStagedSkip.clear
    super.reset()
  }

  override def cleanup(): Int = {
    if (dfs != null) dfs.close()
    SUCCESS
  }

  private def logFiles(label: String, files: mutable.SortedSet[String]) {
    Log.debug("  " + label + ":")
    if (files.isEmpty) Log.debug("") else for ((uri, count) <- files.zipWithIndex) Log.info("      (" + (count+1) + ") " + uri)
  }

}

object Rewrite {

  val PATH_PREFIX_MAX: Int = 10
  val BATCH_WINDOW_SECONDS: Int = 28800 * 2

  def main(arguments: Array[String]): Unit = {
    new Process(null).runner(arguments: _*)
  }

}
