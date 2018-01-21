package com.jag.asystem.astore

import java.util.Calendar

import com.cloudera.framework.common.Driver.{FAILURE_ARGUMENTS, SUCCESS, getApplicationProperty}
import com.cloudera.framework.common.DriverSpark
import com.databricks.spark.avro._
import com.jag.asystem.amodel.DatumFactory.getModelProperty
import com.jag.asystem.astore.Counter._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.collection.mutable

class Process(configuration: Configuration) extends DriverSpark(configuration) {

  private var inputOutputPath: Path = _

  private var filesCount = Array.fill[Int](10)(0)
  private val filesStagedTodo = mutable.Map[(String, String), mutable.SortedSet[String]]()
  private val filesStagedSkip = mutable.Map[(String, String), mutable.SortedSet[String]]()
  private val filesProcessedSets = mutable.Map[(String, String), mutable.SortedSet[String]]()
  private val filesProcessedTodo = mutable.Map[(String, String), mutable.SortedSet[String]]()
  private val filesProcessedRedo = mutable.Map[(String, String), mutable.SortedSet[String]]()
  private val filesProcessedSkip = mutable.Map[(String, String), mutable.SortedSet[String]]()

  private var dfs: FileSystem = _

  private val Log: Logger = LoggerFactory.getLogger(classOf[Process])

  //noinspection ScalaUnusedSymbol
  override def prepare(arguments: String*): Int = {
    if (arguments == null || arguments.length != parameters().length) return FAILURE_ARGUMENTS
    inputOutputPath = new Path(arguments(0))
    dfs = inputOutputPath.getFileSystem(getConf)
    inputOutputPath = dfs.makeQualified(inputOutputPath)
    val files = dfs.listFiles(inputOutputPath, true)
    while (files.hasNext) {
      val fileUri = files.next().getPath.toString
      val fileLabelPattern = ".*/([0-9])/asystem/astore/(.*)/canonical/(.*)/(.*)/(.*)/.*".r
      fileUri match {
        case fileLabelPattern(filePartition, fileLabel, fileFormat, fileEncoding, fileCodec) => fileLabel match {
          case "staged" => val filePartitionsPattern =
            ".*/arouter_version=(.*)/arouter_id=(.*)/arouter_ingest=(.*)/arouter_start=(.*)/arouter_finish=(.*)/arouter_model=(.*)/(.*)".r
            fileUri match {
              case filePartitionsPattern(arouterVersion, arouterId, arouterIngest, arouterStart, arouterFinish, arouterModel, fileName) =>
                try {
                  val fileParent = fileUri.replace(fileName, "")
                  val fileStartFinish = ("" + arouterStart.toLong, "" + arouterFinish.toLong)
                  if (fileName == "_SUCCESS") {
                    if (!filesStagedSkip.contains(fileStartFinish)) filesStagedSkip(fileStartFinish) = mutable.SortedSet()
                    filesStagedSkip(fileStartFinish) += fileParent
                    if (filesStagedTodo.contains(fileStartFinish)) filesStagedTodo(fileStartFinish) -= fileParent
                  }
                  else if (!(filesStagedSkip.contains(fileStartFinish) && filesStagedSkip(fileStartFinish).contains(fileParent))) {
                    if (!filesStagedTodo.contains(fileStartFinish)) filesStagedTodo(fileStartFinish) = mutable.SortedSet()
                    filesStagedTodo(fileStartFinish) += fileParent
                  }
                  countFile(fileUri, filePartition)
                }
                catch {
                  case _: NumberFormatException => countFile(fileUri, filePartition, fileIgnore = true)
                }
              case _ => countFile(fileUri, filePartition, fileIgnore = true)
            }
          case "processed" => val filePartitionsPattern =
            ".*/astore_version=(.*)/astore_year=(.*)/astore_month=(.*)/astore_model=(.*)/astore_metric=(.*)/(.*)".r
            fileUri match {
              case filePartitionsPattern(astoreVersion, astoreYear, astoreMonth, astoreModel, astoreMetric, fileName) =>
                try {
                  val fileParent = fileUri.replace(fileName, "")
                  val fileYearMonth = ("" + astoreYear.toLong, "" + astoreMonth.toLong)
                  if (fileName == "_SUCCESS") {
                    if (!filesProcessedSkip.contains(fileYearMonth)) filesProcessedSkip(fileYearMonth) = mutable.SortedSet()
                    filesProcessedSkip(fileYearMonth) += fileParent
                    if (filesProcessedRedo.contains(fileYearMonth)) filesProcessedRedo(fileYearMonth) -= fileParent
                  }
                  else if (!(filesProcessedSkip.contains(fileYearMonth) && filesProcessedSkip(fileYearMonth).contains(fileParent))) {
                    if (!filesProcessedRedo.contains(fileYearMonth)) filesProcessedRedo(fileYearMonth) = mutable.SortedSet()
                    filesProcessedRedo(fileYearMonth) += fileParent
                  }
                }
                catch {
                  case _: NumberFormatException => countFile(fileUri, filePartition, fileIgnore = true)
                }
              case _ => countFile(fileUri, filePartition, fileIgnore = true)
            }
          case _ => countFile(fileUri, filePartition, fileIgnore = true)
        }
        case _ => countFile(fileUri, fileIgnore = true)
      }
    }
    for ((filesStagedTodoStartFinish, filesStagedTodoParents) <- filesStagedTodo) {
      if (filesStagedTodoParents.nonEmpty) {
        val start = Calendar.getInstance
        val finish = Calendar.getInstance
        start.setTimeInMillis(filesStagedTodoStartFinish._1.toLong * 1000)
        finish.setTimeInMillis(filesStagedTodoStartFinish._2.toLong * 1000)
        while (start.equals(finish) || start.before(finish)) {
          val fileYearMonth = (start.get(Calendar.YEAR).toString, (start.get(Calendar.MONTH) + 1).toString)
          if (!filesProcessedSets.contains(fileYearMonth)) filesProcessedSets(fileYearMonth) = mutable.SortedSet()
          filesProcessedSets(fileYearMonth) ++= filesStagedTodoParents
          start.add(Calendar.MONTH, 1)
        }
      }
    }
    for ((filesStagedSkipStartFinish, filesStagedSkipParents) <- filesStagedSkip) {
      if (filesStagedSkipParents.nonEmpty) {
        val start = Calendar.getInstance
        val finish = Calendar.getInstance
        start.setTimeInMillis(filesStagedSkipStartFinish._1.toLong * 1000)
        finish.setTimeInMillis(filesStagedSkipStartFinish._2.toLong * 1000)
        while (start.equals(finish) || start.before(finish)) {
          val fileYearMonth = (start.get(Calendar.YEAR).toString, (start.get(Calendar.MONTH) + 1).toString)
          if (filesProcessedSets.contains(fileYearMonth)) filesProcessedSets(fileYearMonth) ++= filesStagedSkipParents
          start.add(Calendar.MONTH, 1)
        }
      }
    }
    for ((filesProcessedSetsMonthYear, filesProcessedSetsParents) <- filesProcessedSets) {
      if (filesProcessedSetsParents.nonEmpty) {
        if (filesProcessedSkip.contains(filesProcessedSetsMonthYear)) {
          if (!filesProcessedRedo.contains(filesProcessedSetsMonthYear))
            filesProcessedRedo(filesProcessedSetsMonthYear) = mutable.SortedSet()
          filesProcessedRedo(filesProcessedSetsMonthYear) ++= filesProcessedSkip(filesProcessedSetsMonthYear)
          filesProcessedSkip.remove(filesProcessedSetsMonthYear)
        }
      }
    }
    filesProcessedTodo(("*", "*")) = mutable.SortedSet()
    for ((filesProcessedSetsMonthYear, filesProcessedSetsParents) <- filesProcessedSets)
      filesProcessedTodo(("*", "*")) ++= filesProcessedSetsParents
    if (Log.isInfoEnabled())
      Log.info("Driver [" + this.getClass.getSimpleName + "] prepared with input/output [" + inputOutputPath.toString + "]")
    if (Log.isDebugEnabled()) {
      Log.debug("Driver [" + this.getClass.getSimpleName + "] prepared with input files:")
      logFiles("FILES_STAGED_TODO", filesStagedTodo)
      logFiles("FILES_STAGED_SKIP", filesStagedSkip)
      logFiles("FILES_PROCESSED_SETS", filesProcessedSets)
      logFiles("FILES_PROCESSED_TODO", filesProcessedTodo)
      logFiles("FILES_PROCESSED_REDO", filesProcessedRedo)
      logFiles("FILES_PROCESSED_SKIP", filesProcessedSkip)
    }
    SUCCESS
  }

  override def parameters(): Array[String] = {
    Array("input-output-path")
  }

  //noinspection ScalaUnusedSymbol
  override def execute(): Int = {
    val spark = SparkSession.builder.config(new SparkConf).appName("asystem-astore-process").getOrCreate()
    import spark.implicits._
    for ((_, filesProcessedRedoParents) <- filesProcessedRedo)
      for (filesProcessedRedoParent <- filesProcessedRedoParents) dfs.delete(new Path(filesProcessedRedoParent), true)
    val fileModel = lit(getModelProperty("MODEL_VERSION"))
    val fileVersion = lit(getApplicationProperty("APP_VERSION"))
    val filesProcessedTodoRedo = filesProcessedSets.keySet.union(filesProcessedRedo.keySet)
    val filesProcessedPath = s"$inputOutputPath/${filesCount.zipWithIndex.min._2}/asystem/astore/processed/canonical/parquet/dict/snappy"
    if (filesProcessedTodo(("*", "*")).nonEmpty) filesProcessedTodo(("*", "*"))
      .map(filesProcessedTodoParent => spark.read.avro(filesProcessedTodoParent))
      .reduce((dataLeft: DataFrame, dataRight: DataFrame) => dataLeft.union(dataRight))
      .withColumn("astore_version", fileVersion)
      .withColumn("astore_year", year(to_date(from_unixtime($"bin_timestamp"))))
      .withColumn("astore_month", month(to_date(from_unixtime($"bin_timestamp"))))
      .withColumn("astore_model", fileModel)
      .withColumn("astore_metric", substring_index($"data_metric", "__", 1))
      .repartition(filesProcessedTodoRedo.size, $"astore_version", $"astore_year", $"astore_month", $"astore_model", $"astore_metric")
      .write.mode("append").partitionBy("astore_version", "astore_year", "astore_month", "astore_model", "astore_metric")
      .parquet(filesProcessedPath)
    val filesProcessedSuccess = new Path(filesProcessedPath, "_SUCCESS")
    val filesProcessedDone = mutable.Map[(String, String), mutable.SortedSet[String]]()
    val filesProcessedDoneFiles = mutable.Map[(String, String), mutable.SortedSet[String]]()
    if (dfs.exists(filesProcessedSuccess)) {
      dfs.delete(filesProcessedSuccess, true)
      for ((_, filesStagedTodoParents) <- filesStagedTodo)
        filesStagedTodoParents.foreach(filesStagedTodoParent => dfs.create(new Path(filesStagedTodoParent, "_SUCCESS")))
      val filesProcessed = dfs.listFiles(filesProcessedSuccess.getParent, true)
      while (filesProcessed.hasNext) {
        val fileUri = filesProcessed.next().getPath.toString
        val filePartitionsPattern = ".*/astore_year=(.*)/astore_month=(.*)/astore_model=(.*)/astore_metric=(.*)/(.*\\.parquet)".r
        fileUri match {
          case filePartitionsPattern(astoreYear, astoreMonth, astoreModel, astoreMetric, fileName) =>
            try {
              val fileParent = fileUri.replace(fileName, "")
              val fileMonthYear = (astoreYear, astoreMonth)
              if (filesProcessedTodoRedo.contains(fileMonthYear)) {
                dfs.create(new Path(fileParent, "_SUCCESS"))
                if (!filesProcessedDoneFiles.contains(fileMonthYear))
                  filesProcessedDoneFiles(fileMonthYear) = mutable.SortedSet()
                filesProcessedDoneFiles(fileMonthYear) += fileUri
                if (!filesProcessedDone.contains(fileMonthYear))
                  filesProcessedDone(fileMonthYear) = mutable.SortedSet()
                filesProcessedDone(fileMonthYear) += fileParent
              }
            }
            catch {
              case exception: Exception => countFile(fileUri, fileIgnore = true)
            }
          case _ => countFile(fileUri, fileIgnore = true)
        }
      }
    }
    if (Log.isDebugEnabled()) {
      logFiles("FILES_PROCESSED_DONE", filesProcessedDoneFiles)
    }
    incrementCounter(FILES_STAGED_SKIP, filesStagedTodo.foldLeft(0)(_ + _._2.size) +
      filesStagedSkip.foldLeft(0)(_ + _._2.size) - filesProcessedTodo.foldLeft(0)(_ + _._2.size))
    incrementCounter(FILES_STAGED_REDO, filesProcessedTodo.foldLeft(0)(_ + _._2.size) -
      filesStagedTodo.foldLeft(0)(_ + _._2.size))
    incrementCounter(FILES_STAGED_DONE, filesStagedTodo.foldLeft(0)(_ + _._2.size))
    incrementCounter(FILES_PROCESSED_SKIP, filesProcessedSkip.foldLeft(0)(_ + _._2.size))
    incrementCounter(FILES_PROCESSED_REDO, filesProcessedRedo.foldLeft(0)(_ + _._2.size))
    incrementCounter(FILES_PROCESSED_DONE, filesProcessedDone.foldLeft(0)(_ + _._2.size))
    spark.close()
    SUCCESS
  }

  override def reset(): Unit = {
    filesCount = Array.fill[Int](10)(0)
    filesStagedTodo.clear
    filesStagedSkip.clear
    filesProcessedSets.clear
    filesProcessedTodo.clear
    filesProcessedRedo.clear
    filesProcessedSkip.clear
    super.reset()
  }

  override def cleanup(): Int = {
    if (dfs != null) dfs.close()
    SUCCESS
  }

  private def countFile(fileUri: String, filePartition: String = null, fileIgnore: Boolean = false): Unit = {
    if (filePartition != null) filesCount(filePartition.toInt) += 1
    if (fileIgnore && Log.isWarnEnabled()) Log.warn("Driver [" + this.getClass.getSimpleName + "] ignoring [" + fileUri + "]")
  }

  private def logFiles(label: String, fileMaps: mutable.Map[(String, String), mutable.SortedSet[String]]) {
    Log.debug("  " + label + ":")
    if (fileMaps.isEmpty) Log.debug("") else for ((timeframes, uris) <- ListMap(fileMaps.map {
      case (timeframe, files) =>
        ((timeframe._1, if (timeframe._2.length == 1 && timeframe._2 != "*") "0" + timeframe._2 else timeframe._2), files)
    }.toSeq.sortBy(_._1): _*)) {
      if (uris.isEmpty) Log.debug("") else {
        Log.debug("    " + timeframes)
        for ((uri, count) <- uris.zipWithIndex) Log.info("      (" + (count + 1) + ") " + uri)
      }
    }
  }

}

object Process {

  def main(arguments: Array[String]): Unit = {
    new Process(null).runner(arguments: _*)
  }

}
