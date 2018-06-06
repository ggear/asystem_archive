package com.jag.asystem.astore

import java.util
import java.util.Calendar

import com.cloudera.framework.common.Driver._
import com.cloudera.framework.common.{Driver, DriverSpark}
import com.databricks.spark.avro._
import com.jag.asystem.amodel.DatumFactory.getModelProperty
import com.jag.asystem.astore.Counter._
import com.jag.asystem.astore.Mode.{BATCH, CLEAN, Mode, REPAIR}
import com.jag.asystem.astore.Process.{OptionSnapshots, TempTimeoutMs}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.Instant
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.util.Random

class Process(config: Configuration) extends DriverSpark(config) {

  private var inputMode: Mode = _
  private var inputOutputPath: Path = _

  private var timestampStart: Instant = _

  private var filesCount = Array.fill[Int](10)(0)

  private val filesStageFail = mutable.SortedSet[String]()
  private val filesStageTemp = mutable.SortedSet[String]()
  private val filesStagePure = mutable.SortedSet[String]()

  private val filesStagedTodo = mutable.Map[(String, String), mutable.SortedSet[String]]()
  private val filesStagedSkip = mutable.Map[(String, String), mutable.SortedSet[String]]()

  private val filesProcessedFail = mutable.SortedSet[String]()
  private val filesProcessedPure = mutable.SortedSet[String]()

  private val filesProcessedSets = mutable.Map[(String, String), mutable.SortedSet[String]]()
  private val filesProcessedTodo = mutable.Map[(String, String), mutable.SortedSet[String]]()
  private val filesProcessedRedo = mutable.Map[(String, String), mutable.SortedSet[String]]()
  private val filesProcessedSkip = mutable.Map[(String, String), mutable.SortedSet[String]]()
  private val filesProcessedDone = mutable.Map[(String, String), mutable.SortedSet[String]]()

  private val filesProcessedYear = mutable.Map[String, Int]().withDefaultValue(0)
  private val filesProcessedTops = mutable.Map[String, Int]().withDefaultValue(0)

  private var dfs: FileSystem = _

  private val Log: Logger = LoggerFactory.getLogger(classOf[Process])

  //noinspection ScalaUnusedSymbol
  override def prepare(arguments: String*): Int = {
    var exit = SUCCESS
    timestampStart = Instant.now
    try {
      if (arguments == null || arguments.length != parameters().length) exit = FAILURE_ARGUMENTS else {
        if (!Mode.contains(arguments(0))) {
          if (Log.isErrorEnabled()) Log.error("Driver [" + this.getClass.getSimpleName + "] prepared with invalid mode [" + arguments(0) +
            "], should be one of [" + Mode.values.mkString(", ") + "]")
          exit = FAILURE_ARGUMENTS
        }
        else {
          inputMode = Mode.get(arguments(0)).get
          inputOutputPath = new Path(arguments(1))
          dfs = inputOutputPath.getFileSystem(getConf)
          inputOutputPath = dfs.makeQualified(inputOutputPath)
          val files = dfs.listFiles(inputOutputPath, true)
          while (files.hasNext) {
            val fileUri = files.next().getPath.toString
            val fileLabelPattern = ".*/([0-9])/asystem/astore/(.*)/canonical/(.*)/(.*)/(.*)/.*".r
            fileUri match {
              case fileLabelPattern(filePartition, fileLabel, fileFormat, fileEncoding, fileCodec) => fileLabel match {
                case "staged" => val filePartitionsPattern = ("(.*)/arouter_version=(.*)/arouter_id=(.*)" +
                  "/arouter_ingest=(.*)/arouter_start=(.*)/arouter_finish=(.*)/arouter_model=(.*)/(.*)").r
                  fileUri match {
                    case filePartitionsPattern(fileRoot, arouterVersion, arouterId, arouterIngest,
                    arouterStart, arouterFinish, arouterModel, fileName) =>
                      try {
                        val fileParent = fileUri.replace(fileName, "")
                        val fileStartFinish = ("" + arouterStart.toLong, "" + arouterFinish.toLong)
                        if (fileName == "_SUCCESS") {
                          if (!filesStagedSkip.contains(fileStartFinish)) filesStagedSkip(fileStartFinish) = mutable.SortedSet()
                          filesStagedSkip(fileStartFinish) += fileParent
                          if (filesStagedTodo.contains(fileStartFinish)) filesStagedTodo(fileStartFinish) -= fileParent
                        } else if (fileName.endsWith(".avro") && !fileName.startsWith(".")) {
                          if (!(filesStagedSkip.contains(fileStartFinish) && filesStagedSkip(fileStartFinish).contains(fileParent))) {
                            if (!filesStagedTodo.contains(fileStartFinish)) filesStagedTodo(fileStartFinish) = mutable.SortedSet()
                            filesStagedTodo(fileStartFinish) += fileParent
                          }
                          countFile(filesStagePure, fileUri, filePartition)
                        } else if (fileName.endsWith(".avro.tmp") && fileName.startsWith("."))
                          countFile(filesStageTemp, fileUri, filePartition) else countFile(filesStageFail, fileUri, filePartition)
                      } catch {
                        case _: NumberFormatException => countFile(filesStageFail, fileUri, filePartition, fileIgnore = true)
                      }
                    case _ => countFile(filesStageFail, fileUri, filePartition, fileIgnore = true)
                  }
                case "processed" => val filePartitionsPattern =
                  "(.*)/astore_version=(.*)/astore_year=(.*)/astore_month=(.*)/astore_model=(.*)/astore_metric=(.*)/(.*)".r
                  fileUri match {
                    case filePartitionsPattern(fileRoot, astoreVersion, astoreYear, astoreMonth, astoreModel, astoreMetric, fileName) =>
                      try {
                        val fileParent = fileUri.replace(fileName, "")
                        val fileYearMonth = ("" + astoreYear.toLong, "" + astoreMonth.toLong)
                        if (fileName == "_SUCCESS") {
                          if (!filesProcessedSkip.contains(fileYearMonth)) filesProcessedSkip(fileYearMonth) = mutable.SortedSet()
                          filesProcessedSkip(fileYearMonth) += fileParent
                          if (filesProcessedRedo.contains(fileYearMonth)) filesProcessedRedo(fileYearMonth) -= fileParent
                        } else if (fileName.endsWith(".parquet") && !fileName.startsWith("_")) {
                          if (!(filesProcessedSkip.contains(fileYearMonth) && filesProcessedSkip(fileYearMonth).contains(fileParent))) {
                            if (!filesProcessedRedo.contains(fileYearMonth)) filesProcessedRedo(fileYearMonth) = mutable.SortedSet()
                            filesProcessedRedo(fileYearMonth) += fileParent
                          }
                          val fileYear = new Path(fileParent).getParent.getParent.getParent
                          filesProcessedYear(fileYear.toString) += 1
                          filesProcessedTops(fileYear.getParent.toString) += 1
                          countFile(filesProcessedPure, fileUri, filePartition)
                        } else countFile(filesProcessedFail, fileUri, filePartition)
                      } catch {
                        case _: NumberFormatException => countFile(filesProcessedFail, fileUri, filePartition, fileIgnore = true)
                      }
                    case _ => countFile(filesProcessedFail, fileUri, filePartition, fileIgnore = true)
                  }
                case _ => countFile(filesStageFail, fileUri, filePartition, fileIgnore = true)
              }
              case _ => countFile(filesStageFail, fileUri, fileIgnore = true)
            }
          }
          filesStageFail.union(filesProcessedFail).union(filesStageTemp).foreach(fileUri => {
            val fileParent = new Path(fileUri).getParent.toString + "/"
            filesStagedSkip.foreach(_._2.remove(fileParent))
            filesStagedTodo.foreach(_._2.remove(fileParent))
            filesProcessedSkip.foreach(_._2.remove(fileParent))
            filesProcessedRedo.foreach(_._2.remove(fileParent))
          })
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
                if (filesProcessedSets.contains(fileYearMonth) || !filesProcessedSkip.contains(fileYearMonth)) {
                  if (!filesProcessedSets.contains(fileYearMonth)) filesProcessedSets(fileYearMonth) = mutable.SortedSet()
                  filesProcessedSets(fileYearMonth) ++= filesStagedSkipParents
                }
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
        }
      }
    } catch {
      case exception: Exception =>
        exit = FAILURE_RUNTIME
        if (Log.isErrorEnabled()) Log.error("Driver [" + this.getClass.getSimpleName + "] failed during preparation", exception)
    } finally {
      if (Log.isInfoEnabled()) {
        Log.info("Driver [" + this.getClass.getSimpleName + "] prepared with mode [" +
          (if (inputMode == null) "null" else inputMode.toString.toLowerCase()) + "], input ["
          + inputOutputPath + "] and inputs:")
        logFiles("STAGED_FILES_FAIL", mutable.Map(("*", "*") -> filesStageFail))
        logFiles("STAGED_FILES_TEMP", mutable.Map(("*", "*") -> filesStageTemp))
        logFiles("STAGED_FILES_PURE", mutable.Map(("*", "*") -> filesStagePure))
        logFiles("STAGED_PARTITIONS_TEMP", mutable.Map(("*", "*") -> filesStageTemp))
        logFiles("STAGED_PARTITIONS_TODO", filesStagedTodo)
        logFiles("STAGED_PARTITIONS_SKIP", filesStagedSkip)
        logFiles("PROCESSED_FILES_FAIL", mutable.Map(("*", "*") -> filesProcessedFail))
        logFiles("PROCESSED_FILES_PURE", mutable.Map(("*", "*") -> filesProcessedPure))
        logFiles("PROCESSED_PARTITIONS_SETS", filesProcessedSets)
        logFiles("PROCESSED_PARTITIONS_EXEC", filesProcessedTodo)
        logFiles("PROCESSED_PARTITIONS_REDO", filesProcessedRedo)
        logFiles("PROCESSED_PARTITIONS_SKIP", filesProcessedSkip)
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
    Array("uri", "mode")
  }

  //noinspection ScalaUnusedSymbol
  override def execute(): Int = {
    var exit = SUCCESS
    try {
      val spark = SparkSession.builder.config(new SparkConf).appName(
        getConf.get(CONF_CLDR_JOB_NAME, "asystem-astore-process-" + inputMode.toString.toLowerCase)).getOrCreate()
      import spark.implicits._
      inputMode match {
        case CLEAN =>
          filesProcessedTodo.flatMap(_._2).toSet.union(filesProcessedSkip.flatMap(_._2).toSet).foreach(fileProcessed =>
            dfs.delete(new Path(fileProcessed), true))
        case REPAIR =>
          filesStageTemp.foreach(fileStagedTemp => {
            var fileSuccess = false
            val filePath = new Path(fileStagedTemp)
            var filePathSansTemp = new Path(filePath.getParent, filePath.getName.slice(1, filePath.getName.length - 4)

              // TODO: Remove for demo
              // .replace(".avro", "_%06d.avro".format(Random.nextInt(1000000)))

            )
            dfs.rename(filePath, filePathSansTemp)
            if (dfs.exists(new Path(filePath.getParent, "_SUCCESS"))) dfs.delete(new Path(filePath.getParent, "_SUCCESS"), true)
          })
        case BATCH =>
          val filesProcessedMonths = mutable.Set[Path]()
          for ((_, filesProcessedRedoParents) <- filesProcessedRedo) for (filesProcessedRedoParent <- filesProcessedRedoParents) {
            val filesProcessedMonth = new Path(filesProcessedRedoParent).getParent.getParent
            filesProcessedMonths += filesProcessedMonth
            filesProcessedYear(filesProcessedMonth.getParent.toString) -= 1
            filesProcessedTops(filesProcessedMonth.getParent.getParent.toString) -= 1
          }
          filesProcessedMonths.foreach(dfs.delete(_, true))
          for ((filesProcessedYear, filesProcessedYearCount) <- filesProcessedYear)
            if (filesProcessedYearCount == 0) dfs.delete(new Path(filesProcessedYear), true)
          for ((filesProcessedVersion, filesProcessedVersionCount) <- filesProcessedTops)
            if (filesProcessedVersionCount == 0) dfs.delete(new Path(filesProcessedVersion), true)
          val fileModel = lit(getModelProperty("MODEL_VERSION"))
          val fileVersion = lit(getApplicationProperty("APP_VERSION"))
          val filesProcessedTodoRedo = filesProcessedSets.keySet.union(filesProcessedRedo.keySet)
          val filesProcessedPath = s"$inputOutputPath/${filesCount.zipWithIndex.min._2}" +
            s"/asystem/astore/processed/canonical/parquet/dict/snappy"
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
          if (dfs.exists(filesProcessedSuccess)) {
            dfs.delete(filesProcessedSuccess, true)
            for ((_, filesStagedTodoParents) <- filesStagedTodo)
              filesStagedTodoParents.foreach(filesStagedTodoParent => touchFile(new Path(filesStagedTodoParent, "_SUCCESS")))
            val filesProcessed = dfs.listFiles(filesProcessedSuccess.getParent, true)
            while (filesProcessed.hasNext) {
              val fileUri = filesProcessed.next().getPath.toString
              val filePartitionsPattern = ".*/astore_year=(.*)/astore_month=(.*)/astore_model=(.*)/astore_metric=(.*)/(.*\\.parquet)".r
              fileUri match {
                case filePartitionsPattern(astoreYear, astoreMonth, astoreModel, astoreMetric, fileName) =>
                  val fileParent = fileUri.replace(fileName, "")
                  val fileMonthYear = (astoreYear, astoreMonth)
                  if (filesProcessedTodoRedo.contains(fileMonthYear)) {
                    touchFile(new Path(fileParent, "_SUCCESS"))
                    if (!filesProcessedDone.contains(fileMonthYear))
                      filesProcessedDone(fileMonthYear) = mutable.SortedSet()
                    filesProcessedDone(fileMonthYear) += fileParent
                  }
              }
            }
          }
          if (Log.isInfoEnabled()) {
            logFiles("PROCESSED_PARTITIONS_DONE", filesProcessedDone)
          }
        case _ =>
      }
      spark.close()
    } catch {
      case exception: Exception =>
        exit = FAILURE_RUNTIME
        if (Log.isErrorEnabled()) Log.error("Driver [" + this.getClass.getSimpleName + "] failed during execution", exception)
    } finally {
      val metaData = getMetaData(exit, timestampStart)
      addMetaDataCounter(metaData, STAGED_FILES_FAIL,
        filesStageFail.size)
      addMetaDataCounter(metaData, STAGED_FILES_TEMP,
        filesStageTemp.size)
      addMetaDataCounter(metaData, STAGED_FILES_PURE,
        filesStagePure.size)
      addMetaDataCounter(metaData, STAGED_PARTITIONS_TEMP,
        filesStageTemp.size)
      addMetaDataCounter(metaData, STAGED_PARTITIONS_SKIP, filesStagedTodo.foldLeft(0)(_ + _._2.size) +
        filesStagedSkip.foldLeft(0)(_ + _._2.size) - filesProcessedTodo.foldLeft(0)(_ + _._2.size))
      addMetaDataCounter(metaData, STAGED_PARTITIONS_REDO, filesProcessedTodo.foldLeft(0)(_ + _._2.size) -
        filesStagedTodo.foldLeft(0)(_ + _._2.size))
      addMetaDataCounter(metaData, STAGED_PARTITIONS_DONE,
        filesStagedTodo.foldLeft(0)(_ + _._2.size))
      addMetaDataCounter(metaData, PROCESSED_FILES_FAIL,
        filesProcessedFail.size)
      addMetaDataCounter(metaData, PROCESSED_FILES_PURE,
        filesProcessedPure.size)
      addMetaDataCounter(metaData, PROCESSED_PARTITIONS_SKIP,
        filesProcessedSkip.foldLeft(0)(_ + _._2.size))
      addMetaDataCounter(metaData, PROCESSED_PARTITIONS_REDO,
        filesProcessedRedo.foldLeft(0)(_ + _._2.size))
      addMetaDataCounter(metaData, PROCESSED_PARTITIONS_DONE,
        filesProcessedDone.foldLeft(0)(_ + _._2.size))
      pushMetaData(metaData)
      if (Log.isInfoEnabled()) Log.info("Driver [" + this.getClass.getSimpleName + "] execute metadata: " +
        pullMetaData(metaData).mkString(" "))
    }
    exit
  }

  override def reset(): Unit = {
    filesCount = Array.fill[Int](10)(0)
    filesStageFail.clear
    filesStageTemp.clear
    filesStagePure.clear
    filesStagedTodo.clear
    filesStagedSkip.clear
    filesProcessedFail.clear
    filesProcessedPure.clear
    filesProcessedSets.clear
    filesProcessedTodo.clear
    filesProcessedRedo.clear
    filesProcessedSkip.clear
    filesProcessedDone.clear
    filesProcessedYear.clear
    filesProcessedTops.clear
    super.reset()
  }

  override def cleanup(): Int = {
    if (dfs != null) dfs.close()
    SUCCESS
  }


  def getMetaData(exit: Integer, started: Instant = Instant.now, ended: Instant = Instant.now): Execution = {
    val metaData = new Execution(getConf, new Template(getConf), exit, started, ended)
    if (getConf.get(Process.OptionTags) != null) metaData.addTags(getConf.get(Process.OptionTags).split(",").map(_.trim).toList)
    metaData
  }

  private def countFile(fileStore: mutable.SortedSet[String], fileUri: String,
                        filePartition: String = null, fileIgnore: Boolean = false): Unit = {
    fileStore += fileUri
    if (filePartition != null) filesCount(filePartition.toInt) += 1
    if (fileIgnore && Log.isWarnEnabled()) Log.warn("Driver [" + this.getClass.getSimpleName + "] ignoring [" + fileUri + "]")
  }


  private def logFiles(label: String, fileMaps: mutable.Map[(String, String), mutable.SortedSet[String]]) {
    Log.info("  " + label + ":")
    if (fileMaps.nonEmpty) for ((timeframes, uris) <- ListMap(fileMaps.map {
      case (timeframe, files) =>
        ((timeframe._1, if (timeframe._2.length == 1 && timeframe._2 != "*") "0" + timeframe._2 else timeframe._2), files)
    }.toSeq.sortBy(_._1): _*)) {
      if (uris.nonEmpty) {
        Log.info("    " + timeframes)
        for ((uri, count) <- uris.zipWithIndex) Log.info("      (" + (count + 1) + ") " + uri)
      }
    }
  }

  private def touchFile(path: Path): Unit = {
    val os = dfs.create(path)
    os.write("".getBytes)
    os.close()
  }

}

object Mode extends Enumeration {

  type Mode = Value
  val STATS, CLEAN, REPAIR, BATCH = Value

  def contains(string: String): Boolean = values.exists(_.toString.toLowerCase == string.toLowerCase)

  def get(string: String): Option[Value] = values.find(_.toString.toLowerCase == string.toLowerCase)

}

object Process {

  val OptionTags = "com.jag.metadata.tags"
  val OptionSnapshots = "com.jag.allow.snapshots"

  val TempTimeoutMs: Int = 60 * 60 * 1000

  def main(arguments: Array[String]): Unit = {
    new Process(null).runner(arguments: _*)
  }

}
