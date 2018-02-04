package com.jag.asystem.astore

import com.cloudera.framework.common.Driver.{FAILURE_ARGUMENTS, FAILURE_RUNTIME, SUCCESS, getApplicationProperty}
import com.cloudera.framework.common.DriverSpark
import com.jag.asystem.astore.Check.OptionSnapshots
import com.jag.asystem.astore.Counter._
import com.jag.asystem.astore.Mode.Mode
import com.jag.asystem.astore.Type.Type
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.collection.mutable

class Check(configuration: Configuration) extends DriverSpark(configuration) {

  private var inputType: Type = _
  private var inputMode: Mode = _
  private var inputOutputPath: Path = _

  private var filesCount = Array.fill[Int](10)(0)

  private val filesStagedTodo = mutable.Map[(String, String), mutable.SortedSet[String]]()
  private val filesStagedSkip = mutable.Map[(String, String), mutable.SortedSet[String]]()

  private val filesProcessedTodo = mutable.Map[(String, String), mutable.SortedSet[String]]()
  private val filesProcessedSkip = mutable.Map[(String, String), mutable.SortedSet[String]]()

  private var dfs: FileSystem = _

  private val Log: Logger = LoggerFactory.getLogger(classOf[Check])

  //noinspection ScalaUnusedSymbol
  override def prepare(arguments: String*): Int = {
    if (arguments == null || arguments.length != parameters().length) return FAILURE_ARGUMENTS
    if (Type.contains(arguments(0))) {
      if (Log.isErrorEnabled()) Log.info("Driver [" + this.getClass.getSimpleName + "] prepared with invalid type [" + arguments(0) +
        "], should be one of [" + Type.values.mkString(", ") + "]")
      return FAILURE_ARGUMENTS
    }
    inputType = Type.get(arguments(0)).get
    if (Mode.contains(arguments(1))) {
      if (Log.isErrorEnabled()) Log.info("Driver [" + this.getClass.getSimpleName + "] prepared with invalid mode [" + arguments(1) +
        "], should be one of [" + Mode.values.mkString(", ") + "]")
      return FAILURE_ARGUMENTS
    }
    inputOutputPath = new Path(arguments(2))
    dfs = inputOutputPath.getFileSystem(getConf)
    inputOutputPath = dfs.makeQualified(inputOutputPath)
    val files = dfs.listFiles(inputOutputPath, true)
    while (files.hasNext) {
      val fileUri = files.next().getPath.toString
      val fileLabelPattern = ".*/([0-9])/asystem/astore/(.*)/canonical/(.*)/(.*)/(.*)/.*".r
      fileUri match {
        case fileLabelPattern(filePartition, fileLabel, fileFormat, fileEncoding, fileCodec) => fileLabel match {
          case "staged" => val filePartitionsPattern =
            "(.*)/arouter_version=(.*)/arouter_id=(.*)/arouter_ingest=(.*)/arouter_start=(.*)/arouter_finish=(.*)/arouter_model=(.*)/(.*)".r
            fileUri match {
              case filePartitionsPattern(fileRoot, arouterVersion, arouterId, arouterIngest, arouterStart, arouterFinish, arouterModel,
              fileName) =>
                try {
                  val fileParent = fileUri.replace(fileName, "")
                  val fileStartFinish = ("" + arouterStart.toLong, "" + arouterFinish.toLong)
                  if (fileName == "_SUCCESS") {
                    if (!filesStagedSkip.contains(fileStartFinish)) filesStagedSkip(fileStartFinish) = mutable.SortedSet()
                    filesStagedSkip(fileStartFinish) += fileParent
                    if (filesStagedTodo.contains(fileStartFinish)) filesStagedTodo(fileStartFinish) -= fileParent
                  }
                  else if (fileName.endsWith(".avro") && !fileName.startsWith(".")) {
                    if (!(filesStagedSkip.contains(fileStartFinish) && filesStagedSkip(fileStartFinish).contains(fileParent))) {
                      if (!filesStagedTodo.contains(fileStartFinish)) filesStagedTodo(fileStartFinish) = mutable.SortedSet()
                      filesStagedTodo(fileStartFinish) += fileParent
                    }
                  }
                  countFile(fileUri, filePartition)
                }
                catch {
                  case _: NumberFormatException => countFile(fileUri, filePartition, fileIgnore = true)
                }
              case _ => countFile(fileUri, filePartition, fileIgnore = true)
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
                    if (filesProcessedTodo.contains(fileYearMonth)) filesProcessedTodo(fileYearMonth) -= fileParent
                  }
                  else if (fileName.endsWith(".parquet") && !fileName.startsWith(".")) {
                    if (!(filesProcessedSkip.contains(fileYearMonth) && filesProcessedSkip(fileYearMonth).contains(fileParent))) {
                      if (!filesProcessedTodo.contains(fileYearMonth)) filesProcessedTodo(fileYearMonth) = mutable.SortedSet()
                      filesProcessedTodo(fileYearMonth) += fileParent
                    }
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
    if (Log.isInfoEnabled()) {
      Log.info("Driver [" + this.getClass.getSimpleName + "] prepared with input/output [" + inputOutputPath.toString + "] and input " +
        "files:")
      logFiles("PARTITIONS_STAGED_TODO", filesStagedTodo)
      logFiles("PARTITIONS_STAGED_SKIP", filesStagedSkip)
      logFiles("PARTITIONS_PROCESSED_TODO", filesProcessedTodo)
      logFiles("PARTITIONS_PROCESSED_SKIP", filesProcessedSkip)
    }
    SUCCESS
  }

  override def parameters(): Array[String] = {
    Array("type", "mode", "uri")
  }

  //noinspection ScalaUnusedSymbol
  override def execute(): Int = {
    if (getApplicationProperty("APP_VERSION").endsWith("-SNAPSHOT") && !getConf.getBoolean(OptionSnapshots, false)) {
      if (Log.isErrorEnabled()) {
        Log.error("Driver [" + this.getClass.getSimpleName + "] SNAPSHOT version attempted in production, bailing out without executing")
      }
      return FAILURE_RUNTIME
    }
    incrementCounter(PARTITIONS_STAGED_SKIP, filesStagedSkip.foldLeft(0)(_ + _._2.size))
    incrementCounter(PARTITIONS_STAGED_TODO, filesStagedTodo.foldLeft(0)(_ + _._2.size))
    incrementCounter(PARTITIONS_PROCESSED_SKIP, filesProcessedSkip.foldLeft(0)(_ + _._2.size))
    incrementCounter(PARTITIONS_PROCESSED_TODO, filesProcessedTodo.foldLeft(0)(_ + _._2.size))
    SUCCESS
  }

  override def reset(): Unit = {
    filesCount = Array.fill[Int](10)(0)
    filesStagedTodo.clear
    filesStagedSkip.clear
    filesProcessedTodo.clear
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

}


object Type extends Enumeration {
  type Type = Value
  val ASTORE = Value

  def contains(string: String): Boolean = values.exists(_.toString.toLowerCase == string.toLowerCase)

  def get(string: String): Option[Value] = values.find(_.toString.toLowerCase == string.toLowerCase)
}

object Mode extends Enumeration {
  type Mode = Value
  val STATS, CLEAN, REPAIR = Value

  def contains(string: String): Boolean = values.exists(_.toString.toLowerCase == string.toLowerCase)

  def get(string: String): Option[Value] = values.find(_.toString.toLowerCase == string.toLowerCase)
}

object Check {

  val OptionSnapshots = "com.jag.allow.snapshots"

  def main(arguments: Array[String]): Unit = {
    new Check(null).runner(arguments: _*)
  }

}
