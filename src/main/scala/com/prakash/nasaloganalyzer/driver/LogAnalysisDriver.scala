package com.prakash.nasaloganalyzer.driver

import com.prakash.nasaloganalyzer.model.LogFile
import com.prakash.nasaloganalyzer.service.NasaAccessLogAnalyzer
import com.prakash.nasaloganalyzer.util.FileExtractor
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/**
  * Log analysis Driver. This is entry point for spark application.
  * TODO Currently its hardcoded for one type of processor. I would lke to make it subtype of common driver that
  * can map a processor string to processor type using ENUM.
  */
object LogAnalysisDriver {
  private val MIN_ARGS = 1
  private val DEFAULT_MASTER = "local" // For demo purpose keeping it local
  private val APP_NAME = "NASA Access Log Analyzer"
  private val DEFAULT_FILE_NAME = "nasa_access_log.gz"
  private val DEFAULT_URL = "ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"
  private val DEFAULT_NUM_RECORDS: Int = 5
  private val DEFAULT_DELIM = "\t"

  def main(args: Array[String]): Unit = {

    // TODO Use command line argument parser such as SCOPT to have flexibility of key value pair.
    val fullURLPath: String = if (args.length >= 1) args(0) else DEFAULT_URL
    val numRecords: Int = if (args.length >= 2) args(1).toInt else DEFAULT_NUM_RECORDS

    val spark = SparkSession
      .builder
      .master(DEFAULT_MASTER)
      .appName(APP_NAME)
      .getOrCreate()

    val logFile = LogFile(fullURLPath, DEFAULT_FILE_NAME, DEFAULT_DELIM)


    Try(processLogFile(spark, logFile, numRecords)) match {
      case Success(value) => println("Completed Successfully")
      case Failure(e) => {
        spark.close()
        println("Exception thrown " + e)
        throw e
      } // TODO Should retry here
    }

    spark.close()
  }

  /**
    * Process log files
    *
    * @param spark      Spark Session
    * @param file       Log File object
    * @param numRecords number of records to show
    */
  def processLogFile(spark: SparkSession, file: LogFile, numRecords: Int): Unit = {
    val fileExtractor = new FileExtractor(file)
    fileExtractor.downloadFromUrl() // TODO Have option to not load file either optionally or using flag
    val nasaAccessLogAnalyzer = new NasaAccessLogAnalyzer(spark, file, numRecords)
    nasaAccessLogAnalyzer.process()
  }
}
