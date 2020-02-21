package com.prakash.nasaloganalyzer.service

import com.prakash.nasaloganalyzer.model.LogFile
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.Matchers
import org.scalatest.funsuite.AnyFunSuite

class NasaAccessLogAnalyzerSpec extends AnyFunSuite with Matchers {

  private val DELIMITER = "\t"
  private val testDataPath = "test-data/mock_access_log.txt"
  private val default_records = 3

  //prepare
  private val spark = SparkSession
    .builder()
    .appName("com.prakash.nasaloganalyzer.service.NasaAccessLogAnalyzer")
    .master("local")
    .getOrCreate()

  val logFile = LogFile("mockUrl", testDataPath, DELIMITER)
  val inputDF: DataFrame = spark.read.option("sep", DELIMITER).csv(testDataPath)
  val target = new NasaAccessLogAnalyzer(spark, logFile, default_records)

  import spark.implicits._

  // tests

  test("parseLogToDF with valid arguments should succeed") {
    val parsedDF = target.parseLogToDF(inputDF)
    val actualColumns = parsedDF.columns.toSeq
    val expectedColumns = Seq("host", "timestamp", "url")
    val expectedCount = 28
    expectedColumns should contain theSameElementsAs actualColumns // column names should match
    assert(expectedCount == parsedDF.count()) // counts should match
  }

  test("sanitizeDF with valid arguments should succeed") {
    val sanitizedDF = target.sanitizeDF(target.parseLogToDF(inputDF)) //TODO parseLogToDF should be mocked for isolated test
    val expectedCountAfterSanitization = 27 // Count after bad data is removed
    assert(expectedCountAfterSanitization == sanitizedDF.count())
  }

  test("getTopHosts with valid arguments should succeed") {
    val targetClass = new NasaAccessLogAnalyzer(spark, logFile, 1) // new target class with 1 record
    val cleanedDF = targetClass.sanitizeDF(target.parseLogToDF(inputDF))
    val actualDF = targetClass.getTopHosts(cleanedDF)
    val expectedDF = Seq(("ppp-mia-30.shadow.net", 9, 1)).toDF("host", "host_count", "RK")
    actualDF.collect().toList should contain theSameElementsAs expectedDF.collect().toList
  }

  test("getTopUrls with valid arguments should succeed") {
    val targetClass = new NasaAccessLogAnalyzer(spark, logFile, 1) // new target class with 1 record
    val cleanedDF = targetClass.sanitizeDF(target.parseLogToDF(inputDF))
    val actualDF = targetClass.getTopUrls(cleanedDF)
    val expectedDF = Seq(
      ("/images/USA-logosmall.gif", 5, 1),
      ("/shuttle/missions/sts-71/sts-71-patch-small.gif", 5, 1)
    ).toDF("url", "url_count", "RK")
    actualDF.collect().toList should contain theSameElementsAs expectedDF.collect().toList
  }

  test("combineHostAndUrl with valid arguments should succeed") {
    val targetClass = new NasaAccessLogAnalyzer(spark, logFile, 1) // new target class with 1 record
    val cleanedDF = targetClass.sanitizeDF(target.parseLogToDF(inputDF))
    val topHostDF = targetClass.getTopHosts(cleanedDF)
    val topUrlDF = targetClass.getTopUrls(cleanedDF)
    val actualDF = targetClass.combineHostAndUrl(topHostDF, topUrlDF)
    val expectedDF = Seq(
      (1, "ppp-mia-30.shadow.net", 9, "/shuttle/missions/sts-71/sts-71-patch-small.gif", 5),
      (1, "ppp-mia-30.shadow.net", 9, "/images/USA-logosmall.gif", 5)
    ).toDF("rank", "host", "host_count", "url", "url_count")
    actualDF.collect().toList should contain theSameElementsAs expectedDF.collect().toList
  }

}
