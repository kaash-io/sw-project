package com.prakash.nasaloganalyzer.service

import com.prakash.nasaloganalyzer.model.LogFile
import org.apache.spark.sql.functions.regexp_extract
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @param spark      Spark Session
  * @param file       file object
  * @param numRecords number of records to show //TODO there should be handler to handle to result data
  */
class NasaAccessLogAnalyzer(spark: SparkSession, file: LogFile, numRecords: Int = 5)
  extends BaseFileAccessLogAnalyzer(spark, file) with Serializable {

  import spark.implicits._

  /**
    * Overwrite of base class method
    *
    * @param inputDF input Dataframe
    * @return Spark Dataframe
    */
  override def analyze(inputDF: DataFrame): DataFrame = {
    // Parse and clean columns needed for analysis
    val cleanedDF = sanitizeDF(parseLogToDF(inputDF))
    combineHostAndUrl(getTopHosts(cleanedDF), getTopUrls(cleanedDF))
  }

  /**
    * Parse raw text file (gz) DataFrame to add needed columns
    *
    * @param df input Dataframe
    * @return Parsed DataFrame
    */
  def parseLogToDF(df: DataFrame): DataFrame = {
    // Parse
    df.select(
      // First set of non whitespace chars from beginning until first space
      regexp_extract($"_c0", """(^\S+)""", 1).alias("host"),
      // Capture timestamp e.g. [01/Jul/1995:00:00:15 -0400] in three groups. Take the second one which gives
      // month and year
      regexp_extract($"_c0", """\[(\d{2})/(\w{3}/\d{4})(:\d{2}:\d{2}:\d{2} -\d{4})]""", 2)
        .alias("timestamp"),
      // Capture URL in three groups and take the second one which gives string after GET
      regexp_extract($"_c0", """\"(\S+)\s(\S+)\s*(\S*)\"""", 2).alias("url")
    )
  }

  /**
    * Clean bad data //TODO this will likely change in real world scenario
    *
    * @param df Input DataFrame
    * @return Cleaned DataFrame
    */
  def sanitizeDF(df: DataFrame): DataFrame = {
    // Perform basic sanitization
    val dfTemp = df.filter(row => !row.anyNull) // remove null rows
    dfTemp.filter($"url" =!= "/") // remove URLs with just slash '/'
  }

  /**
    * Get top hosts
    *
    * @param df Input DataFrame
    * @return Result DataFrame
    */
  def getTopHosts(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("nasa") // Alias a view name
    spark.sql("SELECT host, count(*) as host_count FROM nasa GROUP BY host")
      .createOrReplaceTempView("nasa2")
    spark.sql("SELECT host, host_count, dense_rank() OVER (ORDER BY host_count desc ) " +
      "AS RK FROM nasa2").createOrReplaceTempView("nasa3")
    spark.sql(s"SELECT * FROM nasa3 WHERE RK <= $numRecords")
  }

  /**
    * Get top URLs //TODO it is very similar to top host logic. Should be change to have text binding capability so
    * same sql can be reused.
    *
    * @param df Input DataFrame
    * @return Result DataFrame
    */
  def getTopUrls(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("nasa") // Alias a view name
    spark.sql("SELECT url, count(*) as url_count FROM nasa GROUP BY url")
      .createOrReplaceTempView("nasa2")
    spark.sql("SELECT url, url_count, dense_rank() OVER (ORDER BY url_count desc ) " +
      "AS RK FROM nasa2").createOrReplaceTempView("nasa3")
    spark.sql(s"SELECT * FROM nasa3 WHERE RK <= $numRecords")
  }

  /**
    * Join host and url results
    *
    * @param hostDF Host DataFrame
    * @param urlDF  Url DataFrame
    * @return Result DataFrame
    */
  def combineHostAndUrl(hostDF: DataFrame, urlDF: DataFrame): DataFrame = {
    hostDF.createOrReplaceTempView("host")
    urlDF.createOrReplaceTempView("url")
    spark.sql("SELECT a.RK as rank, a.host, a.host_count, b.url, b.url_count FROM host a " +
      "JOIN url b ON a.RK=b.RK")
  }
}
