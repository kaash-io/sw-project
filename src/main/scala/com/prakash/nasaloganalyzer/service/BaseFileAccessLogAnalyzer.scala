package com.prakash.nasaloganalyzer.service

import com.prakash.nasaloganalyzer.model.LogFile
import com.prakash.nasaloganalyzer.reader.FileReader
import com.prakash.nasaloganalyzer.writer.FileWriter
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Base file based log analyzer
  *
  * @param spark Spark Session
  * @param file  file object
  */
abstract class BaseFileAccessLogAnalyzer(spark: SparkSession, file: LogFile)
  extends Serializable {

  /**
    * reads file
    *
    * @param spark spark session
    * @param file  file object
    * @return spark dataframe
    */
  def read(spark: SparkSession, file: LogFile): DataFrame = {
    val reader = new FileReader(spark)
    reader.read(file)
  }

  /**
    * Writes file
    *
    * @param spark Spark Session
    * @param df    Input Spark Dataframe
    * @return
    */
  def write(spark: SparkSession, df: DataFrame): Boolean = {
    val writer = new FileWriter(spark)
    writer.write(df)
  }

  /**
    * Analyze function to be overridden by subclass
    *
    * @param inputDF input Dataframe
    * @return Spark Dataframe
    */
  def analyze(inputDF: DataFrame): DataFrame

  /**
    * Processes all logic
    */
  def process(): Unit = {
    val inputDF = read(spark, file)
    val analyzedDF = analyze(inputDF)
    val result = write(spark, analyzedDF)
  }

}
