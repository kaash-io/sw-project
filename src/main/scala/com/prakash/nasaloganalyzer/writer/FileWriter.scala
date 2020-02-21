package com.prakash.nasaloganalyzer.writer

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Handle result Dataframe in file format. Currenlty it is just a place holder for actual handler.
  *
  * @param spark Spark Session
  */
class FileWriter(spark: SparkSession) {
  //TODO Implement File Writer here. For this exercise, output goes to Stdout
  def write(df: DataFrame): Boolean = {
    df.show(truncate = false)
    true
  }
}
