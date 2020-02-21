package com.prakash.nasaloganalyzer.reader

import com.prakash.nasaloganalyzer.model.LogFile
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Reader class to read files
  *
  * @param spark Spark Session
  */
class FileReader(spark: SparkSession) extends ReaderTrait with Serializable {
  private val JSON = "json"
  private val CSV = "csv"
  private val SEP = "sep"

  /**
    * Reads file
    *
    * @param file log file object
    * @return spark Dataframe
    */
  def read(file: LogFile): DataFrame = {
    file.fileType match {
      case JSON => spark.read.option(SEP, file.delimiter).json(file.filePath)
      case CSV => spark.read.option(SEP, file.delimiter).csv(file.filePath)
    }
  }
}
