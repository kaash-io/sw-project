package com.prakash.nasaloganalyzer.model

/**
  * Models file properties downloaded from URL
  *
  * @param url        url of log file if applicable
  * @param filePath   local file path (relative or full)
  * @param delimiter  delimiter character e.g. ','
  * @param fileType   type of file e.g. csv, json
  * @param outputPath output path where transformed file need to be saved.
  */
case class LogFile(url: String,
                   filePath: String,
                   delimiter: String,
                   fileType: String = "csv",
                   outputPath: String = null
                  ) extends ProductMeta
