package com.prakash.nasaloganalyzer.util

import java.io.File
import java.net.URL

import com.prakash.nasaloganalyzer.model.LogFile
import org.apache.commons.io.FileUtils

/**
  * Wrapper on apache common's FileUtil to download file directly from URL
  * //TODO there should be more option on how we want to handle the source data
  *
  * @param file file object
  */
class FileExtractor(file: LogFile) {
  private val CONNECTION_TIMEOUT = 1000
  private val READ_TIMEOUT = 30000

  /**
    * Download file from URL
    */
  def downloadFromUrl(): Unit = {
    val url = new URL(file.url)
    FileUtils.copyURLToFile(url, new File(
      file.filePath), CONNECTION_TIMEOUT, READ_TIMEOUT)
  }
}
