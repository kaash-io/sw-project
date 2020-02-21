package com.prakash.nasaloganalyzer.exception

/**
  * Custom exception for invalid inputs
  *
  * @param message exception message
  * @param cause   cause
  */
case class AnalyzerInvalidInputException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause) {}
