package com.sparkTutorial.webLogChallenge

import java.util.logging.{Level, Logger}

import java.util.regex.Pattern

import org.apache.spark.sql.Row

object LogLine {
  private val logger = Logger.getLogger("Access")
  private val LOG_ENTRY_PATTERN = "^(\\S+) (\\S+) (\\S+):(\\S+) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\" \"([^\"]*)\" ([A-Z0-9-]+) ([A-Za-z0-9.-]*)$"
  private val PATTERN = Pattern.compile(LOG_ENTRY_PATTERN)

  def parseFromLogLine(logline: String): Row = {
    val m = PATTERN.matcher(logline)

//    println(logline)
    if (!m.find) {
      System.out.println("Cannot parse logline:  " + logline)
      logger.log(Level.ALL, "Cannot parse logline  " + logline)
      throw new RuntimeException("Error parsing logline")
    }

    Row(
      m.group(1), // timestamp
//      m.group(2), // elb name
      m.group(3), // client ip
//      m.group(4).toInt, // client port
//      if (m.group(5) == "") null else m.group(5), // target ip
//      if (m.group(6) == "") null else m.group(6), // target port
//      m.group(7).toFloat, // requestProcessingTime
//      m.group(8).toFloat, // backendProcessingTime
//      m.group(9).toFloat, // responseProcessingTime
//      m.group(10).toInt, // elbStatusCode
//      m.group(11).toInt, // backendStatusCode
//      m.group(12).toLong, // receivedBytes
//      m.group(13).toLong, // sentBytes
//      m.group(14), // request_verb
      m.group(15) // request_url
//      m.group(16), // request_protocol
//      m.group(17), // user agent
//      m.group(18), // ssl Cipher
//      m.group(19)) // ssl protocol
    )
  }
}
