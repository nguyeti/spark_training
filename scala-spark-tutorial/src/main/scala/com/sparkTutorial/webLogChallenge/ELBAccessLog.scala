package com.sparkTutorial.webLogChallenge

import java.io.Serializable
import java.util.logging.Level
import java.util.logging.Logger
import java.util.regex.Matcher
import java.util.regex.Pattern

import org.apache.spark.sql.Row


/*
 * timestamp 
 * elb 
 * client:port 
 * backend:port 
 * request_processing_time 
 * backend_processing_time 
 * response_processing_time 
 * elb_status_code 
 * backend_status_code 
 * received_bytes 
 * sent_bytes 
 * "request" 
 * "user_agent"
 * ssl_cipher 
 * ssl_protocol
 * 
 */
object ELBAccessLog {
  private val logger = Logger.getLogger("Access")
  private val LOG_ENTRY_PATTERN = "^(\\S+) (\\S+) (\\S+):(\\S+) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\" \"([^\"]*)\" ([A-Z0-9-]+) ([A-Za-z0-9.-]*)$"
 // private val LOG_ENTRY_PATTERN = "([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \\\"([^ ]*) ([^ ]*) (- |[^ ]*)\\\" \\\"([^\\\"]*)\\\" ([A-Z0-9-]+) ([A-Za-z0-9.-]*)"
  private val PATTERN = Pattern.compile(LOG_ENTRY_PATTERN)

  def parseFromLogLine(logline: String): Row = {
    val m = PATTERN.matcher(logline)

    println(logline)
    if (!m.find) {
      System.out.println("Cannot parse logline:  " + logline)
      logger.log(Level.ALL, "Cannot parse logline  " + logline)
      throw new RuntimeException("Error parsing logline")
    }

    /*println(m.group(1))
    println(m.group(2))
    println(m.group(3))
    println(m.group(4))
    println("target ip " + m.group(5))
    println("backend port " +m.group(6))
    println("requestProcessingTime " +m.group(7))
    println("backendProcessingTime " +m.group(8))
    println("responseProcessingTime " +m.group(9))
    println("elbStatusCode " +m.group(10))
    println("backendStatusCode " +m.group(11))
    println("receivedBytes " +m.group(12))
    println("sentBytes " +m.group(13))
    println("request " +m.group(14))
    println("request " +m.group(15))
    println("request " +m.group(16))
    println("userAgent " +m.group(17))
    println("sslCipher " +m.group(18))
    println("sslProtocol " +m.group(19))*/
    Row(
      m.group(1),
      m.group(2),
      m.group(3),
      m.group(4),
      m.group(5),
      m.group(6),
      m.group(7),
      m.group(8),
      m.group(9),
      m.group(10),
      m.group(11),
      m.group(12),
      m.group(13),
      m.group(14),
      m.group(15),
      m.group(16),
      m.group(17),
      m.group(18),
      m.group(19))
  }
}

class ELBAccessLog(var dateTimeString: String,
                   var elbName: String,
                   var clientIpAddress: String,
                   val clientPort: Int,
                   var backendIpAddress: String,
                   val backendPort: Int ,
                   val requestProcessingTime: Float,
                   val backendProcessingTime: Float,
                   val responseProcessingTime: Float,
                   val elbStatusCode: Float,
                   val backendStatusCode: Float,
                   val receivedBytes: Long,
                   val sentBytes: Long,
                   val request_verb: String,
                   val request_url: String,
                   val request_protocol: String,
                   val userAgent: String,
                   val sslCipher: String,
                   val sslProtocol: String) extends Serializable {
}