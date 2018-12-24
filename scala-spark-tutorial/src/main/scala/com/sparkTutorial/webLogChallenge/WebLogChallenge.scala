package com.sparkTutorial.webLogChallenge

import java.text.SimpleDateFormat

import org.apache.spark.sql.expressions.Window
import java.sql.Timestamp
import java.util.regex.Pattern

import com.sparkTutorial.webLogChallenge.ELBAccessLog.{LOG_ENTRY_PATTERN, PATTERN}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import java.util.regex.Pattern
import util.control.Breaks._
import scala.util.control.Exception

object WebLogChallenge {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("collect").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val webLogRdd = sc.textFile("in/2015_07_22_mktplace_shop_web_log_sample.log")
    println("Count: " + webLogRdd.count())
    val webLogLines = webLogRdd.map(line => {
      ELBAccessLog.parseFromLogLine(line)
    }).cache()
    // test
    webLogLines.take(10000)
    val df = sparkSession.createDataFrame(webLogLines,
      StructType(
        Seq(
          StructField("timestamp", StringType,true),
          StructField("elbName", StringType,true),
          StructField("clientIpAddress", StringType,true),
          StructField("clientPort", StringType,true),
          StructField("backendIpAddress", StringType,true),
          StructField("backendPort", StringType,true),
          StructField("requestProcessingTime", StringType,true),
          StructField("backendProcessingTime", StringType,true),
          StructField("responseProcessingTime", StringType,true),
          StructField("elbStatusCode", StringType,true),
          StructField("backendStatusCode", StringType,true),
          StructField("receivedBytes", StringType,true),
          StructField("sentBytes", StringType,true),
          StructField("request_verb", StringType,true),
          StructField("request_url", StringType,true),
          StructField("request_protocol", StringType,true),
          StructField("userAgent", StringType,true),
          StructField("sslCipher", StringType,true),
          StructField("sslProtocol", StringType,true)
        )
      )
    )

    df.createOrReplaceTempView("userLog")
    val selectDF = sparkSession.sql("SELECT * FROM userLog ORDER BY timestamp DESC")
    selectDF.show(20, 0)
   /* val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
    val webLogLinesPair = webLogRdd.map(line => {
      val splits = line.split("\\s")
      Row(
        splits(2).split(":")(0),
        new Timestamp(format.parse(splits(0)).getTime),
        splits(3).split(":")(0)
      )
    })

    val df = sparkSession.createDataFrame(webLogLinesPair,
        StructType(Seq(StructField("userId", StringType,true),
                       StructField("timestamp", TimestampType,true),
                       StructField("uri", StringType,true))))

    //df.withColumn("timestamp", "timestamp".cast(LongType).cast(TimestampType))
    //println(df.count())
    //df.select("userId", "timestamp").write.mode(SaveMode.Append).format("parquet").save("out/userLog.parquet")
    //val parquetFileDF = sparkSession.read.parquet("out/userLog.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    df.createOrReplaceTempView("userLog")
    val namesDF = sparkSession.sql("SELECT userId, timestamp, targetIp, lag(timestamp) OVER (PARTITION BY userId ORDER BY timestamp) as lag_timestamp FROM (SELECT * FROM userLog WHERE userId ='123.242.248.130') ORDER BY timestamp")
    //namesDF.withColumn("difference", namesDF."timestamp")
    //namesDF.show(20, 0)
    val df2 = namesDF.withColumn("diff_in_secs", unix_timestamp(col("timestamp")) - unix_timestamp(col("lag_timestamp")))
    //df2.show(20, 0)

    val df3 = df2.withColumn("new_session", when(df2("diff_in_secs") >= 30*60, 1).otherwise(0))
   // df3.show(100, 0)
   // val hitByUsers = webLogLinesPair.groupByKey()
    val wSpec = Window.partitionBy("userId").orderBy("timestamp")
    val df4 = df3.withColumn("session_id", concat(concat(df3("userId"),lit("_")),(sum("new_session")).over(wSpec)))
    //df4.show(100, 0)
   // df4.select("userId", "timestamp", "targetIp","session_id").write.mode(SaveMode.Append).format("parquet").save("out/logs.parquet")
    val parquetFileDF = sparkSession.read.parquet("out/logs.parquet")
    // distinct ip
    val wSpec2 = Window.partitionBy("targetIp").orderBy("timestamp")
    val resDF = parquetFileDF
      .filter(parquetFileDF("session_id") === "123.242.248.130_1" || parquetFileDF("session_id") === "123.242.248.130_2")
      .withColumn("targetIpCount", row_number().over(wSpec2))
    val resDF2= resDF
        .withColumn("targetCountIp", when(resDF("targetIpCount") < 2,1).otherwise(0))
    //parquetFileDF.createOrReplaceTempView("logs")
    //val resDF = sparkSession.sql("SELECT *, row_number()  as ff from logs where session_id ='123.242.248.130_1' ")
          resDF2.show()
    val wSpec3 = Window
      .partitionBy("targetIp")
    val tt = resDF2
      .select(resDF2("session_id"),resDF2("targetIp"), sum(resDF2("targetCountIp")).over(wSpec3).alias("test")).distinct()
    tt.show()
    val ttt = tt.groupBy("session_id").sum("test")
    ttt.show()*/


    //val sample = hitByUsers.sample(withReplacement = true, fraction = 0.005)
    //for ((user, hits) <- sample.collectAsMap()) println(user + ": " + hits.toList)
    //webLogLinesPair.saveAsTextFile("out/logsBig")
  }
}

//case class Row(userId: String, timestamp: Timestamp)