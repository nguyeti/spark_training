package com.sparkTutorial.webLogChallenge

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Main object class.
  * - Read and parse the log file and save the logs data in parquet.
  * - Read data from parquet and do some calculations
  */
object WebLogChallenge {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setAppName("collect")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", 2) // default is 200
      //      .config("spark.sql.inMemoryColumnarStorage.batchSize", 1500000)
      .getOrCreate()
    var parquetFileDF: DataFrame = null

    try {
      parquetFileDF = sparkSession.read.parquet("out/userLog.parquet")
    } catch {
      case ae: AnalysisException => {
        val webLogRdd = sc.textFile("in/2015_07_22_mktplace_shop_web_log_sample.log")
        //    val webLogRdd = sc.textFile("in/wrong_line.log")
        println("rdd Count: " + webLogRdd.count())

        val webLogLines = webLogRdd.map(line => {
          LogLine.parseFromLogLine(line)
        }).cache()

        val df = sparkSession.createDataFrame(webLogLines,
          StructType(
            Seq(
              StructField("timestampStr", StringType, true),
              //              StructField("elbName", StringType, true),
              StructField("clientIpAddress", StringType, true),
              //              StructField("clientPort", IntegerType, true),
              //              StructField("backendIpAddress", StringType, true),
              //              StructField("backendPort", StringType, true),
              //              StructField("requestProcessingTime", FloatType, true),
              //              StructField("backendProcessingTime", FloatType, true),
              //              StructField("responseProcessingTime", FloatType, true),
              //              StructField("elbStatusCode", IntegerType, true),
              //              StructField("backendStatusCode", IntegerType, true),
              //              StructField("receivedBytes", LongType, true),
              //              StructField("sentBytes", LongType, true),
              //              StructField("request_verb", StringType, true),
              StructField("request_url", StringType, true)
              //              StructField("request_protocol", StringType, true),
              //              StructField("userAgent", StringType, true),
              //              StructField("sslCipher", StringType, true),
              //              StructField("sslProtocol", StringType, true)
            )
          )
        ).cache()

        println("df count: " + df.count())
        // Save the DF in parquet
        df
          .select("timestampStr", "clientIpAddress", "request_url")
          .withColumn("timestamp", df("timestampStr").cast(TimestampType))
          .orderBy("clientIpAddress", "timestamp")
          .write.mode(SaveMode.Append).format("parquet").save("out/userLog.parquet")
      }
    }
    // Read the parquet file
    parquetFileDF = sparkSession
      .read
      .parquet("out/userLog.parquet")

    // Create a Window spec object
    val wSpec = Window
      .partitionBy("clientIpAddress")
      .orderBy("timestamp")

    // Create a new lag column in the DF
    val lagDF = parquetFileDF
      .withColumn("lag", lag("timestamp", 1).over(wSpec))

    // Calculate the difference in seconds between the timestamp and lag columns for each row
    val difference = unix_timestamp(col("timestamp")) - unix_timestamp(col("lag"))
    val differenceInSecDf = lagDF
      .withColumn("differenceInSecs", difference)
      .cache()

    // Create a flag for a new session
    val newSessionFlagDf = differenceInSecDf
      .withColumn("new_session", when(differenceInSecDf("differenceInSecs") >= 30 * 60, 1).otherwise(0))
      .cache()
    
    val wSpec2 = Window
      .partitionBy("clientIpAddress")
      .orderBy("timestamp")

    // Create a DF with session ids assigned
    val dfWithSessionId = newSessionFlagDf
      .withColumn("session_id", concat(concat(newSessionFlagDf("clientIpAddress"), lit("_")), (sum("new_session")).over(wSpec2)))
      .select("session_id", "timestamp", "clientIpAddress", "request_url", "differenceInSecs")
      .cache()

    // Calculate the total session duration per session id
    val wSpec3 = Window.partitionBy("session_id")
    val totalSessionTime = dfWithSessionId.withColumn("total_session_time", sum("differenceInSecs").over(wSpec3))
    totalSessionTime.cache()

    // select distinct total_session_time per session id
    val timeBySession = totalSessionTime
      .select("session_id", "clientIpAddress", "total_session_time")
      .distinct()
      .cache()

    // Overall average session time
    val avgSessionTime = timeBySession
      .select(avg("total_session_time"))
    //    avgSessionTime.explain()
    avgSessionTime.show()
    
    // Unique URL count per session
    val uniqueUrlPerSession = dfWithSessionId
      //      .filter(dfWithSessionId("clientIpAddress") === "123.242.248.130")
      .groupBy("session_id")
      .agg(countDistinct("request_url"))
      .show(100, 0)
    
    // Most engaged users
    val mostEngagedUsers = timeBySession
      .orderBy(desc("total_session_time"))
    mostEngagedUsers.show(10, 0)
  }
}
