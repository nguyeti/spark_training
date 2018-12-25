package com.sparkTutorial.webLogChallenge

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}


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
          ELBAccessLog.parseFromLogLine(line)
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
        df
          .select("timestampStr", "clientIpAddress", "request_url")
          .withColumn("timestamp", df("timestampStr").cast(TimestampType))
          .orderBy("clientIpAddress", "timestamp").write.mode(SaveMode.Append).format("parquet").save("out/userLog.parquet")
      }
    }
    parquetFileDF = sparkSession.read.parquet("out/userLog.parquet")
    val w = Window.partitionBy("clientIpAddress").orderBy("timestamp")
    val lagDF = parquetFileDF.withColumn("lag", lag("timestamp", 1).over(w))
    lagDF
      .select("timestamp", "lag", "clientIpAddress", "request_url").cache()
    //            .show(100, false)

    val diff_in_sec_df = lagDF.withColumn("diff_in_secs", unix_timestamp(col("timestamp")) - unix_timestamp(col("lag"))).cache()
    //        diff_in_sec_df.show(20, 0)

    val new_session_flag_DF = diff_in_sec_df.withColumn("new_session", when(diff_in_sec_df("diff_in_secs") >= 30 * 60, 1).otherwise(0)).cache()
    //         new_session_flag_DF.show(10000, 0)
    //
    val wSpec = Window.partitionBy("clientIpAddress").orderBy("timestamp")
    val df4 = new_session_flag_DF
      .withColumn("session_id", concat(concat(new_session_flag_DF("clientIpAddress"), lit("_")), (sum("new_session")).over(wSpec)))
      .select("session_id", "timestamp", "clientIpAddress", "request_url", "diff_in_secs")
      .cache()

    val wSpec2 = Window.partitionBy("session_id")
    val avg_time_by_session = df4.withColumn("total_session_time", sum("diff_in_secs").over(wSpec2))
    avg_time_by_session.cache()

    val time_by_session = avg_time_by_session.select("session_id", "clientIpAddress", "total_session_time").distinct().cache()

    val avg_session_time = time_by_session.select(avg("total_session_time"))
    //    avg_session_time.explain()
    avg_session_time.show()

    val unique_url_per_session = df4
      //      .filter(df4("clientIpAddress") === "123.242.248.130")
      .groupBy("session_id")
      .agg(countDistinct("request_url"))
      .show(100, 0)
    val most_engaged_users = time_by_session.orderBy(desc("total_session_time"))
    most_engaged_users.show(10, 0)

  }
}
