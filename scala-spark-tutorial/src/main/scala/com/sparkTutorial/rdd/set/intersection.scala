package com.sparkTutorial.rdd.set

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object intersection {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("take").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    val inputWords2 = List("car", "hadoop")
    val wordRdd = sc.parallelize(inputWords)
    val wordRdd2 = sc.parallelize(inputWords2)

    val intersection = wordRdd.intersection(wordRdd2).collect().foreach(println)
    //for(i <- union) println(i)

  }
}