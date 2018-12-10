package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */

    val conf = new SparkConf().setAppName("SumOfNumbersProblem").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/prime_nums.text")
    val primeNumbers = lines.flatMap(line => line.split("\\s+"))
    val validPrimeNumbers = primeNumbers.filter(number => !number.isEmpty)
    //validPrimeNumbers.collect().foreach(println)
    // validPrimeNumbers is a string RDD, if we sum like below it is going to concatenate all the numbers
    val validPrimeNumbersToInt = validPrimeNumbers.map(i => i.toInt)
    println("sum is: " + validPrimeNumbersToInt.reduce((x, y) => x + y))
  }
}
