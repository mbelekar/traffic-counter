package com.traffic.counter

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Test")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
}

object SparkSessionTestWrapper extends SparkSessionTestWrapper {}