package com.traffic.counter

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("TrafficCounter")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
}

object SparkSessionWrapper extends SparkSessionWrapper {}
