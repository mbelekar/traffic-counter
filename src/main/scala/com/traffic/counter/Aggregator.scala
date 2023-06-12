package com.traffic.counter

import com.traffic.counter.SparkSessionWrapper.spark.implicits._
import com.traffic.counter.models.EnrichedTrafficData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object Aggregator extends SparkSessionWrapper {
  def totalCars(ds: Dataset[EnrichedTrafficData]): DataFrame = {
    ds.groupBy().agg(sum("numCars"))
      .select(col("sum(numCars)").as("Total Cars"))
  }

  def totalCarsByDate(ds: Dataset[EnrichedTrafficData]): DataFrame =
    ds.groupBy("date")
      .agg(sum("numCars"))
      .select(col("date").as("Date"), col("sum(numCars)").as("Number of Cars"))
      .orderBy("Date")

  def topThreeHalfHoursWithMostCars(ds: Dataset[EnrichedTrafficData]): Dataset[Row] =
    ds.groupBy("isoDate")
      .agg(sum("numCars"))
      .orderBy(col("sum(numCars)").desc)
      .select(col("isoDate").as("Date"), col("sum(numCars)").as("Number of Cars"))
      .limit(3)

  def contiguousPeriodWithLeastCars(ds: Dataset[EnrichedTrafficData], windowDuration: String): DataFrame = {
    ds.groupBy(window($"timestamp", windowDuration))
      .agg(sum("numCars"))
      .orderBy(col("sum(numCars)"))
      .select(col("window.start") as ("Window start"), col("window.end") as ("Window end"),
        col("sum(numCars)").as("Number of Cars"))
      .limit(1)
  }
}
