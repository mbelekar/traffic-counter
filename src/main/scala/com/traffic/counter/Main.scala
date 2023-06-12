package com.traffic.counter

import org.apache.log4j.LogManager

object Main extends SparkSessionWrapper with App {
  val logger = LogManager.getLogger("TrafficCounterApp")

  private val maybeDs = Loader.createDataSet("src/main/resources/*.csv", logger)
  val ds = Loader.toEnrichedDS(maybeDs)

  if (ds.isEmpty) {
    logger.error("No data found. Check if your file path is correct or if file has valid data! ")
  } else {
    logger.warn("Total cars for all days:")
    Aggregator.totalCars(ds).show()

    logger.warn("Total cars by date:")
    Aggregator.totalCarsByDate(ds).show()

    logger.warn("Top 3 half hours with most cars:")
    Aggregator.topThreeHalfHoursWithMostCars(ds).show(30, truncate = false)

    logger.warn("1.5 hour period with least cars:")
    Aggregator.contiguousPeriodWithLeastCars(ds, "90 minutes").show(30, truncate = false)
  }

  spark.stop()
}
