package com.traffic.counter

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.traffic.counter.models.{EnrichedTrafficData, TrafficData}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.funspec.AnyFunSpec

import java.sql.{Date, Timestamp}
import scala.util.Try

class AggregatorSpec extends AnyFunSpec with SparkSessionTestWrapper with DatasetComparer {

  val logger: Logger = LogManager.getLogger("test")

  val sourceFile = "src/test/resources/data.csv"

  val maybeDs: Try[Dataset[TrafficData]] = Loader.createDataSet(sourceFile, logger)
  val ds: Dataset[EnrichedTrafficData] = Loader.toEnrichedDS(maybeDs)

  describe("Aggregate") {
    it("should return total cars for all days") {
      val actualDF = Aggregator.totalCars(ds).toDF()
      val expectedSchema = List(
        StructField("Total Cars", LongType, nullable = false)
      )
      val expectedData = Seq(Row(76.toLong))
      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )
      assertSmallDatasetEquality(actualDF, expectedDF, ignoreNullable = true)
    }

    it("should return total cars by date") {
      val actualDF = Aggregator.totalCarsByDate(ds).toDF()
      val expectedSchema = List(
        StructField("Date", DateType, nullable = false),
        StructField("Total Cars", LongType, nullable = false)
      )
      val expectedData = Seq(
        Row(Date.valueOf("2021-12-01"), 17.toLong),
        Row(Date.valueOf("2021-12-05"), 15.toLong),
        Row(Date.valueOf("2021-12-08"), 44.toLong)
      )
      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )
      assertSmallDatasetEquality(actualDF, expectedDF, ignoreNullable = true)
    }

    it("should return top three half hours with most cars") {
      val actualDF = Aggregator.topThreeHalfHoursWithMostCars (ds).toDF()
      val expectedSchema = List(
        StructField("Date", StringType, nullable = false),
        StructField("Total Cars", LongType, nullable = false)
      )
      val expectedData = Seq(
        Row("2021-12-08T18:00:00", 33.toLong),
        Row("2021-12-05T15:30:00", 15.toLong),
        Row("2021-12-01T05:30:00", 12.toLong)
      )
      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )
      assertSmallDatasetEquality(actualDF, expectedDF, ignoreNullable = true)
    }

    it("should return 1.5 hour period with least cars") {
      val actualDF = Aggregator.contiguousPeriodWithLeastCars(ds, "90 minutes").toDF()
      val expectedSchema = List(
        StructField("Window start", TimestampType, nullable = false),
        StructField("Window end", TimestampType, nullable = false),
        StructField("Total Cars", LongType, nullable = false)
      )
      val expectedData = Seq(
        Row(Timestamp.valueOf("2021-12-08 23:00:00"), Timestamp.valueOf("2021-12-09 00:30:00"),11.toLong)
      )
      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )
      assertSmallDatasetEquality(actualDF, expectedDF, ignoreNullable = true)
    }
  }
}
