package com.traffic.counter

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.traffic.counter.SparkSessionTestWrapper.spark.implicits._
import com.traffic.counter.models.Record
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.funspec.AnyFunSpec

import java.sql.Timestamp
import scala.util.{Failure, Success}

class LoaderSpec extends AnyFunSpec with SparkSessionTestWrapper with DatasetComparer {
  val logger: Logger = LogManager.getLogger("test")

  def actualDS(sourceFile: String): Dataset[Record] = Loader.createDataSet(sourceFile, logger) match {
    case Failure(_) => List.empty[Record].toDS()
    case Success(ds) => ds
  }

  describe("With valid data file") {
    describe("Loads data") {
      it("should have correct schema and data") {
        val sourceFile = "src/test/resources/valid.csv"
        val actualDF = actualDS(sourceFile).toDF()

        val expectedSchema = List(
          StructField("timestamp", TimestampType, nullable = false),
          StructField("numCars", IntegerType, nullable = false)
        )

        val expectedData = Seq(
          Row(Timestamp.valueOf("2021-12-01 05:00:00"), 5),
          Row(Timestamp.valueOf("2021-12-01 05:30:00"), 12),
          Row(Timestamp.valueOf("2021-12-08 23:00:00"), 11)
        )
        val expectedDF = spark.createDataFrame(
          spark.sparkContext.parallelize(expectedData),
          StructType(expectedSchema)
        )
        assertSmallDatasetEquality(actualDF, expectedDF, ignoreNullable = true)
      }
    }
  }

  describe("With invalid file path") {
    it("should return empty dataset on an exception") {
      val sourceFile = "src/test/resources/foo.csv"
      val actualDF = actualDS(sourceFile).toDF()
      val expectedSchema = List(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("numCars", IntegerType, nullable = false)
      )

      val expectedData = Seq.empty[Row]
      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      assertSmallDatasetEquality(actualDF, expectedDF, ignoreNullable = true)
    }
  }
}