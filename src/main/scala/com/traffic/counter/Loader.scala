package com.traffic.counter

import com.traffic.counter.SparkSessionWrapper.spark.implicits._
import com.traffic.counter.models.{EnrichedRecord, Record}
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

object Loader extends SparkSessionWrapper {
  def createDataSet(fileName: String, logger: Logger): Try[Dataset[Record]] = {
    try {
      val schema = new StructType()
        .add("timestamp", "timestamp")
        .add("numCars", "int")
      val df = spark.read
        .schema(schema)
        .options(Map("delimiter" -> " ", "header" -> "false"))
        .csv(fileName)

      Success(df.as[Record])
    } catch {
      case ex: Exception =>
        logger.error(s"Exception occurred: $ex")
        Failure(ex)
    }
  }

  def toEnrichedDS(ds: Try[Dataset[Record]]): Dataset[EnrichedRecord] = ds match {
    case Failure(_) => List.empty[EnrichedRecord].toDS()
    case Success(ds) => ds.map(_.as[EnrichedRecord])
  }
}
