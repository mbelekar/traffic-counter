package com.traffic.counter.models

import java.sql.{Date, Timestamp}
import java.time.ZoneOffset

case class Record(timestamp: Timestamp, numCars: Int) {
  def as[T](implicit f: Record => T): T = f(this)
}

case object Record {
  implicit val toEnrichedRecord: Record => EnrichedRecord = (a: Record) => {
    val localDateTime = a.timestamp.toLocalDateTime
    val isoDateTime: String = localDateTime.toInstant(ZoneOffset.UTC).toString.replace("Z", "")
    val date: Date = Date.valueOf(localDateTime.toLocalDate)
    EnrichedRecord(a.timestamp, date, isoDateTime, a.numCars)
  }
}


