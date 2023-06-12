package com.traffic.counter.models

import java.sql.{Date, Timestamp}
import java.time.ZoneOffset

case class TrafficData(timestamp: Timestamp,  numCars: Int) {
  def as[T](implicit f: TrafficData => T): T = f(this)
}

case object TrafficData {
  implicit def toEnrichedTrafficData: TrafficData => EnrichedTrafficData = (a: TrafficData) => {
    val localDateTime = a.timestamp.toLocalDateTime
    val isoDateTime: String = localDateTime.toInstant(ZoneOffset.UTC).toString.replace("Z", "")
    val date: Date = Date.valueOf(localDateTime.toLocalDate)
    EnrichedTrafficData(a.timestamp, date, isoDateTime, a.numCars)
  }
}


