package com.traffic.counter.models

import java.sql.{Date, Timestamp}

case class EnrichedTrafficData(timestamp: Timestamp, date: Date, isoDate: String, numCars: Int)
