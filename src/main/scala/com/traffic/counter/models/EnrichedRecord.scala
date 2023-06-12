package com.traffic.counter.models

import java.sql.{Date, Timestamp}

case class EnrichedRecord(timestamp: Timestamp, date: Date, isoDate: String, numCars: Int)
