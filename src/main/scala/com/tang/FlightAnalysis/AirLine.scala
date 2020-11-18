package com.tang.FlightAnalysis

case class Flight(
                   date: Flight.Date, // fields 1-4.
                   times: Flight.Times, // fields 5-8, 12-16, 20-21
                   uniqueCarrier: String, //  9:  unique carrier code
                   flightNum: Int, // 10:  flight number
                   tailNum: String, // 11:  plane tail number
                   origin: String, // 17:  origin IATA airport code
                   dest: String, // 18:  destination IATA airport code
                   distance: Int, // 19:  in miles
                   canceled: Int, // 22:  was the flight canceled?
                   cancellationCode: String, // 23:  reason for cancellation (A = carrier, B = weather, C = NAS, D = security)
                   diverted: Int, // 24:  1 = yes, 0 = no
                   carrierDelay: Int, // 25:  in minutes
                   weatherDelay: Int, // 26:  in minutes
                   nasDelay: Int, // 27:  in minutes
                   securityDelay: Int, // 28:  in minutes
                   lateAircraftDelay: Int // 29:  in minutes
                 )

object Flight {

  case class Date(year: Int, month: Int, dayOfMonth: Int, dayOfWeek: Int)

  case class Times(depTime: Int, //  5:  actual departure time (local, hhmm)
                   crsDepTime: Int, //  6:  scheduled departure time (local, hhmm)
                   arrTime: Int, //  7:  actual arrival time (local, hhmm)
                   crsArrTime: Int, //  8:  scheduled arrival time (local, hhmm)
                   actualElapsedTime: Int, // 12:  in minutes
                   crsElapsedTime: Int, // 13:  in minutes
                   airTime: Int, // 14:  in minutes
                   arrDelay: Int, // 15:  arrival delay, in minutes
                   depDelay: Int, // 16:  departure delay, in minutes
                   taxiIn: Int, // 20:  taxi in time, in minutes
                   taxiOut: Int // 21:  taxi out time in minutes
                  )

  def parse(line: String): Option[Flight] = {
    val fields = line.split(",")
    if (fields(0).endsWith("year")) { //如果这行是schema的话
      None
    } else {
      try {
        import Conversions._
        Some(Flight(
          Date(
            toInt(fields(0)),
            toInt(fields(1)),
            toInt(fields(2)),
            toInt(fields(3))),
          Times(
            toInt(fields(4)),
            toInt(fields(5)),
            toInt(fields(6)),
            toInt(fields(7)),
            toInt(fields(11)),
            toInt(fields(12)),
            toInt(fields(13)),
            toInt(fields(14)),
            toInt(fields(15)),
            toInt(fields(19)),
            toInt(fields(20))),
          fields(8).trim,
          toInt(fields(9)),
          fields(10).trim,
          fields(16).trim,
          fields(17).trim,
          toInt(fields(18)),
          toInt(fields(21)),
          fields(22).trim,
          toInt(fields(23)),
          toInt(fields(24)),
          toInt(fields(25)),
          toInt(fields(26)),
          toInt(fields(27)),
          toInt(fields(28))
        ))
      } catch {
        case e: IndexOutOfBoundsException => {
          Console.err.println(s"${line} parse error, exception is : $e")
          None
        }
      }
    }
  }
}

case class Airport(iata: String, airport: String, city: String,
                   state: String, country: String, lat: Double, lng: Double)

object Airport {
  val headerRE = """^\s*"iata"\s*,.*""".r
  val lineRE = """^\s*"([^"]+)"\s*,\s*"([^"]+)"\s*,\s*"?([^"]+)"?\s*,\s*"?([^"]+)"?\s*,\s*"([^"]+)"\s*,\s*([-.\d]+)\s*,\s*([-.\d]+)\s*$""".r

  def parse(line: String): Option[Airport] = line match {
    case headerRE() => None
    case lineRE(iata, airport, city, state, country, lat, lng) =>
      Some(Airport(iata.trim, airport.trim, city.trim, state.trim, country.trim, lat.toFloat, lng.toFloat))
    case line =>
      Console.err.println(s"ERROR: Invalid Airport line: $line")
      None
  }
}
