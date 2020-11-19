package com.tang.FlightAnalysis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object FlightAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
      .appName("FlightAnalysis")
      .master("local")
      .getOrCreate()

    // 直接读取生成DataFrame
    //    val airPortDF = sc.read
    //      .option("header", "true")
    //      .option("inferSchema", "true")
    //      .csv("data/airInfo/airports.csv")
    //
    //    val flightDF = sc.read
    //      .option("inferSchema", "true")
    //      .csv("data/airInfo/2008.csv")

    // 获取08年航班信息
    val rawFlightRDD: RDD[String] = spark.sparkContext.textFile("data/airInfo/2008.csv")
    val flightRDD: RDD[Flight] = rawFlightRDD.flatMap(line => Flight.parse(line))
    val flightDF = spark.createDataFrame(flightRDD)

    // 获取机场信息
    val rawAirportRDD = spark.sparkContext.textFile("data/airInfo/airports.csv")
    val airportsRDD = rawAirportRDD.flatMap(Airport.parse(_))
    val airportsDF = spark.createDataFrame(airportsRDD)

    // 查出取消的航班信息
    val calceledFlights = flightDF.filter(flightDF("canceled") > 0)

    // 查询一个月航班取消的次数
    import spark.implicits._
    val monthCalceledFlights = calceledFlights.groupBy($"date.month")
      .count().orderBy($"date.month".asc)

    // 查询两个机场间的航班数
    spark.conf.set("spark.sql.shuffle.partitions", 2)
    val airportsFlightsCount = flightDF.select($"origin", $"dest").groupBy($"origin", $"dest")
      .count().orderBy($"count".desc, $"origin", $"dest")
    //airportsFlightsCount.write.mode(SaveMode.Overwrite).csv("data/airInfo/info")

    // cube分析数据
    //flightDF.select($"origin", $"dest").groupBy($"origin", $"dest").count().show(1000)
    //flightDF.select($"origin", $"dest").cube($"origin", $"dest").count().show(1000)
    //    flightDF.cube($"origin", $"dest").agg(Map(
    //      "*" -> "count",
    //      "times.actualElapsedTime" -> "avg",
    //      "distance" -> "avg"
    //    )).orderBy($"avg(distance)".desc).show()

    import org.apache.spark.sql.functions._
    flightDF.agg(
      min($"times.actualElapsedTime"),
      max($"times.actualElapsedTime"),
      avg($"times.actualElapsedTime"),
      sum($"times.actualElapsedTime")).show(1000)

    spark.stop()
  }
}
