package com.tang.complextype

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, MapType, StringType, StructType}

object JsonSQLFunctionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JsonSQLFunctionTest")
      .master("local").getOrCreate()

    val dataRDD = spark.sparkContext.textFile(s"data/example/complex_type.json")

    val schema = new StructType().add("dc_id", StringType).add("source",
      MapType(
        StringType,
        new StructType().add("description", StringType).add("ip", StringType).add("id", LongType).add("temp", LongType).add("c02_level", LongType).add("geo",
          new StructType().add("lat", DoubleType).add("long", DoubleType)
        )
      )
    )
    import spark.implicits._
    val ds = spark.createDataset(dataRDD)

    val df = spark.read.schema(schema).json(ds)

    df.printSchema()
    df.show(false)

    import org.apache.spark.sql.functions._

    // explode
    val explodedDF = df.select($"dc_id", explode($"source"))
    explodedDF.printSchema()
    explodedDF.show()

    //getItem
    val devicesDF = explodedDF.select($"dc_id".alias("id"),
      $"key" as "deviceType",
      $"value".getItem("description") as "description",
      $"value".getItem("geo").getItem("lat") as "lat"
    )
    devicesDF.show(false)
    spark.stop()
  }
}


