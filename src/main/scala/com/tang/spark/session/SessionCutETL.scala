package com.tang.spark.session

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SessionCutETL {
  private val logTypeSet = Set("pageview", "click")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SessionCutETL")
    conf.setMaster("local")

    // 开启kryo序列化
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    // 加载数据
    val rawRDD: RDD[String] = sc.textFile("data/rawdata/visit_log.txt")

    // 解析数据并过滤
    val parseLog: RDD[TrackerLog] = rawRDD.flatMap(line => RawLogParser.parse(line))
      .filter(log => logTypeSet.contains(log.getLogType.toString))


    parseLog.collect.foreach(println)

    sc.stop()
  }
}
