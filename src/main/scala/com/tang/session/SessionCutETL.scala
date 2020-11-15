package com.tang.session

import org.apache.parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}

object SessionCutETL {
  private val logTypeSet = Set("pageview", "click")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SessionCutETL")
    conf.setMaster("local")

    // 开启kryo序列化
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    //网站域名标签map，可以放在数据库中，然后从数据库中捞取出来
    val domainLabelMap = Map("www.baidu.com" -> "level1", "www.ali.com" -> "level2",
      "jd.com" -> "level3", "youku.com" -> "level4")

    val domainLabelList = sc.broadcast(domainLabelMap)

    // 加载数据
    val rawRDD: RDD[String] = sc.textFile("data/rawdata/visit_log.txt")

    // 解析数据并过滤
    val parseLogRDD: RDD[TrackerLog] = rawRDD.flatMap(line => RawLogParser.parse(line))
      .filter(log => logTypeSet.contains(log.getLogType.toString))

    //缓存
    parseLogRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // 分组数据
    val userGroupRDD: RDD[(String, Iterable[TrackerLog])] = parseLogRDD
      .groupBy(log => log.getCookie.toString)

    // 会话切割
    val userSessionRDD: RDD[(String, TrackerSession)] = userGroupRDD.flatMapValues { case iter =>
      // 混入
      val userProcessor = new OneUserTrackerLogsProcessor(iter.toArray) with PageViewSessionGenerator
      userProcessor.buildSession(domainLabelList.value)
    }

    //  给会话的cookie打上标签
    val cookieLabelRDD: RDD[(String, String)] = sc.textFile("data/cookie_label.txt").map { case line =>
      val temp = line.split("\\|")
      (temp(0), temp(1)) // (cookie, cookie_label)
    }

    val joinRDD: RDD[(String, (TrackerSession, Option[String]))] =
      userSessionRDD.leftOuterJoin(cookieLabelRDD)

    val cookieLabelSessionRDD: RDD[TrackerSession] = joinRDD.map { case (cookie, (session, cookieLabelOption)) =>
      if (cookieLabelOption.nonEmpty) {
        session.setCookieLabel(cookieLabelOption.get)
      }
      else {
        session.setCookieLabel("-")
      }
      session
    }

    OutputComponent.fromOutPutFileType("Text")
      .writeOutputData(sc, parseLogRDD, "data/output", cookieLabelSessionRDD)
    //cookieLabelSessionRDD.collect().foreach(println)

    sc.stop()
  }

}
