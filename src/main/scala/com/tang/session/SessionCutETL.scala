package com.tang.session

import org.apache.parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
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
      val userProcessor = new OneUserTrackerLogsProcessor(iter.toArray)
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

    val baseOutputPath = "data/output/trackerLog"
    AvroWriteSupport.setSchema(sc.hadoopConfiguration, TrackerLog.SCHEMA$)
    parseLogRDD.map((null, _)).saveAsNewAPIHadoopFile(baseOutputPath, classOf[Void], classOf[TrackerLog],
      classOf[AvroParquetOutputFormat[TrackerLog]])

    val baseTrackerSessionOutputPath = "data/output/trackerSession"
    AvroWriteSupport.setSchema(sc.hadoopConfiguration, TrackerSession.SCHEMA$)
    cookieLabelSessionRDD.map((null, _)).saveAsNewAPIHadoopFile(baseTrackerSessionOutputPath,
      classOf[Void], classOf[TrackerSession],
      classOf[AvroParquetOutputFormat[TrackerSession]])
    //cookieLabelSessionRDD.collect().foreach(println)

    sc.stop()
  }
}
