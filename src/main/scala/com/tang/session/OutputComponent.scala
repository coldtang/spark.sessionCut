package com.tang.session

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait OutputComponent {
  def writeOutputData(sc: SparkContext, parseLogRDD: RDD[TrackerLog],
                      outputPath: String, cookieLabelSessionRDD: RDD[TrackerSession]) = {
    pathIsExists(sc, outputPath)
  }

  /**
    * 校验路径是否存在文件，有则删除
    *
    * @param sc
    * @param outputPath
    * @return
    */
  private def pathIsExists(sc: SparkContext, outputPath: String) = {
    var filestream = FileSystem.get(sc.hadoopConfiguration)
    if (filestream.exists(new Path(outputPath))) {
      filestream.delete(new Path(outputPath), true)
    }
  }
}

object OutputComponent {
  def fromOutPutFileType(fileType: String) = {
    if (fileType.equals("Text")) {
      new TextFileOutput()
    }
    else {
      new ParquetFileOutput()
    }
  }
}

/**
  * 输出Text数据
  */
class TextFileOutput extends OutputComponent {
  /**
    * 输出Text数据
    *
    * @param sc
    * @param parseLogRDD
    * @param outputPath
    * @param cookieLabelSessionRDD
    */
  override def writeOutputData(sc: SparkContext, parseLogRDD: RDD[TrackerLog],
                               outputPath: String, cookieLabelSessionRDD: RDD[TrackerSession]): Unit = {

    super.writeOutputData(sc, parseLogRDD, outputPath, cookieLabelSessionRDD)

    val baseOutputPath = s"${outputPath}/trackerLog"

    parseLogRDD.map((null, _)).saveAsTextFile(baseOutputPath)

    val baseTrackerSessionOutputPath = s"${outputPath}/trackerSession"

    cookieLabelSessionRDD.map((null, _)).saveAsTextFile(baseTrackerSessionOutputPath)
  }
}

/**
  * 输出Parquet数据
  */
class ParquetFileOutput extends OutputComponent {
  /**
    * 输出Parquet数据
    *
    * @param sc
    * @param parseLogRDD
    * @param outputPath
    * @param cookieLabelSessionRDD
    */
  override def writeOutputData(sc: SparkContext, parseLogRDD: RDD[TrackerLog],
                               outputPath: String, cookieLabelSessionRDD: RDD[TrackerSession]): Unit = {
    super.writeOutputData(sc, parseLogRDD, outputPath, cookieLabelSessionRDD)

    val baseOutputPath = s"${outputPath}/trackerLog"

    AvroWriteSupport.setSchema(sc.hadoopConfiguration, TrackerLog.SCHEMA$)
    parseLogRDD.map((null, _)).saveAsNewAPIHadoopFile(baseOutputPath, classOf[Void], classOf[TrackerLog],
      classOf[AvroParquetOutputFormat[TrackerLog]])

    val baseTrackerSessionOutputPath = s"${outputPath}/trackerSession"

    AvroWriteSupport.setSchema(sc.hadoopConfiguration, TrackerSession.SCHEMA$)
    cookieLabelSessionRDD.map((null, _)).saveAsNewAPIHadoopFile(baseTrackerSessionOutputPath,
      classOf[Void], classOf[TrackerSession],
      classOf[AvroParquetOutputFormat[TrackerSession]])
  }
}