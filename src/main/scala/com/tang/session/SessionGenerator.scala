package com.tang.session

import org.apache.commons.lang3.time.FastDateFormat

import scala.collection.mutable.ArrayBuffer

/**
  * 会话切割接口
  */
trait SessionGenerator {
  private val dataFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    * 会话切割
    * 每隔30分钟切割会话
    *
    * @return
    */
  def cutSession(sortedTrackerLogs: Array[TrackerLog]): ArrayBuffer[ArrayBuffer[TrackerLog]] = {
    var cutSessionLogs = new ArrayBuffer[TrackerLog]()
    var initBuilder = ArrayBuffer.newBuilder[ArrayBuffer[TrackerLog]]
    val cuttedSessionLogsResult: ArrayBuffer[ArrayBuffer[TrackerLog]] =
      sortedTrackerLogs.foldLeft((initBuilder, Option.empty[TrackerLog])) {
        case ((builder, preLog), currLog) =>
          val currLogTime = dataFormat.parse(currLog.getLogServerTime.toString).getTime
          if (preLog.nonEmpty) {
            val preLogTime = dataFormat.parse(preLog.get.getLogServerTime.toString).getTime
            if (currLogTime - preLogTime > 30 * 60 * 1000) {
              builder += cutSessionLogs.clone()
              cutSessionLogs.clear()
            }
          }
          cutSessionLogs += currLog
          (builder, Some(currLog))
      }._1.result()

    // 获取最后的会话
    if (cutSessionLogs.nonEmpty) {
      cuttedSessionLogsResult += cutSessionLogs
    }

    cuttedSessionLogsResult
  }
}

/**
  * 按照PageView切割会话
  */
trait PageViewSessionGenerator extends SessionGenerator {
  override def cutSession(sortedTrackerLogs: Array[TrackerLog]): ArrayBuffer[ArrayBuffer[TrackerLog]] = {
    var cutSessionLogs = new ArrayBuffer[TrackerLog]()
    var initBuilder = ArrayBuffer.newBuilder[ArrayBuffer[TrackerLog]]
    val cuttedSessionLogsResult: ArrayBuffer[ArrayBuffer[TrackerLog]] =
      sortedTrackerLogs.foldLeft((initBuilder)) {
        case (builder, currLog) =>
          if (currLog.getLogType.toString.equals("pageview") && cutSessionLogs.nonEmpty) {
            builder += cutSessionLogs.clone()
            cutSessionLogs.clear()
          }
          cutSessionLogs += currLog
          builder
      }.result()

    // 获取最后的会话
    if (cutSessionLogs.nonEmpty) {
      cuttedSessionLogsResult += cutSessionLogs
    }

    cuttedSessionLogsResult
  }
}