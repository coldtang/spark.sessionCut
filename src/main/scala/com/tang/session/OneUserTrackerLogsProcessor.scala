package com.tang.session

import org.apache.commons.lang3.time.FastDateFormat

import scala.collection.mutable.ArrayBuffer

/**
  * 处理单个user下面的所有的trackerLogs
  *
  * @param trackerLog
  */
class OneUserTrackerLogsProcessor(trackerLog: Array[TrackerLog]) {

  private val sortedTrackerLogs = trackerLog.sortBy(m => m.getLogServerTime.toString)
  private val dataFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    * 处理单个user下面的所有的trackerLogs
    *
    * @return
    */
  def buildSession(): Array[TrackerSession] = {
    //会话切割
    var cutSessionLogs = new ArrayBuffer[TrackerLog]()
    var initBuilder = ArrayBuffer.newBuilder[ArrayBuffer[TrackerLog]]
    sortedTrackerLogs.foldLeft((initBuilder, Option.empty[TrackerLog])) {
      case ((builder, preLog), currLog) =>
        val currLogTime = dataFormat.parse(currLog.getLogServerTime.toString).getTime
        val preLogTime = dataFormat.parse(preLog.get.getLogServerTime.toString).getTime
        if (preLog.nonEmpty && currLogTime - preLogTime >= 30 * 60 * 1000) {
          builder += cutSessionLogs.clone()
          cutSessionLogs.clear()
        }
        cutSessionLogs += currLog
        (builder, Some(currLog))
    }

    //生成会话

    Array()
  }
}
