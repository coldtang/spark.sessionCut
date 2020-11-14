package com.tang.session

import java.net.URL
import java.util.UUID

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
  def buildSession(domainLabelMap: Map[String, String]): ArrayBuffer[TrackerSession] = {
    //会话切割
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

    //生成会话
    cuttedSessionLogsResult.map { case sessionLogs =>
      val session = new TrackerSession()
      session.setSessionId(UUID.randomUUID().toString)
      session.setSessionServerTime(sessionLogs.head.getLogServerTime)
      session.setCookie(sessionLogs.head.getCookie)

      session.setIp(sessionLogs.head.getIp)
      val pageviewLogs = sessionLogs.filter(_.getLogType.toString.equals("pageview"))
      if (pageviewLogs.length == 0) {
        session.setLandingUrl("-")
      } else {
        session.setLandingUrl(pageviewLogs.head.getUrl)
      }
      session.setPageviewCount(pageviewLogs.length)

      val clickLogs = sessionLogs.filter(_.getLogType.toString.equals("click"))
      session.setClickCount(clickLogs.length)

      if (pageviewLogs.length == 0) {
        session.setDomain("-")
      } else {
        val url = new URL(pageviewLogs.head.getUrl.toString)
        session.setDomain(url.getHost)
      }

      val domainLabel = domainLabelMap.getOrElse(session.getDomain.toString, "-")
      session.setDomainLabel(domainLabel)

      // 统计会话的个数
      //sessionCountAcc.add(1)

      session
    }
  }
}
