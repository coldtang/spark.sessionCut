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
class OneUserTrackerLogsProcessor(trackerLog: Array[TrackerLog]) extends SessionGenerator{

  private val sortedTrackerLogs = trackerLog.sortBy(m => m.getLogServerTime.toString)

  /**
    * 处理单个user下面的所有的trackerLogs
    *
    * @return
    */
  def buildSession(domainLabelMap: Map[String, String]): ArrayBuffer[TrackerSession] = {
    //会话切割
    val cuttedSessionLogsResult = cutSession(sortedTrackerLogs)

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
