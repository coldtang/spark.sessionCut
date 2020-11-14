package com.tang.session

/**
  * 格式化日志
  * @author tang
  */
object RawLogParser {
  def parse(line: String): Option[TrackerLog] = {
    if (line.startsWith("#")) None
    else {
      var fields = line.split("\\|")
      var trackerLog = new TrackerLog()
      trackerLog.setLogType(fields(0))
      trackerLog.setLogServerTime(fields(1))
      trackerLog.setCookie(fields(2))
      trackerLog.setIp(fields(3))
      trackerLog.setUrl(fields(4))
      Some(trackerLog)
    }
  }
}
