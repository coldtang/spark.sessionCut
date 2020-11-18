package com.tang.FlightAnalysis

/**
  * 类型转换
  * @author tang
  */
object Conversions {
  /**
    *  字符串转成整形，如果字符串为空或者等于NA的时候，返回默认值
    *  如果传进来的字符串不是数字的话，则返回默认值
    * @param str
    * @param default
    * @return
    */
  def toInt(str: String, default: Int = -1): Int = {
    if (str.trim.isEmpty || str.trim.endsWith("NA")) {
      default
    } else {
      try {
        str.toInt
      } catch {
        case e : NumberFormatException =>
          Console.err.println(s"$str is not a number， exception is : $e")
          default
      }
    }
  }
}
