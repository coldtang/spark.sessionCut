# spark.sessionCut
##会话切割
  技术栈：spark、hdfs
  提交任务方式：standalone
  从数据库中提取需要处理的click和pageView数据。click事件以默认半小时没有操作，即为一个会话，划分会话个数。
  pageView以每一次浏览为一个会话数划分会话。利用spark RDD的方式统计会话数据。
##航班数据分析
  技术栈：spark
  数据库：航班信息、机场信息
  利用spark SQL的方式统计航班取消信息、机场航班信息
##json数据格式化
  技术栈：spark
  利用get_json_object、from_json、to_json、selectExpr等方法，并利用spark SQL解析数据
