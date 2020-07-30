package com.tags

import org.apache.spark.sql.Row
import com.util.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast

/**
  * @Classname TagApp
  * @Date 20/07/30 8:58
  * @Created by YELIUHUISHI
  *
  *
  */
object TagApp extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val dic = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]
    // 获取appname和appid
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    if (StringUtils.isNoneBlank(appname)) {
      list :+= ("APP:" + appname, 1)
    } else if (StringUtils.isNoneBlank(appid)) {
      list :+= ("APP" + dic.value.getOrElse(appid, "其他"), 1)
    }
    list
  }
}
