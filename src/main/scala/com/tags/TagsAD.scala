package com.tags

import com.util.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * @Classname TagsAD
  * @Date 20/07/28 17:55
  * @Created by YELIUHUISHI
  *
  *
  */
object TagsAD extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    val adType = row.getAs[Int]("adspacetype")
    val adName = row.getAs[String]("adspacetypename")
    adType match {
      case v if v > 9           => list :+= ("LC" + v, 1)
      case v if v > 0 && v <= 9 => list :+= ("LC0" + v, 1)
    }
    // 保证名字不为空
    if (StringUtils.isNotBlank(adName)) {
      list :+= ("LN" + adName, 1)
    }
    // 渠道
    val china = row.getAs[Int]("adplatformproviderid")
    // 地域标签
    val provincename = row.getAs[String]("provincename")
    val cityname = provincename, row.getAs[String]("cityname")
    if(StringUtils.isNoneBlank(provincename)){
      list:+=("ZP"+provincename,1)
      list:+=("ZC"+cityname,1)
    }
    list
  }
}
