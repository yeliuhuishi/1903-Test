package com.util

import org.apache.spark.sql.Row
import org.spark_project.jetty.util.StringUtil

/**
  * @Classname TagUtils
  * @Date 20/07/28 16:25
  * @Created by YELIUHUISHI
  *
  *
  */
object TagUtils {
  def getAnyOneUserId(row: Row): Unit = {
    row match {
      case v if StringUtil.isNotBlank(v.getAs[String]("imei")) =>
        "TM:" + v.getAs[String]("imei")
      case v if StringUtil.isNotBlank(v.getAs[String]("mac")) =>
        "MC:" + v.getAs[String]("mac")
      case v if StringUtil.isNotBlank(v.getAs[String]("idfc")) =>
        "ID:" + v.getAs[String]("idfc")
      case v if StringUtil.isNotBlank(v.getAs[String]("openudid")) =>
        "OD:" + v.getAs[String]("openudid")
      case v if StringUtil.isNotBlank(v.getAs[String]("androidid")) =>
        "AD:" + v.getAs[String]("androidid")

    }
  }

  val OneUserId =
    """
      |imei !='' or mac != '' or idfa != '' or openudid != '' or androidid != ''
      |""".stripMargin

}
