package com.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagUtils {
  // 获取所有不为空的ID
  def getAllUserId(row: Row): Unit = {
    var list = List[String]()
    if (StringUtils.isNoneBlank(row.getAs[String]("imei")))
      list :+= "IM" + row.getAs[String]("imei")
    if (StringUtils.isNoneBlank(row.getAs[String]("mac")))
      list :+= "MC" + row.getAs[String]("mac")
    if (StringUtils.isNoneBlank(row.getAs[String]("idfe")))
      list :+= "ID" + row.getAs[String]("idfe")
    if (StringUtils.isNoneBlank(row.getAs[String]("openudid")))
      list :+= "OD" + row.getAs[String]("openudid")
    if (StringUtils.isNoneBlank(row.getAs[String]("androidid")))
      list :+= "androidid" + row.getAs[String]("androidid")
  }
  // 获取用户唯一不为空的ID
  def getAnyOneUserId(row: Row) = {
    row match {
      case v: Row if StringUtils.isNotBlank(v.getAs[String]("imei")) =>
        "TM:" + v.getAs[String]("imei")
      case v: Row if StringUtils.isNotBlank(v.getAs[String]("mac")) =>
        "MC:" + v.getAs[String]("mac")
      case v: Row if StringUtils.isNotBlank(v.getAs[String]("idfa")) =>
        "ID:" + v.getAs[String]("idfa")
      case v: Row if StringUtils.isNotBlank(v.getAs[String]("openudid")) =>
        "OD:" + v.getAs[String]("openudid")
      case v: Row if StringUtils.isNotBlank(v.getAs[String]("androidid")) =>
        "AD:" + v.getAs[String]("androidid")
    }
  }

  val OneUserId =
    """
      |imei !='' or mac !='' or idfa!= '' or openudid!='' or androidid !=''
      |""".stripMargin

}
