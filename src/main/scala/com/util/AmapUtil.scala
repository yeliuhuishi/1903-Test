package com.util

import com.alibaba.fastjson.{JSON, JSONObject}
import scala.collection.mutable.ListBuffer

/**
  * @Classname AmapUtil
  * @Date 20/07/31 9:55
  * @Created by YELIUHUISHI
  * 从高德获取商圈信息
  *
  */
object AmapUtil {
  def getBusinessFromAmap(long: Double, lat: Double): String = {
    // https://restapi.amap.com/v3/geocode/regeo
    // ?location=116.310003,39.991957&key=<用户的key>&radius=3000
    // 拼接URL
    val location = long + "," + lat
    val url = "https://restapi.amap.com/v3/geocode/regeo?location=" + location + "&key=&radius=3000"
    // 发送HTTP请求协议
    val json = HttpUtilTest.get(url)

    //    println(json)
    var list = ListBuffer[String]()
    val jObject = JSON.parseObject(json)
    // 判断当前的json串必须有数据
    val status = jObject.getIntValue("status")
    if (status == 0) return ""
    // 如果不为空，再次获取值
    val regeocode = jObject.getJSONObject("regeocode")
    if (regeocode == null) return ""
    val address = regeocode.getJSONObject("addressComponent")
    if (address == null) return ""
    val businessAreas = address.getJSONArray("businessAreas")
    if (businessAreas == null) return ""

    // 循环处理数组
    for (arr <- businessAreas.toArray) {
      // 将元素类型转换JSON
      if (arr.isInstanceOf[JSONObject]) {
        val json = arr.asInstanceOf[JSONObject]
        val name = json.getString("name") // 商圈名称
        list.append(name)
      }
    }
    list.mkString(",")
  }
}
