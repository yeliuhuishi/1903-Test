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
  def getBusinessFromAmap(longs: Double, lat: Double): String = {
    //https://restapi.amap.com/v3/geocode/regeo?
    // location=116.310003,39.991957&
    // key=ebb5a0146b1cdde7e8ad8de001a945ee&
    // radius=3000&
    // extensions=base
    val location = longs + "," + lat
    val url = "https://restapi.amap.com/v3/geocode/regeo?location=" + location + "&" +
      "key=ebb5a0146b1cdde7e8ad8de001a945ee&" +
      "radius=3000&" +
      "extensions=base"
    // 发送http请求协议
    val json = HttpUtilTest.get(url)

    /*
            {
          "status": "1",
          "regeocode": {
            "addressComponent": {
              "city": [],
              "province": "北京市",
              "adcode": "110108",
              "district": "海淀区",
              "towncode": "110108015000",
              "streetNumber": {
                "number": "5号",
                "location": "116.310454,39.9927339",
                "direction": "东北",
                "distance": "94.5489",
                "street": "颐和园路"
              },
              "country": "中国",
              "township": "燕园街道",
              "businessAreas": [
                {
                  "location": "116.303364,39.97641",
                  "name": "万泉河",
                  "id": "110108"
                },
                {
                  "location": "116.314222,39.98249",
                  "name": "中关村",
                  "id": "110108"
                },
                {
                  "location": "116.294214,39.99685",
                  "name": "西苑",
                  "id": "110108"
                }
              ],
              "building": {
                "name": "北京大学",
                "type": "科教文化服务;学校;高等院校"
              },
              "neighborhood": {
                "name": "北京大学",
                "type": "科教文化服务;学校;高等院校"
              },
              "citycode": "010"
            },
            "formatted_address": "北京市海淀区燕园街道北京大学"
          },
          "info": "OK",
          "infocode": "10000"
        }

     */
    var list = ListBuffer[String]()

    val jObject = JSON.parseObject(json)
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
      // 将元素类型转换成json
      val json: JSONObject = arr.asInstanceOf[JSONObject]
      val name = json.getString("name") // 商圈名
      list.append(name)
    }
    list.mkString(",")
  }

}
