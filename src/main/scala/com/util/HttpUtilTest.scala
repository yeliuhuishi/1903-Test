package com.util

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
  * @Classname HttpUtilTest
  * @Date 20/07/31 9:59
  * @Created by YELIUHUISHI
  * 发送get请求
  *
  */
object HttpUtilTest {
  def get(url: String): String = {
    val client = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    // 发送
    val respense = client.execute(httpGet)
    // 获取返回结果
    EntityUtils.toString(respense.getEntity, "UTF-8")
  }
}
