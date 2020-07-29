package com.util

/**
  * @Classname Tags
  * @Date 20/07/28 17:51
  * @Created by YELIUHUISHI
  * 定义打标签的接口
  *
  */
trait Tags {
  def makeTags(args: Any*): List[(String, Int)]
}
