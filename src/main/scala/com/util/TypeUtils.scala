package com.util

/**
  * @Classname TypeTtils
  * @Date 20/07/24 12:26
  * @Created by YELIUHUISHI
  *
  *
  */
object TypeUtils {

  def str2Int(str: String): Int = {
    try {
      str.toInt
    } catch {
      case _: Exception => 0
    }
  }

  def str2Double(str: String): Double = {
    try {
      str.toDouble
    } catch {
      case _: Exception => 0
    }
  }
}
