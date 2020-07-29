package com.tags

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Classname TagsContext
  * @Date 20/07/28 16:08
  * @Created by YELIUHUISHI
  * 上下文标签->用于合并总标签
  *
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    val Array(inputPath) = args;
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession
      .builder()
      .appName("log2Parquet")
      .master("local")
      .config(conf) // 加载配置
      .getOrCreate()

    val df = spark.read.parquet(inputPath)
    df.filter(TagUtils.OneUserId)
      //进行打标签处理
      .rdd
      .map(row => {
        // 获取不为空的唯一UserId
        val userId = TagUtils.getAnyOneUserId(row)
        //  广告位类型（标签格式：LC0x -> 1 或者 LCxx -> 1 ）xx 为数字
        // 小于 10 补 0,，把广告位类型名称，LN 插屏 -> 1
        row.getAs[Int]("adspacetype")
        row.getAs[Int]("adspacetypename")
        // 广告类型标签
        val adList: List[(String, Int)] = TagsAD.makeTags(row)

      })

  }
}
