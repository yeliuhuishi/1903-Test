package com.tags

import com.util.{JedisConnectionPool, TagUtils}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
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
    val Array(inputPath, app_dic, stopWords) = args;
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession
      .builder()
      .appName("log2Parquet")
      .master("local")
      .config(conf) // 加载配置
      .getOrCreate()

    val dicMap = spark.sparkContext
      .textFile(app_dic)
      .map(_.split("\t", -1))
      .filter(_.length >= 5)
      .map(arr => {
        (arr(4), arr(1))
      })
      .collectAsMap()
    // 广播字典
    val mapBroad: Broadcast[collection.Map[String, String]] =
      spark.sparkContext.broadcast(dicMap)
    // 读取停用词库
    val arr = spark.sparkContext.textFile(stopWords).collect()
    val stopBroad = spark.sparkContext.broadcast(arr)
    // 获取数据
    val df = spark.read.parquet(inputPath)
    // 判断用户的唯一ID必须要存在
    df.filter(TagUtils.OneUserId)
      //进行打标签处理
      .rdd
      .mapPartitions(rdd => {
        val jedis = JedisConnectionPool.getConnection
        val ite = rdd.map(row => {
          // 获取不为空的唯一UserId
          val userId = TagUtils.getAnyOneUserId(row)
          //  广告位类型（标签格式：LC0x -> 1 或者 LCxx -> 1 ）xx 为数字
          // 小于 10 补 0,，把广告位类型名称，LN 插屏 -> 1
          row.getAs[Int]("adspacetype")
          row.getAs[Int]("adspacetypename")
          // 广告类型标签
          val adList: List[(String, Int)] = TagsAD.makeTags(row)
          // APP标签
          val appList = TagApp.makeTags(row, mapBroad)
          // 设备标签
          val devList = TagsDev.makeTags(row)
          // 关键字标签
          val kwList = TagsKeyWork.makeTags(row, stopBroad)
          // 商圈标签
          val busList: List[(String, Int)] = TagBusiness.makeTags(row, jedis)
          (userId, adList ++ appList ++ devList ++ kwList ++ busList)
        })
        jedis.close()
        ite
      })

  }
}
