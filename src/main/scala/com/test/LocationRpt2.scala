package com.test

import com.util.RptUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @Classname LocationRpt
  * @Date 20/07/25 8:08
  * @Created by YELIUHUISHI
  * 地域维度指标分析
  *
  */
object LocationRpt2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession
      .builder()
      .appName("log2Parquet")
      .master("local")
      .config(conf) // 加载配置
      .getOrCreate()
    // 读取数据
    val df = spark.read.parquet("E:\\TestLog")
    df.rdd
      .map((row: Row) => {
        val requestmode = row.getAs[Int]("requestmode")
        val processnode = row.getAs[Int]("processnode")
        val iseffective = row.getAs[Int]("iseffective")
        val isbilling = row.getAs[Int]("isbilling")
        val isbid = row.getAs[Int]("isbid")
        val iswin = row.getAs[Int]("iswin")
        val adorderid = row.getAs[Int]("adorderid")
        val winprice = row.getAs[Double]("winprice")
        val adpayment = row.getAs[Double]("adpayment")
        val list1 = RptUtils.requestProcessor(requestmode, processnode)
        val list2 = RptUtils.isBidAndWin(
          iseffective,
          isbilling,
          isbid,
          iswin,
          adorderid,
          winprice,
          adpayment
        )
        val list3 = RptUtils.showsAndClk(requestmode, processnode)

        (
          (row.getAs[String]("provincename"), row.getAs[String]("cityname")),
          list1 ++ list2 ++ list3
        )
      })
      // 聚合
      .reduceByKey((list1, list2) => {
        list1
          .zip(list2)
          .map(t => t._1 + t._2)
      })
      .foreach(println)
    spark.stop()
  }
}
