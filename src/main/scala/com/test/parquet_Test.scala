package com.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @Classname parquet_Test
  * @Date 20/07/24 16:31
  * @Created by YELIUHUISHI
  * 测试
  *
  */
object parquet_Test {
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
//    // 数据处理
//    // 注册临时视图
//    df.createTempView("log")
//    // 执行SQl语句
//    val df2 = spark.sql("""
//        |select
//        |provincename,
//        |cityname,
//        |count(*) ct
//        |from log
//        |group by provincename,cityname
//        |""".stripMargin)
//    df2.write.partitionBy("provincename", "cityname").json("output")
//    df.rdd
//      .map(row => {
//        ((row.getAs[String]("provincename"), row.getAs[String]("cityname")), 1)
//      })
//      .reduceByKey(_ + _)
//      .take(10)
//      .foreach(println)
    spark.stop()

  }
}
