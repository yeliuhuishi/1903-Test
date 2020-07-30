package day01.new01

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @Classname new02
  * @Date 20/07/29 10:59
  * @Created by YELIUHUISHI
  *
  *
  */
object parquetToJson {

  def main(args: Array[String]): Unit = {
    case class statistics(provincename: String, cityname: String, ct: Int)
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession
      .builder()
      .appName("log2Parquet")
      .master("local")
      .config(conf) // 加载配置
      .getOrCreate()
    // 读取数据
    val df = spark.read.parquet("E:\\TestLog1")

    df.createTempView("log")

    val sql: DataFrame = spark.sql("""
        |select 
        |provincename,
        |cityname,
        |count(*) ct
        |from log
        |group by provincename,cityname
        |""".stripMargin)
    // 1、将数据以json的形式存储在本地
    sql.write.partitionBy("provincename", "cityname").json("E:\\jsonTest")
    // 2、将数据存入到mysql中
    val url =
      "jdbc:mysql://Centos7:3306/day43-画像项目?useUnicode=true&characterEncoding=utf8"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "LXQHXZ20")
    sql.write
      .mode(SaveMode.Overwrite)
      .jdbc(url, "GeographicalStatistics", properties)
    // 3、使用spark算子计算结果
    df.rdd
      .map(row => {
        ((row.getAs[String]("provincename"), row.getAs[String]("cityname")), 1)
      })
      .reduceByKey(_ + _)
      .take(10)
      .foreach(println)
    // 关闭spark
    spark.stop()

  }

}
