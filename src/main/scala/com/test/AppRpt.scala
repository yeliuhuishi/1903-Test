package com.test

import com.util.RptUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @Classname AppRpt
  * @Date 20/07/28 8:23
  * @Created by YELIUHUISHI
  * 媒体维度统计
  *
  */
object AppRpt {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      sys.exit()
    }
    val Array(inputPath, app_dic) = args
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
    val mapBroad = spark.sparkContext.broadcast(dicMap)

    val df = spark.read.parquet(inputPath)

    df.rdd
      .map((row: Row) => {
        val appid = row.getAs[String]("appid")
        val appname = row.getAs[String]("appname")
        // 判读AppName是否存在
        if (StringUtils.isNoneBlank(appname)) {
          appname == mapBroad.value.getOrElse(appid, "其他")
        }
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

        (appname, list1 ++ list2 ++ list3)
      })
      .reduceByKey((list1, list2) => {
        list1.zip(list2).map(t => t._1 + t._2)
      })
      .foreach(println)

    spark.stop()
  }
}
