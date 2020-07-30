package day01.new01

import com.etl.log
import com.util.TypeUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Classname Log2Parquet
  * @Date 20/07/29 9:56
  * @Created by YELIUHUISHI
  * log数据转成parquet
  *
  */
object logToParquet {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("parquet.compression", "snappy")
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local")
      .config(conf) // 加载配置
      .getOrCreate()
    val lines = spark.sparkContext.textFile("data\\textLog.log")

    // 读取数据
    val rddlog: RDD[log] = lines
      .filter(t => t.split(",", -1).length >= 85)
      .map(t => {
        val arr = t.split(",", -1)
        new log(
          arr(0),
          TypeUtils.str2Int(arr(1)),
          TypeUtils.str2Int(arr(2)),
          TypeUtils.str2Int(arr(3)),
          TypeUtils.str2Int(arr(4)),
          arr(5),
          arr(6),
          TypeUtils.str2Int(arr(7)),
          TypeUtils.str2Int(arr(8)),
          TypeUtils.str2Double(arr(9)),
          TypeUtils.str2Double(arr(10)),
          arr(11),
          arr(12),
          arr(13),
          arr(14),
          arr(15),
          arr(16),
          TypeUtils.str2Int(arr(17)),
          arr(18),
          arr(19),
          TypeUtils.str2Int(arr(20)),
          TypeUtils.str2Int(arr(21)),
          arr(22),
          arr(23),
          arr(24),
          arr(25),
          TypeUtils.str2Int(arr(26)),
          arr(27),
          TypeUtils.str2Int(arr(28)),
          arr(29),
          TypeUtils.str2Int(arr(30)),
          TypeUtils.str2Int(arr(31)),
          TypeUtils.str2Int(arr(32)),
          arr(33),
          TypeUtils.str2Int(arr(34)),
          TypeUtils.str2Int(arr(35)),
          TypeUtils.str2Int(arr(36)),
          arr(37),
          TypeUtils.str2Int(arr(38)),
          TypeUtils.str2Int(arr(39)),
          TypeUtils.str2Double(arr(40)),
          TypeUtils.str2Double(arr(41)),
          TypeUtils.str2Int(arr(42)),
          arr(43),
          TypeUtils.str2Double(arr(44)),
          TypeUtils.str2Double(arr(45)),
          arr(46),
          arr(47),
          arr(48),
          arr(49),
          arr(50),
          arr(51),
          arr(52),
          arr(53),
          arr(54),
          arr(55),
          arr(56),
          TypeUtils.str2Int(arr(57)),
          TypeUtils.str2Double(arr(58)),
          TypeUtils.str2Int(arr(59)),
          TypeUtils.str2Int(arr(60)),
          arr(61),
          arr(62),
          arr(63),
          arr(64),
          arr(65),
          arr(66),
          arr(67),
          arr(68),
          arr(69),
          arr(70),
          arr(71),
          arr(72),
          TypeUtils.str2Int(arr(73)),
          TypeUtils.str2Double(arr(74)),
          TypeUtils.str2Double(arr(75)),
          TypeUtils.str2Double(arr(76)),
          TypeUtils.str2Double(arr(77)),
          TypeUtils.str2Double(arr(78)),
          arr(79),
          arr(80),
          arr(81),
          arr(82),
          arr(83),
          TypeUtils.str2Int(arr(84))
        )
      })

    import spark.implicits._
    val df: DataFrame = rddlog.toDF()
    df.show(1, true)
    df.write.parquet("E:\\TestLog1")
    // 使用parquet格式+snappy压缩格式实现文件的压缩，减少数据的大小，增加数据的加载速度
    spark.stop()
  }
}
