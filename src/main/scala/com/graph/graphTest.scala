package com.graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @Classname graphTest
  * @Date 20/08/03 9:02
  * @Created by YELIUHUISHI
  * 图计算案例
  *
  */
object graphTest {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().appName("graph").master("local").getOrCreate()
    val rdd: RDD[(Long, (String, Int))] = spark.sparkContext.makeRDD(
      Seq(
        (1L, ("张三", 23)),
        (2L, ("历史", 56)),
        (6L, ("王五", 28)),
        (9L, ("赵六", 24)),
        (16L, ("小明", 53)),
        (21L, ("小红", 33)),
        (44L, ("小黄", 26)),
        (5L, ("白胡子", 72)),
        (7L, ("索隆", 25)),
        (133L, ("田七", 20)),
        (168L, ("路费", 23)),
        (138L, ("小绿", 23))
      )
    )
    val rdd2 = spark.sparkContext.makeRDD(
      Seq(
        Edge(1L, 133L, 0),
        Edge(2L, 133L, 0),
        Edge(6L, 133L, 0),
        Edge(9L, 133L, 0),
        Edge(6L, 138L, 0),
        Edge(16L, 138L, 0),
        Edge(21L, 138L, 0),
        Edge(44L, 138L, 0),
        Edge(5L, 158L, 0),
        Edge(7L, 158L, 0)
      )
    )
    // 构建图
    val grap: Graph[(String, Int), Int] = Graph(rdd, rdd2)
//    grap.vertices.foreach(println)
//    grap.edges.foreach(println)
    grap.connectedComponents().vertices.foreach(println)
    grap.connectedComponents().edges.foreach(println)
  }
}
