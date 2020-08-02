package com.tags

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, Tags, TypeUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * @Classname TagBusiness
  * @Date 20/07/31 9:22
  * @Created by YELIUHUISHI
  *
  *
  */
object TagBusiness extends Tags {

  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]
    // 获取经纬度
    if (TypeUtils.str2Double(row.getAs[String]("longs")) >= 73
        && TypeUtils.str2Double(row.getAs[String]("longs")) <= 135
        && TypeUtils.str2Double(row.getAs[String]("lat")) >= 3
        && TypeUtils.str2Double(row.getAs[String]("lat")) <= 53) {
      val long = TypeUtils.str2Double(row.getAs[String]("longs"))
      val lat = TypeUtils.str2Double(row.getAs[String]("lat"))
      val business = getBusiness(long, lat, jedis)
      if (StringUtils.isNotBlank(business)) {
        val arr = business.split(",")
        arr.foreach(str => {
          list :+= (str, 1)
        })
      }
    }
    // 通过经纬度获取商圈
    // 如果直接访问高德第三方插件，是收费的，所以不能直接这么做，太烧钱了
    // 先去缓存访问一次，没有的话，再去高德获取，如果直接拿去，不用访问高德
    // 缓存数据->redis 问题：key如果设置？
    // 1、经纬度为key -> 因为的太多了
    // 2、省份为key -> 精度不够。
    // 3、使用GeoHash算法 -> 将经纬度转换成Geo编码来当Key
    //    val arr = AmapUtil.getBusinessFromAmap(long, lat).split(",")
    //    arr.foreach(t => {
    //      list :+= (t, 1)
    //    })

    list
  }

  /**
    * 获取商圈
    *
    * @param long
    * @param lat
    * @return
    */
  def getBusiness(long: Double, lat: Double, jedis: Jedis) = {
    // 转码
    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 6)
    var business = redis_queryBuiness(geoHash, jedis)
    // 如果数据库没查到
    if (business == null || business.length == 0) {
      business = AmapUtil.getBusinessFromAmap(long, lat)
      // 请求完成后，在将此商圈存入数据库一份，为了下次使用
      if (business != null || business.length > 0) {
        redis_insertBusiness(geoHash, business, jedis)
      }
    }
    business
  }

  /**
    * 数据库查询
    * @param geoHash
    * @return
    */
  def redis_queryBuiness(geoHash: String, jedis: Jedis) = {
    jedis.get(geoHash)
  }

  /**
    *  存入数据库
    * @param geoHash
    * @param business
    * @param jedis
    * @return
    */
  def redis_insertBusiness(geoHash: String, business: String, jedis: Jedis) = {

    jedis.set(geoHash, business)
  }
}
