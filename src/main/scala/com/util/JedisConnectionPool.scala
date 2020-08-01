package com.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * @Classname JedisConnectionPool
  * @Date 20/07/31 11:21
  * @Created by YELIUHUISHI
  *  获取连接
  *
  */
object JedisConnectionPool {

  private val config = new GenericObjectPoolConfig
  config.setMaxTotal(10)
  config.setMaxIdle(5)
  private val pool = new JedisPool(config, "Centos7", 6379, 10000, "LXQHXZ20")
  def getConnection() = {
    pool.getResource
  }
}
