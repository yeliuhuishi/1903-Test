package com.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * 获取连接
  */
object JedisConnectionPool {

  private val config = new GenericObjectPoolConfig
  // 最大链接数
  config.setMaxTotal(100)
  // 空闲时最大链接数
  config.setMaxIdle(10)
  // 空闲最小
  config.setMinIdle(8)
  // 链接最大等待时间
  config.setMaxWaitMillis(40000)
  private val pool =
    new JedisPool(config, "192.168.203.7", 6379, 10000)
  def getConnection() = {
    pool.getResource
  }
}
