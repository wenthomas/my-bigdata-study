package com.wenthomas.streaming.adsproject.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
 * @author Verno
 * @create 2020-03-24 23:04 
 */

/*
redis客户端两种方式：本次选用连接池方式
1. 使用连接池创建客户端 √

2. 直接创建客户端
 */
object RedisUtils {

    private val conf = new JedisPoolConfig
    val host = "127.0.0.1"

    conf.setMaxTotal(100)
    conf.setMaxIdle(10)
    conf.setMinIdle(10)
    //忙碌时是否等待
    conf.setBlockWhenExhausted(true)
    //最大等待时间
    conf.setMaxWaitMillis(10000)

    private val pool = new JedisPool(conf, host, 6379)

    def getClient = pool.getResource

}
