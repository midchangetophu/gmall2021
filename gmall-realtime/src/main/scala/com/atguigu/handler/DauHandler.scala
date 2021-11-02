package com.atguigu.handler

import java.lang

import com.atguigu.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/*
*   实现批次间、批次内去重
* */

object DauHandler {
  def filterByRedis(startUpLogDStream: DStream[StartUpLog]) = {
    // 方案一
    /*val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
      // 创建redis连接
      val jedisClient = new Jedis("hadoop102", 6379)

      // 定义redisKey
      val redisKey: String = "DAU:" + log.logDate

      // 对比数据,将重复的数据去掉

      val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)

      // 关闭连接
      jedisClient.close()

      !boolean
    })
    value*/

    // 方案二
    // 在每个分区下获取连接，以减少连接个数
    val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      // 获取redis连接
      val jedis = new Jedis("hadoop102", 6379)
      val logs: Iterator[StartUpLog] = partition.filter(log => {
        // 利用redis中的方法判断数据是否存在
        val redisKey: String = "DAU:" + log.logDate
        val boolean: Boolean = jedis.sismember(redisKey, log.mid)
        !boolean
      })
      jedis.close()
      logs
    })
    value

  }


  /*
*  将去重后的数据写入redis，为下一批数据去重用
*
* */
  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd=>{
      // 这里使用foreachPartition，这样进行连接redis数据库的时候只需要一次分区连接一次
        rdd.foreachPartition(partition=>{
            // 1.创建连接redis数据库的链接，这里需要注意的是redis中的配置文件的bind也是指向hadoop102
            val jedisClient = new Jedis("hadoop102", 6379)
            // 先使用foreachPartition后使用foreach
            // 2.写库
            partition.foreach(log=>{
                // 非关系型数据库的key，只使用logDate的话在有其他需求的情况下不好区分，因此使用一个DAU字段进行区分
                val redisKey: String = "DAU:"+log.logDate
                // 将mid存入redis,使用set存储格式
              jedisClient.sadd(redisKey,log.mid)
              }
            )
            // 关闭连接
            jedisClient.close()
          }
        )
      }
    )
  }



}
