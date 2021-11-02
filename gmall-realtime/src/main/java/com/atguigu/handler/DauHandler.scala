package com.atguigu.handler

import java.lang

import com.atguigu.bean.StartUpLog.StartUpLog
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis


object DauHandler {
  //进行批次内去重
  def filterByGroup(filterByredisDStream:DStream[StartUpLog])={
    val value: DStream[StartUpLog] = {
      val midanddate: DStream[((String, String), StartUpLog)] = filterByredisDStream.mapPartitions(partition => {
        //将数据转化为kv
        partition.map(log => {
          ((log.mid, log.logDate), log)
        })
      })
      //将相同key数据聚合到一个分区
      val midanddateDStream: DStream[((String, String), Iterable[StartUpLog])] = midanddate.groupByKey()
      //将数据排序取第一条数据
      val midanddatesort: DStream[((String, String), List[StartUpLog])] = midanddateDStream.mapValues(iter => {
        iter.toList.sortWith(_.ts < _.ts).take(1)
      })
      //将数据扁平化
      midanddatesort.flatMap(_._2)
    }
    value
  }
  //进行批次间去重
  def filterByredis(startUpLogDStream: DStream[StartUpLog],sc:SparkConf)={
    val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
      //创建  redis连接
      val jidisCline = new Jedis("hadoop102", 6379)
      //redis key
      val rediskey = "DAU:" + log.logDate
      //对比数据 将重复的进行过滤
      val boolean: lang.Boolean = jidisCline.sismember(rediskey, log.mid)
      jidisCline.close()
      !boolean
      true
    })
    value
  }
  def saveMidToRedis(startUpLogDStream:DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd =>{
      rdd.foreachPartition(partiton=>{
        //创建连接
        val jedis = new Jedis("hadoop102",6379)
        //写库
        partiton.foreach(log=>{
          val rediskey: String = "DAU:"+log.logDate
          jedis.sadd(rediskey,log.mid)
          jedis.close()
        })

      })
    })
  }

}
