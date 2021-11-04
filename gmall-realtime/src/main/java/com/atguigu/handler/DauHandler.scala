package com.atguigu.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.StartUpLog.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
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
  def filterByredis(startUpLogDStream: DStream[StartUpLog], sc:SparkContext)={
   /* val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
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
}*/
  def saveMidToRedis(startUpLogDStream:DStream[StartUpLog]) = {
 /*   startUpLogDStream.foreachRDD(rdd =>{
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
    })*/
    //方案二：在分区下创建连接（优化）
/* val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
         //创建redis连接
         val jedisClient: Jedis = new Jedis("hadoop102", 6379)

         val logs: Iterator[StartUpLog] = partition.filter(log => {
           //redisKey
           val rediskey = "DAU:" + log.logDate

           //对比数据，重复的去掉，不重的留下来
           val boolean: lang.Boolean = jedisClient.sismember(rediskey, log.mid)
           !boolean
         })
         //关闭连接
         jedisClient.close()
         logs
       })
       value*/

   //方案三:在每个批次内创建一次连接，来优化连接个数
   val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
   val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
     //1.获取redis连接
     val jedisClient: Jedis = new Jedis("hadoop102", 6379)

     //2.查redis中的mid
     //获取rediskey
     val rediskey = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))

     val mids: util.Set[String] = jedisClient.smembers(rediskey)

     //3.将数据广播至executer端
val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

     //4.根据获取到的mid去重
     val midFilterRDD: RDD[StartUpLog] = rdd.filter(log => {
       !midBC.value.contains(log.mid)
     })

     //关闭连接
     jedisClient.close()
     midFilterRDD
   })
   value

 }

  }

}}
