package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Dauapp {
  def main(args: Array[String]): Unit = {
//创建sparkconf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    //创建ssc
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    //连接Kafka，消费Kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)
    /*kafkaDStream.foreachRDD(rdd=>{
      rdd.foreach(record=>{
        println(record.value())
      })
    })*/

    //将JSON转化为样例类 补全字段
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(
      partition => {
        partition.map(recode => {
          val startUpLog: StartUpLog = JSON.parseObject(recode.value(), classOf[StartUpLog])
          //yyyy-MM-dd HH
          val str: String = sdf.format(new Date(startUpLog.ts))
          //补充字段 logdate yyyy-MM-dd
          startUpLog.logDate = str.split(" ")(0)
          //补充字段 loghour HH
          startUpLog.logHour = str.split(" ")(1)
          startUpLog
        })
      })
    /*startUpLogDStream.cache()*/

    startUpLogDStream.count().print()
    //批次间去重
    val filterByredis: DStream[StartUpLog] = DauHandler.filterByredis(startUpLogDStream,sparkConf)
    //批次内去重
    val filterBygroup: DStream[StartUpLog] = DauHandler.filterByGroup(filterByredis)
    filterBygroup.cache()
    filterBygroup.count().print()
    //将去重后的数据 保存到redis中
    DauHandler.saveMidToRedis(filterBygroup)
    //将数据保存到hbase
    filterBygroup.foreachRDD(rdd=>{
      rdd.saveToPhoenix(
        "GMALL2021_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

   ssc.start()
    ssc.awaitTermination()

  }

}
