package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer

/**
 * 1. 准备实时环境
 * 2. 从Redis中读取偏移量
 * 3. 从kafka中消费数据
 * 4. 提取偏移量结束点
 * 5. 处理数据
 * 6. 写入Mysql
 * 7. 提交offsets
 */
object DwdDauApp {

  def main(args: Array[String]): Unit = {

    //1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //2. 从Redis中读取offset
    val topicName: String = "DWD_RADAR_DATA_LOG_TOPIC"
    val groupId: String = "DWD_RADAR_DAU_GROUP"
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    //3. 从Kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)
    }

    //4. 提取offset结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //5. 处理数据
    // 5.1 转换结构
    val pageLogDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val dataLog: JSONObject = JSON.parseObject(value)
        dataLog
      }
    )


    //维度关联+算法
    val dauInfoDStream: DStream[JSONObject] = pageLogDStream.mapPartitions(
      dataLogIter => {
        val dauInfos: ListBuffer[JSONObject] = ListBuffer[JSONObject]()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (dataLog <- dataLogIter) {

          //算法

          dauInfos.append(dataLog)
        }
        jedis.close()
        dauInfos.iterator
      }
    )
    //dauInfoDStream.print(100)

    dauInfoDStream

    //写入到OLAP中
    //按照天分割索引，通过索引模板控制mapping、settings、aliases等.
    //准备ES工具类
    dauInfoDStream.foreachRDD(
      rdd => {
        //MySqlUtils.SeparatorSave(doc._1, doc._2.toString())

        val jsonObject: JSONObject = JSON.parseObject(MyJsonUtils.HdfsGetJsonConfig())

        rdd.foreachPartition(
          dauInfoIter => {
            val docs: List[(JSONObject, JSONObject)] = {
              dauInfoIter.map(dauInfo => (jsonObject, dauInfo)).toList
            }
            docs.map {
              doc => {
                MySqlUtils.SeparatorSave(doc._1,s"algorithmJar.${doc._2.getString("dataType")}.table",doc._2.toString())
              }
            }
          }
        )
        //提交offset
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
