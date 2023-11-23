package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.{MyKafkaUtils, MyOffsetsUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 日志数据的消费分流
 * 1. 准备实时处理环境 StreamingContext
 *
 * 2. 从Kafka中消费数据
 *
 * 3. 处理数据
 * 3.1 转换数据结构
 * 专用结构  Bean
 * 通用结构  Map JsonObject
 * 3.2 分流
 *
 * 4. 写出到DWD层
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    //1. 准备实时环境
    //TODO 注意并行度与Kafka中topic的分区个数的对应关系
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))


    //2. 从kafka中消费数据
    val topicName: String = "ODS_BASE_RADAR_LOG" //对应生成器配置中的主题名
    val groupId: String = "ODS_BASE_RADAR_LOG_GROUP"


    //TODO  从Redis中读取offset， 指定offset进行消费
    val offsets: Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(topicName, groupId)

    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      //指定offset进行消费
      kafkaDStream =
        MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)

    } else {
      //默认offset进行消费
      kafkaDStream =
        MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)

    }

    // TODO 补充: 从当前消费到的数据中提取offsets , 不对流中的数据做任何处理.
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges // 在哪里执行? driver
        rdd
      }
    )

    //kafkaDStream.print(100)
    //3. 处理数据
    //3.1 转换数据结构
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        //获取ConsumerRecord中的value,value就是数据
        val log: String = consumerRecord.value()
        //转换成Json对象
        val jsonObj: JSONObject = JSON.parseObject(log)
        //返回
        jsonObj
      }
    )
    // jsonObjDStream.print(1000)

    //3.2 分流
    // 日志数据：
    //      信息数据
    //      错误数据
    val DWD_RADAR_DATA_LOG_TOPIC: String = "DWD_RADAR_DATA_LOG_TOPIC" // 信息数据
    val DWD_RADAR_ERROR_LOG_TOPIC: String = "DWD_RADAR_ERROR_LOG_TOPIC" // 错误数据
    //分流规则:
    // 错误数据: 不做任何的拆分， 只要包含错误字段，直接整条数据发送到对应的topic
    // 信息数据: 拆分别发送到对应的topic

    jsonObjDStream.foreachRDD(
      rdd => {

        rdd.foreachPartition(
          jsonObjIter => {
            for (jsonObj <- jsonObjIter) {
              //分流过程
              //分流错误数据
              val errObj: JSONObject = jsonObj.getJSONObject("err")
              if (errObj != null) {
                //将错误数据发送到 DWD_RADAR_ERROR_LOG_TOPIC
                MyKafkaUtils.send(DWD_RADAR_ERROR_LOG_TOPIC, jsonObj.toJSONString)
              } else {

                MyKafkaUtils.send(DWD_RADAR_DATA_LOG_TOPIC, jsonObj.toJSONString)
              }
            }
            //刷写Kafka
            MyKafkaUtils.flush()
          }
        )
        MyOffsetsUtils.saveOffset(topicName, groupId, offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
