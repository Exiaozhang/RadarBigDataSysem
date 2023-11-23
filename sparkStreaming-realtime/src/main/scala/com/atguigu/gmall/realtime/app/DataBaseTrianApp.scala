package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

import java.net.{URL, URLClassLoader}

object DataBaseTrianApp {

  def main(args: Array[String]): Unit = {

    //1. 准备实时环境
    val sparkConf: SparkConf = new SparkConf().setAppName("radar_train_app").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    //2. 从Redis中读取offset
    val topicName: String = "ODS_DATABASE_RADAR_LOG"
    val groupId: String = "ODS_DATABASE_RADAR_GROUP"
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
    //kafkaDStream.print(100)
    //5. 处理数据
    // 转换数据结构
    val jsonObjDStream: DStream[JSONObject] = offsetRangesDStream.map(
      consumerRecord => {
        //获取ConsumerRecord中的value,value就是日志数据
        val log: String = consumerRecord.value()
        //转换成Json对象
        val jsonObj: JSONObject = JSON.parseObject(log)
        //返回
        jsonObj
      }
    )

    jsonObjDStream.foreachRDD(
      rdd => {

        val config: String = MyJsonUtils.HdfsGetJsonConfig()
        val jsonConfigObj: JSONObject = JSON.parseObject(config)

        rdd.foreachPartition(
          jsonObjIter => {
            for (jsonObj <- jsonObjIter) {

              val dataType: String = jsonObj.getString("dataType")
              val algorithm: String = jsonObj.getString("algorithm")

              try {

                //val result: String = MyJsonUtils.HdfsAlgorithmConfigReader(HdfsJsonConfigReader("algorithmJar." + dataType) + HdfsJsonConfigReader(s"algorithmJar.${dataType}.${algorithm}") + "Config.json")
                //val result: String = ""
                val JarName: String = jsonConfigObj.getString("algorithmJar." + dataType)
                val methodName: String = jsonConfigObj.getString("algorithmJar." + dataType + "." + algorithm)

                val scHere = SparkContext.getOrCreate()
                scHere.addFile(MyPropsUtils(MyConfig.ALGORITHMJAR_PATH) + JarName + methodName + "Config.json")
                val configLocalPath: String = SparkFiles.get(JarName + methodName + "Config.json")
                val result = MyJsonUtils.ReaderLocalJson(configLocalPath)

                println(result)

                scHere.addFile(MyPropsUtils(MyConfig.ALGORITHMJAR_PATH) + JarName + ".jar")
                val str: String = SparkFiles.get(JarName + ".jar")

                println(str)

                val urls = Array(new URL("file:/" + str))
                val classLoader = new URLClassLoader(urls)
                val clazz = classLoader.loadClass("com.ujn.radar.MyJar")
                val instance = clazz.newInstance()

                //val value: String = clazz.getMethod("TestMethon").invoke(instance).asInstanceOf[String]
                val value: String = clazz.getMethod(methodName, classOf[String], classOf[String]).invoke(instance, jsonObj.toJSONString, result).asInstanceOf[String]

                //MySqlUtils.SeparatorSave(s"algorithmJar.${dataType}.${algorithm}.table", value)

                println(value)

              }
              catch {
                case e: Exception => print(e)
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
