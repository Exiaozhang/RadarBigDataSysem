package com.atguigu.gmall.realtime.util

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

/**
 * Kafka工具类， 用于生产数据和消费数据
 */
object MyKafkaUtils {



  /**
   * 生产者对象
   */
  val producer : KafkaProducer[String,String] = createProducer()

  /**
   * 创建生产者对象
   */
  def createProducer():KafkaProducer[String,String] = {
    val producerConfigs: util.HashMap[String, AnyRef] = new util.HashMap[String,AnyRef]
    //生产者配置类 ProducerConfig
    //kafka集群位置
    //producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092")
    //producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,MyPropsUtils("kafka.bootstrap-servers"))
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
    //kv序列化器
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , "org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , "org.apache.kafka.common.serialization.StringSerializer")
    //acks
    producerConfigs.put(ProducerConfig.ACKS_CONFIG , "all")
    //batch.size  16kb
    //linger.ms   0
    //retries
    //幂等配置
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG , "true")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](producerConfigs)
    producer
  }

  /**
   * 生产（按照默认的黏性分区策略）
   */
  def send(topic : String  , msg : String ):Unit = {
    producer.send(new ProducerRecord[String,String](topic , msg ))
  }

  /**
   * 生产（按照key进行分区）
   */
  def send(topic : String  , key : String ,  msg : String ):Unit = {
    producer.send(new ProducerRecord[String,String](topic , key ,  msg ))
  }

  /**
   * 关闭生产者对象
   */
  def close():Unit = {
    if(producer != null ) producer.close()
  }

  /**
   * 刷写 ，将缓冲区的数据刷写到磁盘
   *
   */
  def flush(): Unit ={
    producer.flush()
  }
}
