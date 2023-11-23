package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.util.MyKafkaUtils

import java.util.{Date, Random, Timer, TimerTask}




/**
 * 模拟日志数据生成
 */
object BaseLogProduceApp {
  def main(args: Array[String]): Unit = {


    //数据生产的主题名
    val topicName: String = "ODS_BASE_RADAR_LOG" //对应生成器配置中的主题

    //创建定时器
    val timer = new Timer()

    val Radartype = Array("type1", "type2", "type3")
    val rand = new Random(System.currentTimeMillis())

    //每秒生成一次数据
    timer.schedule(new TimerTask {
      override def run(): Unit = {
        //创建data的JSON对象
        var Radardata = new JSONObject()
        val Commondata = new JSONObject()
        var i = scala.util.Random.nextFloat() * scala.util.Random.nextInt(100)

        val random_index = rand.nextInt(Radartype.length)
        val radartype = Radartype(random_index)

        Commondata.put("radartype",radartype)
        Commondata.put("data", i)

        if (scala.util.Random.nextFloat() > 0.01) {

        }
        else {
          val Errordata = new JSONObject()
          Errordata.put("status", true)
          Radardata.put("err", Errordata)
        }
        Radardata.put("common", Commondata)
        Radardata.put("ts", getNowDate())
        //将JSON对象转化为String
        val data: String = JSON.toJSONString(Radardata, new SerializeConfig(true))


        MyKafkaUtils.send(topicName, data)
      }
    }, 1000, 1000)
    
    /**
     * 获取时间戳
     *
     * @return
     */
    def getNowDate(): Long = {
      var now: Date = new Date()
      now.getTime
    }

  }
}
