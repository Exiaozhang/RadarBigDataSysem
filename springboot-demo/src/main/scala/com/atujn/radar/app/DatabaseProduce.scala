package com.atujn.radar.app

import com.atguigu.gmall.realtime.util.MyKafkaUtils

object DatabaseProduce {

  def producedata(json: String) = {
    //数据生产的主题名
    val topicName: String = "ODS_DATABASE_RADAR_LOG" //对应生成器配置中的主题


    MyKafkaUtils.send(topicName, json)

  }

}
