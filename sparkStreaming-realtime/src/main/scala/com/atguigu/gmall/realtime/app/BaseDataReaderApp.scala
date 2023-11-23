package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.{RcsfDataConfig, RcsfDataData}
import com.atguigu.gmall.realtime.util.MyKafkaUtils

import java.io.File
import java.nio.charset.Charset
import java.util
import java.util.{Timer, TimerTask}
import scala.Console.println
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.{Codec, Source}
import scala.util.control.Breaks.{break, breakable}

object BaseDataReaderApp {
  def canConvertToFloat(str: String): Boolean = {
    try {
      val floatValue = str.toFloat
      true
    } catch {
      case _: NumberFormatException => false
    }
  }

  def main(args: Array[String]): Unit = {
    //数据生产的主题名
    val topicName: String = "ODS_BASE_RADAR_LOG" //对应生成器配置中的主题

    val filename = "C:\\Users\\Administrator\\Desktop\\RCS\\Type1\\111D36XH.rcsF"

    val dir = new File("C:\\Users\\Administrator\\Desktop\\RCS\\Type1")

    val files = dir.listFiles.filter(_.isFile).toList
    files.foreach(file => {
      println(file.getName())
      println(file.getAbsolutePath)
    })

    var fileSource = Source.fromFile(filename)(Codec(Charset.forName("GBK")))

    var datas: Array[String] = Array[String]()
    var temp: Array[String] = Array[String]()
    val fileData: Array[String] = fileSource.getLines().toArray
    var isData: Boolean = false
    var isFloatData: Boolean = false
    var fo = Array[Float]()
    var rcs = Array[Float]()
    var phi = Array[Float]()
    var f = new util.ArrayList[Float]()
    var r = new util.ArrayList[Float]()
    var p = new util.ArrayList[Float]()

    fileData.foreach(str => {
      breakable {
        if (str.contains("[TASK]") || str.contains("[SYSTEM]") || str.contains("[CONDITIONS]")) {
          isData = true;
          break
        }
        if (str.contains("[/TASK]") || str.contains("[/SYSTEM]") || str.contains("[/CONDITIONS]")) {
          isData = false
          break
        }
        if (!isData) {
          break
        }
        temp = str.split("=")
        if (temp.length > 1)
          datas = datas :+ temp(1)
        else
          datas = datas :+ ""
      }
    })

    isData = false;

    fileData.foreach(str => {
      breakable {
        if (str.contains("[DATA]")) {
          isData = true;
          break
        }
        if (str.contains("[/DATA]")) {
          isFloatData = false;
          isData = false
          break
        }
        if (isData && isFloatData) {
          temp = str.split("\t")
          f.add(temp(0).toFloat)
          r.add(temp(1).toFloat)
          p.add(temp(2).toFloat)
          break()
        }
        if (!isData) {
          break
        } else {
          isFloatData = true
          break
        }

      }
    })

    fileSource.close()

    fo = f.asScala.toArray
    rcs = r.asScala.toArray
    phi = p.asScala.toArray

    println(datas.length)
    println(fo.length)


    val rcsfConfig: RcsfDataConfig = RcsfDataConfig("Hello", "1", "2", "3", datas(0), datas(1), datas(2), datas(3), datas(4), datas(5),
      datas(6), datas(7), datas(8), datas(9), datas(10), datas(11), datas(12), datas(13), datas(14), datas(15), datas(16),
      datas(17), datas(18), datas(19), datas(20), datas(21), datas(22), datas(23), datas(24), datas(25), datas(26), datas(27),
      datas(28), datas(29), datas(30), datas(31), datas(32), datas(33), datas(34), datas(35), datas(36), datas(37),
      datas(38), datas(39), datas(40), datas(41), datas(42), datas(43), datas(44))

    val rcsfDta: RcsfDataData = RcsfDataData(fo, rcs, phi)


    val config: String = JSON.toJSONString(rcsfConfig, new SerializeConfig(true))
    val data: String = JSON.toJSONString(rcsfDta, new SerializeConfig(true))

    //println(rcsfDataJsonString)

    var jsonObject: JSONObject = new JSONObject()
    jsonObject.put("dataType","rcsf")
    jsonObject.put("config", config)
    jsonObject.put("data", data)

    MyKafkaUtils.send(topicName, jsonObject.toJSONString)

    val timer = new Timer()
    timer.schedule(new TimerTask {
      override def run(): Unit = {
        println("Wait for new file")
      }
    }, 1000, 1000)

  }


}
