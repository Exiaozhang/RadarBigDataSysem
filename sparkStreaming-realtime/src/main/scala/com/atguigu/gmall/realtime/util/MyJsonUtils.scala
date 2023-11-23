package com.atguigu.gmall.realtime.util

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.spark.sql.SparkSession

import java.io.File
import java.nio.file.{Files, Paths}

object MyJsonUtils {

  def JsonArryToFolatArray(jsonArray: JSONArray): Array[Float] = {

    val floatArray = new Array[Float](jsonArray.size)

    var i = 0
    while ( {
      i < jsonArray.size
    }) {
      floatArray(i) = jsonArray.getFloatValue(i)
      i += 1
    }
    floatArray
  }

  def HdfsJsonConfigReader(key: String): String = {

    val spark = SparkSession.builder().master("local[4]")
      .appName("HdfsJsonReader").getOrCreate()

    // 定义输入文件路径
    val inputPath = "hdfs://hadoop102:9000/Algorithm/config.json" // HDFS上的JSON文件路径

    // 从HDFS中读取JSON文件
    val jsonDF = spark.read.json(inputPath)

    val jsonString = jsonDF.toJSON.collect().mkString("", ",", "")
    val jsonObject = JSON.parseObject(jsonString)
    //println(jsonObject.getString("algorithmJar.rcsf"))

    //spark.stop()
    jsonObject.getString(key)
  }

  def HdfsGetJsonConfig(): String = {
    val spark = SparkSession.builder().master("local[4]")
      .appName("HdfsJsonReader").getOrCreate()

    // 定义输入文件路径
    val inputPath = "hdfs://hadoop102:9000/Algorithm/config.json" // HDFS上的JSON文件路径

    // 从HDFS中读取JSON文件
    val jsonDF = spark.read.json(inputPath)

    val jsonString = jsonDF.toJSON.collect().mkString("", ",", "")

    jsonString
  }

  def HdfsAlgorithmConfigReader(method: String): String = {

    val spark = SparkSession.builder().master("local[4]")
      .appName("HdfsJsonReader").getOrCreate()

    // 定义输入文件路径
    val inputPath = s"hdfs://hadoop102:9000/Algorithm/${method}" // HDFS上的JSON文件路径

    // 从HDFS中读取JSON文件
    val jsonDF = spark.read.json(inputPath)

    val jsonString = jsonDF.toJSON.collect().mkString("", ",", "")

    spark.stop()

    jsonString
  }


  def ReaderLocalJson(path: String): String = {

    val file = new File(path)
    val StringjsonString = new String(Files.readAllBytes(Paths.get(file.getPath)))
    StringjsonString
  }

}
