package com.atguigu.gmall.realtime.util


import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.bean.{DataTimeInfo, RcsfDataTimeInfo}

import java.sql.{Connection, DriverManager, Timestamp}
import java.time.LocalDateTime
import java.util
import scala.reflect.runtime.universe._

object MySqlUtils {

  def main(args: Array[String]): Unit = {


  }


  /** Sql客户端对象 */
  var connection: Connection = build()

  /** 创建Sql客户端对象 */
  def build(): Connection = {
    // 连接到端口号为3306的mysql数据库
    val url = "jdbc:mysql://hadoop102:3306/radar"
    val driver = "com.mysql.jdbc.Driver"
    val username = "root"
    val password = "000000"

    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
    } catch {
      case e: Exception => e.printStackTrace
    }
    connection
  }

  /** 关闭Sql对象 */
  def close(): Unit = {
    if (connection != null) connection.close()
  }

  /** 生成雪花Id * */
  val mySnowIdUtils: MySnowIdUtils = new MySnowIdUtils(1)

  /**
   * 1. 批写
   */
  def bulkSave(indexName: String, docs: List[(String, DataTimeInfo)]): Unit = {

    for ((docId, docObj) <- docs) {
      try {
        val sql: String = s"INSERT INTO `$indexName` (`data`, `ts`,`radartype`,`dt`,`hr`) VALUES (?, ?,?,?,?)"
        val prep = connection.prepareStatement(sql)
        prep.setFloat(1, docObj.data)
        prep.setLong(2, docObj.ts)
        prep.setString(3, docObj.radartype)
        prep.setString(4, docObj.dt)
        prep.setString(5, docObj.hr)
        prep.executeUpdate
      } catch {
        case e: Exception => print(e)
          val sql: String = s"CREATE TABLE `$indexName`( `data` FLOAT, `ts` BIGINT, `radartype` TEXT, `dt` TEXT, `hr` TEXT );"
          val prep = connection.prepareStatement(sql)
          prep.executeUpdate
          bulkSave(indexName, docs)
      }
    }

  }

  /**
   * 2. 单写
   */
  def separateSave(indexName: String, docs: (String, DataTimeInfo)): Unit = {

    try {
      val sql: String = s"INSERT INTO `$indexName` (`data`, `ts`,`radartype`,`dt`,`hr`) VALUES (?, ?,?,?,?)"
      val prep = connection.prepareStatement(sql)
      prep.setFloat(1, docs._2.data)
      prep.setLong(2, docs._2.ts)
      prep.setString(3, docs._2.radartype)
      prep.setString(4, docs._2.dt)
      prep.setString(5, docs._2.hr)
      prep.executeUpdate
    } catch {
      case e: Exception => print(e)
        val sql: String = s"CREATE TABLE `$indexName`( `data` FLOAT, `ts` BIGINT, `radartype` TEXT, `dt` TEXT, `hr` TEXT );"
        val prep = connection.prepareStatement(sql)
        prep.executeUpdate
        separateSave(indexName, docs)
    }

  }

  /**
   * 查询指定的字段
   */
  def searchFiled(indexName: String, fieldName: String) = {

    val statement = connection.createStatement()


    val sql: String = s"SELECT $fieldName FROM `$indexName`"
    val resultSet = statement.executeQuery(sql)
    while (resultSet.next()) {
      val message = resultSet.getObject(fieldName)

      println(" message= " + message)
    }
  }

  def BulkSave(datatype: String, docs: (String, DataTimeInfo)): Unit = {
  }

  def SeparatorSave(indexName: String, docs: (String, RcsfDataTimeInfo)): Unit = {

    try {
      val mirror = runtimeMirror(docs._2.getClass.getClassLoader)

      val fields = docs._2.getClass.getDeclaredFields
      val id = mySnowIdUtils.nextId
      var floatData: List[(String, Array[Float])] = List[(String, Array[Float])]()

      val now: LocalDateTime = LocalDateTime.now()
      val timestamp: Timestamp = java.sql.Timestamp.valueOf(now)
      val sqlTime = timestamp.toString

      InsertValue("rcs_config", "id", "", id.toString())
      UpdateValue("rcs_config", "gmt_create", sqlTime, "id", id.toString())

      fields.foreach { field =>
        field.setAccessible(true)
        val fieldName: String = field.getName
        if (field.getType == Array[Float]().getClass) {
          val datas: Array[Float] = field.get(docs._2).asInstanceOf[Array[Float]]
          println(datas.length)
          floatData = floatData :+ (fieldName, datas)
        }
        else {
          //println(fieldName + "|" + field.get(docs._2).toString())
          UpdateValue("rcs_config", fieldName, field.get(docs._2).toString(), "id", id.toString())
        }
      }

      val dataLength = floatData(0)._2.length;
      val dataMount = floatData.size

      for (i <- 0 to dataLength - 1) {
        var dataFieldNames: String = "id,gmt_create,"
        var dataFieldValues: String = s"$id,'$sqlTime',"
        for (p <- 0 to dataMount - 1) {
          if (p == (dataMount - 1)) {
            dataFieldNames = dataFieldNames + floatData(p)._1
            dataFieldValues = dataFieldValues + floatData(p)._2(i)
          } else {
            dataFieldNames = dataFieldNames + floatData(p)._1 + ","
            dataFieldValues = dataFieldValues + floatData(p)._2(i) + ","
          }
        }
        InsertValue("rcs_data", dataFieldNames, "", dataFieldValues)
        dataFieldNames = "id,gmt_create,"
        dataFieldValues = s"$id,'$sqlTime',"
      }

    } catch {
      case e: Exception => print(e)

    }

  }

  def InsertValue(indexName: String, fieldName: String, fieldType: String, fieldValue: String): Unit = {
    val sql: String = s"INSERT INTO $indexName ($fieldName) VALUES ($fieldValue)"
    println(sql)
    val prep = connection.prepareStatement(sql)
    prep.executeUpdate
  }

  def UpdateValue(indexName: String, fieldName: String, fieldValue: String, conditionName: String, conditionValue: String): Unit = {
    val sql: String = s"UPDATE $indexName SET $fieldName = '$fieldValue' WHERE $conditionName = '$conditionValue'"
    val prep = connection.prepareStatement(sql)
    prep.executeUpdate
  }

  //废弃
  def SeparatorSave(tableConfigKey: String, json: String): Unit = {


    //println(json)

    val jsonObject: JSONObject = JSON.parseObject(json)
    val configJsonObj: JSONObject = jsonObject.getJSONObject("config")
    val dataJsonObj: JSONObject = jsonObject.getJSONObject("data")

    val id = mySnowIdUtils.nextId
    var floatData: List[(String, Array[Float])] = List[(String, Array[Float])]()

    val now: LocalDateTime = LocalDateTime.now()
    val timestamp: Timestamp = java.sql.Timestamp.valueOf(now)
    val sqlTime = timestamp.toString

    val configTable: String = MyJsonUtils.HdfsJsonConfigReader(tableConfigKey + ".config")
    val dataTable: String = MyJsonUtils.HdfsJsonConfigReader(tableConfigKey + ".data")

    InsertValue(configTable, "id_pr", "", id.toString())
    UpdateValue(configTable, "gmt_create", sqlTime, "id_pr", id.toString())

    configJsonObj.keySet().forEach(key => {
      val value = configJsonObj.getString(key)
      println(key)
      UpdateValue(configTable, key, value.toString(), "id_pr", id.toString())
    })

    val dataCount: Int = dataJsonObj.keySet().size()

    var arraysName = new util.ArrayList[String]()
    var arraysData = new util.ArrayList[JSONArray]()

    dataJsonObj.keySet().forEach(key => {
      val value = dataJsonObj.getJSONArray(key)
      arraysData.add(value)
      arraysName.add(key)
    })

    for (i <- 0 to arraysData.get(0).size() - 1) {
      var dataFieldNames: String = "id_pr,gmt_create,"
      var dataFieldValues: String = s"$id,'$sqlTime',"

      for (p <- 0 to arraysData.size() - 1) {

        if (p == (arraysData.size() - 1)) {
          dataFieldNames = dataFieldNames + arraysName.get(p)
          dataFieldValues = dataFieldValues + arraysData.get(p).get(i)
        } else {
          dataFieldNames = dataFieldNames + arraysName.get(p) + ","
          dataFieldValues = dataFieldValues + arraysData.get(p).get(i) + ","
        }
      }

      InsertValue(dataTable, dataFieldNames, "", dataFieldValues)
    }

  }


  def SeparatorSave(config: JSONObject, tableConfigKey: String, json: String): Unit = {


    //println(json)

    val jsonObject: JSONObject = JSON.parseObject(json)
    val configJsonObj: JSONObject = jsonObject.getJSONObject("config")
    val dataJsonObj: JSONObject = jsonObject.getJSONObject("data")

    val id = mySnowIdUtils.nextId
    var floatData: List[(String, Array[Float])] = List[(String, Array[Float])]()

    val now: LocalDateTime = LocalDateTime.now()
    val timestamp: Timestamp = java.sql.Timestamp.valueOf(now)
    val sqlTime = timestamp.toString

    val configTable: String = config.getString(tableConfigKey + ".config")
    val dataTable: String = config.getString(tableConfigKey + ".data")

    println(configTable)
    println(dataTable)

    InsertValue(configTable, "id", "", id.toString())
    UpdateValue(configTable, "gmt_create", sqlTime, "id", id.toString())

    val keys: util.Set[String] = configJsonObj.keySet()


    configJsonObj.keySet().forEach(key => {
      val value = configJsonObj.getString(key)
      UpdateValue(configTable, key, value.toString(), "id", id.toString())
    })

    val dataCount: Int = dataJsonObj.keySet().size()

    var arraysName = new util.ArrayList[String]()
    var arraysData = new util.ArrayList[JSONArray]()

    dataJsonObj.keySet().forEach(key => {
      val value = dataJsonObj.getJSONArray(key)
      arraysData.add(value)
      arraysName.add(key)
    })

    for (i <- 0 to arraysData.get(0).size() - 1) {
      var dataFieldNames: String = "id,gmt_create,"
      var dataFieldValues: String = s"$id,'$sqlTime',"

      for (p <- 0 to arraysData.size() - 1) {

        if (p == (arraysData.size() - 1)) {
          dataFieldNames = dataFieldNames + arraysName.get(p)
          dataFieldValues = dataFieldValues + arraysData.get(p).get(i)
        } else {
          dataFieldNames = dataFieldNames + arraysName.get(p) + ","
          dataFieldValues = dataFieldValues + arraysData.get(p).get(i) + ","

        }
      }

      InsertValue(dataTable, dataFieldNames, "", dataFieldValues)
    }

  }

}
