package com.atguigu.gmall.realtime.app

object Main {
  def main(args: Array[String]): Unit = {

    try {
//      val sparkConf: SparkConf = new SparkConf().setAppName("radar_train_app").setMaster("local[4]")
//      val sc = new SparkContext(sparkConf)
//
//      val config: String = MyJsonUtils.HdfsGetJsonConfig()
//      val jsonConfigObj: JSONObject = JSON.parseObject(config)
//
//      sc.addFile(MyPropsUtils(MyConfig.ALGORITHMJAR_PATH) + jsonConfigObj.getString("algorithmJar." + "rcsf") + ".jar")
//      val str: String = SparkFiles.get(jsonConfigObj.getString("algorithmJar." + "rcsf") + ".jar")
//
//
//      val urls = Array(new URL("file:/" + str))
//      val classLoader = new URLClassLoader(urls)
//      val clazz = classLoader.loadClass("com.ujn.radar.MyJar")
//      val instance = clazz.newInstance()
//      clazz.getMethod("TestMethon").invoke(instance)


    }
    catch {
      case e: Exception => println(e)
    }



    //    //创建SparkSession
    //    val spark = SparkSession.builder().master("local[4]")
    //      .appName("HdfsJsonReader").getOrCreate()
    //    val keyToExtract = "algorithmJar.hdfspath"
    //    // 定义输入文件路径
    //    val inputPath = "hdfs://hadoop102:9000/Algorithm/config.json" // HDFS上的JSON文件路径
    //    // 从HDFS中读取JSON文件
    //    val jsonDF = spark.read.json(inputPath)
    //    val jsonString = jsonDF.toJSON.collect().mkString("", ",", "")
    //    println(jsonString)
    //    spark.stop()
    //    println(HdfsJsonConfigReader("algorithmJar.rcsf"))


  }
}

