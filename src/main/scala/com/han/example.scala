package com.han
import org.apache.spark.sql.SparkSession
object example {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var selloutDb2 = "kopo_region_mst"

    // jdbc (java database connectivity) 연결
    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load
    val selloutDataFromOracle2 = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb2, "user" -> staticUser, "password" -> staticPw)).load
    //selloutData=바꿔야 하는값
    //jdbc는설정
    //val 고정 변수


    selloutDataFromOracle.createOrReplaceTempView("selloutTable")
    selloutDataFromOracle2.createOrReplaceTempView("selloutTable2")
    //creater
    selloutDataFromOracle.show(2)
    selloutDataFromOracle2.show(2)


    // table 간 조인

    selloutDataFromOracle.createOrReplaceTempView("selloutTable")
    selloutDataFromOracle2.createOrReplaceTempView("selloutTable2")

    var leftJoinData = spark.sql("select a.*, b.* " +
      "from selloutTable a left outer join selloutTable2 b " +
      "on a.regionId = b.regionId")

    var InnerJoinData = spark.sql("select a.*, b.* " +
      "from selloutTable a inner join selloutTable2 b " +
      "on a.regionId = b.regionId")
  }
}