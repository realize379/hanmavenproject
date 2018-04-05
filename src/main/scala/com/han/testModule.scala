package com.han

import org.apache.spark.sql.SparkSession

object testModule {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
    //Load data
    var testData= spark.read.text("c:/spark/README.md")
    //Save data
    testData.
      coalesce(1). // 파일개수
      write.format("csv").  // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      save("test") // 저장파일명
    println("spark test completed")

    // 접속정보 설정
    var staticUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_batch_season_mpara"

    // jdbc (java database connectivity) 연결
    val selloutDataFromPg= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    selloutDataFromPg.createOrReplaceTempView("")

    //데이터저장
    // 데이터베이스 주소 및 접속정보 설정
    var outputUrl = "jdbc:oracle:thin:@192.168.110.9:1522/XE"
    //staticUrl = "jdbc:oracle:thin:@127.0.0.1:1522/XE"
    var outputUser = "haiteam"
    var outputPw = "haiteam"

    // 데이터 저장
    var prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)
    var table = "kopo_channel_seasonality_user"
    //append
    selloutDataFromPg.write.mode("overwrite").jdbc(outputUrl, table, prop)




  }
}

