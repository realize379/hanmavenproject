package com.han

import org.apache.spark.sql.SparkSession
object fdfd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hanmavenproject").
      config("spark.master","local").
      getOrCreate()

    var staticUrl = "jdbc:mysql://192.168.110.70:3306/kopo"
    var staticUser = "haiteam"
    var staticPw = "haiteam"
    var selloutDb = "KOPO_PRODUCT_VOLUME"

    // jdbc (java database connectivity)
    val selloutDataFromMysql= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromMysql.createOrReplaceTempView("selloutTable")
selloutDataFromMysql.show(1)
  }

}
