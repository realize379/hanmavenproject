package com.han

object check {
  def main(args: Array[String]): Unit = {

    val spark = sparksession.builder().appName("Hkproject").
      config("spark.master","local").
      getOrCreate()
    var score = 9

    var level = "기타"
    if ( score > 9) {
      level = "수"
      else if( (score > 8) &&
        (score <= 9)){
        level = "우"
        else{
          level = "양"
        }
      }

    }
  }

}
