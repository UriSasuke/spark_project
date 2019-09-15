package com.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Enzo Cotter on 2019/9/1.
  */
object ConnectHive {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Connect Hive").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val sql = "select * from user_info";

    val rdd =  sparkSession.sql(sql).rdd
    rdd.foreach(println(_))
//    sparkSession.sql("show databases").collect().foreach(println)

    println("-----------------------------执行结束-----------------------------")
  }
}
