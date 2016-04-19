package com.dt.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by root on 4/8/16.
  */
object RDDBasedOnLocalFile {
  def main(args: Array[String]) {
    val conf = new SparkConf() //创建SparkConf对象

    conf.setAppName("RDDBasedOnLocalFile")  //设置应用程序名称，在程序运行的监控页面可以看到名称

    conf.setMaster("local")

    val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkContext实例来定制Spark运行具体参数和配置信息

    val rdd = sc.textFile("/root/data/sampleData")  //创建一个Scala集合

    val linesLength = rdd.map(line => line.length)

    val sum = linesLength.reduce(_+_)

    println("The total characters of the file is : " + sum)
  }

}
