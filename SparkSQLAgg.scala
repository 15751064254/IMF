package com.dt.spack.sql

import org.apache.spark.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._

/**
  * 使用Scala开发集群运行的Spark WordCount程序
  * @author DT大数据梦工厂
  * 新浪微博：http://weibo.com/ilovepains/
  * 
  * 使用Spark SQL 中的内置函数对数据进行分析，Spark SQL API不同的是，DataFrame中的内置函数操作的结果是返回一个Column对象，而DataFrame天生就是 "A distributed collection of data organized into named columns",这就是为数据的复杂分析建立了坚实的基础，并提供了极大的方便性，例如说，我们操作DataFrame的方法中可以随时调用内置函数进行业务需要的处理，这之于我们构建复杂的业务逻辑而言是可以极大的减少不必须的时间消耗（基本上就是实际模型的映射）,让我们聚焦在数据分析上，这对于提高工程师的生产力而言是非常有价值的
  * Spark 1.5.x 开始提供了大量的内置函数，例如agg:
  * def agg(aggExpr: String, String), aggExprs:(String, String)*): DataFrame = {
  *   groupBy().agg(aggExpr, aggExprs : _*)
  * }
  * 还有 max, mean, min, sum, avg, explode, size, sort_array, day, to_date, abs, acros, asin, atan
  * 总体上而言内置函数包含了五大基本类型
  * 1.聚合函数，例如：countDistinct， sumDistinct 等
  * 2.集合函数，例如：sort_array, explode 等
  * 3.日期，时间函数，例如：hour, quarter, next_day 等
  * 4.数学函数，例如：asin, atan, sqrt, tan, round 等
  * 5.开窗函数，例如：rowNumber 等
  * 6.字符函数，例如：concat, format_number, rexexp_extract
  * 7.其它函数，例如：isNaN, sha, randn, callUDF
  */

object SparkSQLAgg{
  def main(args: Array[String]){

    /**
      * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息
      * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设置为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差
      */
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("SparkSQLInnerFunctions") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.setMaster("spark://Master:7077") //程序在Spark集群
    //conf.serMaster("local")

    /**
      * 第2步：创建SparkContext对象
      * SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala、Java、Python、R等都必须有一个SparkContext
      * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括DAGScheduler、TaskScheduler、SchedulerBackend，同时还会负责Spark程序往Master注册程序等
      * SparkContext是整个Spark应用程序中最为至关重要的一个对象
      */
    val sc = new SparkConf(conf)  //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息
    val sqlContext = new SQLContext(sc) //构建SQL上下文

    //要使用Spark SQL的内置函数，就一定要导入SQLContext下的隐式转换
    import sqlContext.implicits._


    /**
      * 第三步：模拟电商访问的数据，实际情况会比模拟数据复杂很多，最后生成RDD
      */
    val userData = Array(
        "2016-3-27,001,http://spark.apache.org/,1000",
        "2016-3-27,001,http://hadoop.apache.org/,1001",
        "2016-3-27,002,http://fink.apache.org/,1002",
        "2016-3-28,003,http://kafka.apache.org/,1020",
        "2016-3-28,004,http://spark.apache.org/,1010",
        "2016-3-28,002,http://hive.apache.org/,1200",
        "2016-3-28,001,http://parquet.apache.org/,1500",
        "2016-3-28,001,http://spark.apache.org/,1800"
    )

    val userDataRDD = sc.parallelize(userData)  //生成RDD分布式集合对象


    /**
      * 第四步：根据业务需要对数据进行预处理生成DataFrame，要想把RDD转换成DataFrame，需要先把RDD中的元素类型变成Row类型，于此同时要提供DataFrame中的Columns的元数据信息描述
      */
    val userDataRDDRow = userDataRDD.map(row => {val splited = row.split(","); Row(splited(0), splited(1).toInt, splited(2), splited(3).toInt)})

    val userDataDF = sqlContext.createDataFrame(userDataRDDRow, structTypes)


    /**
      * 第五步：使用Spark SQL提供的内置函数对DataFrame进行操作，特别注意：内置函数生成的Column对象且自定进行CG(代码生成)
      */
    userDataDF.groupBy("time").agg('time, countDistinct('id)).map(row => Row(row(1), row(2))).collect.foreach(println)

    userDataDF.groupBy("time").agg('time, sum('amount)).show

  }
}
