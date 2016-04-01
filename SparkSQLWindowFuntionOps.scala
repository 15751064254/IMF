package com.dt.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object SparkSQLWindowFuntionOps{
	def main(args: Array[String]): Unit = {

		val conf = new SparkConf()  //创建SparkConf对象
		conf.setAppName("SparkSQLWindowFuntionOps") //设置应用程序的名称，在程序运行的监控界面可以看到名称。
		conf.setMaster("spark://Master:7077") //指定程序在Spark集群

    //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息。
		val sc = new SparkContext(conf)

    /**
      * 第一：在目前企业级大数据Spark开发的时候绝大多数情况下是采用Hive作为数据仓库的.
      * Spark提供了Hive的支持功能，Spark通过HiveContext可以直接操作Hive中的数据.
      * 基于HiveContext我们可以使用sql/hql两种方式才编写SQL语句对Hive进行操作，包括创建表、删除表、往表里导入数据以及用SQL语法构造各种SQL语句对表中的数据进行CRUD操作.
      * 第二：我们也可以直接通过saveAsTable的方式把DataFrame中的数据保存到Hive数据仓库中.
      * 第三：可以直接通过HiveContext.table方式来直接加载Hive中的表而生成DataFrame.
      */

		val hiveContext = new HiveContext(sc)

		hiveContext.sql("use hive") //使用名称为hive的数据库，我们接下来所有的表的操作都位于这个库中.

    /**
      * 如果要创建的表存在就删除，然后创建我们要导入数据的表
      */
		hiveContext.sql("DROP TABLE IF EXISTS scores")
		hiveContext.sql("CREATE TABLE IF NOT EXISTS scores(name STRING, score INT) ")
		                +"ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\\n'")

    //把要处理的数据导入Hive的表中
		hiveContext.sql("LOAD DATA LOCAL INPATH '/topNGroup.txt' INTO TABLE scores")


    /**
      * 使用子查询的方式完成目标数据的提取，在目标数据内幕使用窗口函数row_number来进行分组排序
      * PARTITION BY :指定窗口函数分组的Key
      * ORDER BY：分组后进行排序
      */
		val result = hiveContext.sql("SELECT name,score "
			+ "FROM ("
				+ "SELECT "
				+ "name, "
				+ "score, "
				+ "row_number() OVER (PARTITION BY name ORDER BY score DESC) rank"
				+ "FORM scores "
				+ ") sub_scores "
				+ "WHERE rank <= 4 "
		)

    result.show //在Driver的控制台上打印出结果内容

    //把数据保存在Hive数据仓库中
    hiveContext.sql("DROP TABLE IF EXISTS sortedResultScores")
    result.saveAsTable("sortedResultScores")

	}
}
