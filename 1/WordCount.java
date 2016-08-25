package com.dt.spark.SparkWordCount;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
  * 使用Java的方式开发进行本地测试Spark的WordCount程序
  * @author DT大数据梦工厂
  * http://weibo.com/ilovepains
  *
  */

public class WordCount {
  public static void main(String[] args){
    /**
      * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
      * 例如说通过setMaster来设置程序要连接的Spark集群的Master的URL，如果设置
      * 为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差的学者
      */
    SparkConf conf = new SparkConf().setAppName("Spark WordCount written by Java").setMaster("local");

    /**
      * 第2步：创建SparkContext对象
      * SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala、Java、Python、R等都必须有一个SparkContext(不同的语言具体的类名不同，如果是Java的话则为JavaSparkContext)
      * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括DAGScheduler、TaskScheduler、SchedulerBackend，同时还会负责Spark程序往Master注册程序等 
      * SparkContext是整个Spark应用程序中最为至关重要的一个对象
      */
    JavaSparkContext sc = new JavaSparkContext(conf); //其底层实际上就是Scala的SparkContext

    /**
      * 第3步：根据具体的数据来源（HDFS， HBase， Local FS， DB， S3 等）通过JavaSparkContext来创建JavaRDD
      * JavaRDD的创建基本有三种方式：根据外部的数据来源（例如HDFS），根据Scala集合，由其它的JavaRDD操作数据会被
      * JavaRDD划分成为一系列的 Partitions，分配到每个Partition的数据属于一个Task的处理范畴
      */
    JavaRDD<String> lines = sc.textFile("/root/data/input");

    /**
      * 第4步：对初始的JAVARDD进行Transformation级别处理，例如 map， filter 等高阶函数的编程，来进行具体的数据计算
      *   第4.1步：讲每一行的字符串拆分成每个的单词(如果是Scala，由于SAM转换，可以写成 val words = lines.flatMap{ line => line.split(" ")} )
      */
    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>(){

        @Override
        public Iterable<String> call(String line) throws Exception {
          return Arrays.asList(line.split(" "));
        }
    });

    /**
      * 第4.2步：在单词拆分的基础上对每个单词实例计数为1, 也就是 word => (word, 1)
      */
    JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>(){

		@Override
		public Tuple2<String, Integer> call(String word) throws Exception {
			return new Tuple2<String, Integer>(word, 1);
		}
    	
    });

    /**
      * 第4.3步：在每个单词实例计数为1基础之上统计每个单词在文件中出现的总次数
      */
    JavaPairRDD<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){
        @Override
        public Integer call(Integer v1, Integer v2) throws Exception {
          return v1 + v2;
        }
    });

    wordsCount.foreach(new VoidFunction<Tuple2<String, Integer>>(){
        @Override
        public void call(Tuple2<String, Integer> pairs) throws Exception {
          System.out.println(pairs._1 + " : " + pairs._2);
        }
    });

    sc.close();
    
  }
}
