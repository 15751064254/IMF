package com.dt.spark.Streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

import scala.Tuple2;

public class WordCountOnHDFS {

	public static void main(String[] args) {

		/*
		 * 第一步：配置SparkConf： 1，至少2条线程：因为Spark Streaming应用程序在运行的时候，至少有一条
		 * 线程用于不断的循环接收数据，并且至少有一条线程用于处理接受的数据（否则的话无法
		 * 有线程用于处理数据，随着时间的推移，内存和磁盘都会不堪重负）；
		 * 2，对于集群而言，每个Executor一般肯定不止一个Thread，那对于处理Spark Streaming的
		 * 应用程序而言，每个Executor一般分配多少Core比较合适？根据我们过去的经验，5个左右的
		 * Core是最佳的（一个段子分配为奇数个Core表现最佳，例如3个、5个、7个Core等）；
		 */
		/*
		 * SparkConf conf = new SparkConf().setMaster("local[2]").
		 * setAppName("WordCountOnline");
		 */

		final SparkConf conf = new SparkConf().setMaster("spark://Master:7077").setAppName("WordCountOnHDFS");

		/*
		 * 第二步：创建SparkStreamingContext： 1，这个是SparkStreaming应用程序所有功能的起始点和程序调度的核心
		 * SparkStreamingContext的构建可以基于SparkConf参数，
		 * 也可基于持久化的SparkStreamingContext的内容 来恢复过来（典型的场景是Driver崩溃后重新启动，由于Spark
		 * Streaming具有连续7*24小时不间断运行的特征，
		 * 所有需要在Driver重新启动后继续上衣系的状态，此时的状态恢复需要基于曾经的Checkpoint）； 2，在一个Spark
		 * Streaming应用程序中可以创建若干个SparkStreamingContext对象，
		 * 使用下一个SparkStreamingContext 之前需要把前面正在运行的SparkStreamingContext对象关闭掉，由此，
		 * 我们获得一个重大的启发SparkStreaming框架也只是 Spark Core上的一个应用程序而已，只不过Spark
		 * Streaming框架箱运行的话需要Spark工程师写业务逻辑处理代码；
		 */

		// JavaStreamingContext jsc = new
		// JavaStreamingContext(conf,Durations.seconds(5));

		// hdfs dfs -mkdri /library/SparkStreaming
		// hdfs dfs -mkdir /library/SparkStreaming/Data
		// hdfs dfs -mkdir /library/SparkStreaming/CheckPointData
		final String checkpointDirectory = "hdfs://mycluster:8020/library/SparkStreaming/CheckPointData";

		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {

			@Override
			public JavaStreamingContext create() {
				// TODO Auto-generated method stub
				return createContext(checkpointDirectory, conf);
			}
		};

		/**
		 * 可以从失败中回复Driver,不过还需要制定Driver这个进程运行在Cluster,并且提交应用程序的时候制定--supervise;
		 */
		JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);

		/*
		 * 第三步：创建Spark Streaming输入数据来源input Stream：
		 * 1，数据输入来源可以基于File、HDFS、Flume、Kafka、Socket等 2,
		 * 在这里我们指定数据来源于网络Socket端口，Spark Streaming连接上该端口并在运行的时候一直监听该端口
		 * 的数据（当然该端口服务首先必须存在）,并且在后续会根据业务需要不断的有数据产生(当然对于Spark Streaming
		 * 应用程序的运行而言，有无数据其处理流程都是一样的)；
		 * 3,如果经常在每间隔5秒钟没有数据的话不断的启动空的Job其实是会造成调度资源的浪费，因为并没有数据需要发生计算，所以
		 * 实例的企业级生成环境的代码在具体提交Job前会判断是否有数据，如果没有的话就不再提交Job；
		 * 4,此处没有Receiver,SparkStreaming应用程序只是按照时间间隔监控目录下每个Batch新增的内容(把新增的)
		 * 作为RDD的数据来源生产原始RDD
		 */

		// JavaReceiverInputDStream lines =
		// jsc.socketTextStream("localhost",9999);

		JavaDStream lines = jsc.textFileStream("hdfs://mycluster:8020/library/SparkStreaming/Data");

		/*
		 * 第四步：接下来就像对于RDD编程一样基于DStream进行编程！！！原因是DStream是RDD产生的模板（或者说类），在Spark
		 * Streaming具体 发生计算前，其实质是把每个Batch的DStream的操作翻译成为对RDD的操作！！！
		 * 对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
		 * 第4.1步：讲每一行的字符串拆分成单个的单词
		 */
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() { // 如果是Scala，由于SAM转换，所以可以写成val
																							// words
																							// =
																							// lines.flatMap
																							// {
																							// line
																							// =>
																							// line.split("
																							// ")}

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
		});

		/*
		 * 第四步：对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
		 * 第4.2步：在单词拆分的基础上对每个单词实例计数为1，也就是word => (word, 1)
		 */
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		/*
		 * 第四步：对初始的DStream进行Transformation级别的处理，例如map、filter等高阶函数等的编程，来进行具体的数据计算
		 * 第4.3步：在每个单词实例计数为1基础之上统计每个单词在文件中出现的总次数
		 */
		JavaPairDStream<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() { // 对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		/*
		 * 此处的print并不会直接出发Job的执行，因为现在的一切都是在Spark Streaming框架的控制之下的，对于Spark
		 * Streaming 而言具体是否触发真正的Job运行是基于设置的Duration时间间隔的
		 * 
		 * 诸位一定要注意的是Spark Streaming应用程序要想执行具体的Job，对Dtream就必须有output Stream操作，
		 * output
		 * Stream有很多类型的函数触发，类print、saveAsTextFile、saveAsHadoopFiles等，最为重要的一个
		 * 方法是foraeachRDD,因为Spark
		 * Streaming处理的结果一般都会放在Redis、DB、DashBoard等上面，foreachRDD
		 * 主要就是用用来完成这些功能的，而且可以随意的自定义具体数据到底放在哪里！！！
		 *
		 */
		wordsCount.print();

		/*
		 * Spark
		 * Streaming执行引擎也就是Driver开始运行，Driver启动的时候是位于一条新的线程中的，当然其内部有消息循环体，用于
		 * 接受应用程序本身或者Executor中的消息；
		 */
		jsc.start();

		jsc.awaitTermination();
		jsc.close();

	}

	private static JavaStreamingContext createContext(String checkpointDirectory, SparkConf sparkConf) {

		// If you do not see this printed, that means the StreamingContext has
		// been loaded
		// from the new checkpoint
		System.out.println("Creating new context");

		// Create the context with a 1 second batch size
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(15));

		ssc.checkpoint(checkpointDirectory);

		return ssc;
	}
	
	/*
	 * SparkStreaming.sh
	 *	bin/spark-submit --class com.dt.spark.Streaming.WordCountOnHDFS --files hive/conf/hive-site.xml --driver-class-path hive/lib/mysql-connector-java-5.1.38-bin.jar --master spark://hadoop-namenode01:7077,hadoop-namenode02:7077 WordCountOnHDFS.jar
	 *
	 * hdfs dfs -put /root/data/sampleData /library/SparkStreaming/Data
	 */
}
