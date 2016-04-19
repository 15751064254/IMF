
package com.dt.spark.SparkApps.sql;

import org.apache.spark.SparkConf;

public class SparkSQLUserLogsOps {
  public static void main(String[] args){
    SparkConf conf = new SparkConf.setMaster("local").setAppName("SparkSQLwithJoin");
    JavaSparkContext sc = new JavaSparkContext(conf);
    HiveContext hiveContext = new HiveContext(sc.sc());

    String twodaysago = getTwodaysage();

    pvStatistic(hiveContext,twodaysage);
  }

  private static void pvStatistic(HiveContext hiveContext, String twodaysago){
    hiveContext.sql("use hive");
    String sqlText = "SELECT data, pageID, pv" 
      +  "FROM ( SELECT data, pageID, count(*) pv FROM userlogs"
      +  "WHERE action = 'View' AND date = 'twodaysago' GROUP BY date, pageID ) subquery" 
      +  "group by date, pageID order by pv desc limit 10"

    hiveContext.sql(sqlText);
    //把执行结果放在数据库或者Hive
  }

  private static String getTwodaysage(){
    SimpleDateFormat date = new SimpleDateFormat("yyy-MM-dd");
    Calendar cal = Calendar.getInstance();
    cal.setTime(new Date());
  }
}
/**
  * ./hive --service metadata
  * ./spark-sql --master spark://hadoop-namenode01:7077,hadoop-namenode02:7077
  *
  * drop table userlogs;
  * create table userLogs(data String, timestamp bigint, userID bigint, pageID bigint, channel String, action String) 
                          row format delimited fields terminated by '\t' lines terminated by '\n';
  * load data local inpath '/root/data/userLog.log' into table userlogs;
  */

