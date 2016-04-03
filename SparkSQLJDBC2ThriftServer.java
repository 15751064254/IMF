package com.dt.spark.SparkApps.sql;

import java.util.ArrayList;
import java.sql.Connection;
import java.sql.DriverManager;
/**
  * 实战演示Java通过JDBC访问Thrift Server，进而访问Spark SQL，进而访问Hive，这是企业级开发中最为常见的方式；
  */


public class SparkSQLJDBC2ThriftServer {
  public static void main(String[] args) {
    String sql = "select name from people where age = ?";
    Connection conn = null;
    ResultSet resultSet = null;
    try{

      Class.forName("org.apache.hive.jdbc.HiveDriver");
      conn = DriverManager.getConnection("jdbc:hive2://<host>:10001/<database>?hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice", "root", "");
      PreparedStatement preparedStatement = conn.prepareStatement(sql);
      preparedStatement.setInt(1, 30);
      resultSet = preparedStatement.executeQuery();

      while(resultSet.next()){
        System.out.println(resultSet.getString(1)); //此处的数据应该保存在Parquet中

      }

    }catch(Exception e){
      e.printStackTrace();
    }finally{

      conn.close();
    }

  }
}
