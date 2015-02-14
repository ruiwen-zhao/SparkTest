package main;

import java.io.IOException;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.util.Bytes;

public class Test {
  public static void main(String[] args) throws IOException {
//    String logFile = "/usr/lib/spark/README.md"; // Should be some file on your system
//    SparkConf conf = new SparkConf().setAppName("Simple Application");
//    JavaSparkContext sc = new JavaSparkContext(conf);
//    JavaRDD<String> logData = sc.textFile(logFile).cache();
//
//    long numAs = logData.filter(new Function<String, Boolean>() {
//      public Boolean call(String s) { return s.contains("a"); }
//    }).count();
//
//    long numBs = logData.filter(new Function<String, Boolean>() {
//      public Boolean call(String s) { return s.contains("b"); }
//    }).count();
//
//    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
//    
//    
    Configuration hbaseConf = HBaseConfiguration.create();
    HTable table = new HTable(hbaseConf, "car_table");  
    Get get = new Get(Bytes.toBytes("0001"));  
    Result result = table.get(get);   
    
    // 输出结果  
    for (KeyValue rowKV : result.raw()) {  
        System.out.print("Row Name: " + new String(rowKV.getRow()) + " ");  
        System.out.print("Timestamp: " + rowKV.getTimestamp() + " ");  
        System.out.print("column Family: " + new String(rowKV.getFamily()) + " ");  
        System.out.print("Row Name:  " + new String(rowKV.getQualifier()) + " ");  
        System.out.println("Value: " + new String(rowKV.getValue()) + " ");  
    }  

    
  }
}