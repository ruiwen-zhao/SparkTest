package main;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Serializable;
import scala.Tuple2;

public class Test implements Serializable {

  private final static Integer INTERVAL_THRESHOLD = 5;
	
  private static Integer byteArrToInteger(byte[] bytes) {
	  
	  ByteBuffer id = ByteBuffer.allocate(4);
      id.putInt((Integer) Bytes.toInt(bytes));
      return Integer.valueOf(Bytes.toString(id.array()));
  }
	
  public void start() throws IOException {
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);

    
    Configuration hbaseConf = HBaseConfiguration.create();
    
    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes("0001"));
    scan.setStopRow(Bytes.toBytes("0004"));
    scan.addFamily(Bytes.toBytes("info"));
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("carID"));
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"));
    ClientProtos.Scan proto = ProtobufUtil.toScan(scan);    
    String scanStr = Base64.encodeBytes(proto.toByteArray()); 
    
    String tableName = "records";
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName);
    hbaseConf.set(TableInputFormat.SCAN, scanStr); 
    
    JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(hbaseConf,
            TableInputFormat.class, ImmutableBytesWritable.class,
            Result.class);
       
    
    System.out.println("here: " + hBaseRDD.count());
        
    
    PairFunction<Tuple2<ImmutableBytesWritable, Result>, Integer, Integer> pairFunc = 
    		new PairFunction<Tuple2<ImmutableBytesWritable, Result>, Integer, Integer>() {
        @Override
        public Tuple2<Integer, Integer> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
        	
        	byte[] time = immutableBytesWritableResultTuple2._2().getValue(Bytes.toBytes("info"), Bytes.toBytes("time"));
        	byte[] id = immutableBytesWritableResultTuple2._2().getValue(Bytes.toBytes("info"), Bytes.toBytes("carID"));
            if (time != null && id != null) {
                return new Tuple2<Integer, Integer>(byteArrToInteger(id), byteArrToInteger(time));
            }
            else {
            	return null;
            }
        }
    };
    
    JavaPairRDD<Integer, Integer> pairRdd = hBaseRDD.mapToPair(pairFunc);
    
    
    Function<Integer, List<Integer>> combiner = new Function<Integer, List<Integer>>() {
    	public List<Integer> call(Integer i) {
    		List<Integer> intList = new ArrayList<Integer>();
    		intList.add(i);
    		return intList;
    	}
    };
    
    
    Function2<List<Integer>, Integer, List<Integer>> merger = new Function2<List<Integer>, Integer, List<Integer>>() {
    	public List<Integer> call(List<Integer> intList, Integer i) {
    		boolean added = false;
    		for(int k = 0 ; k < intList.size(); k++ ) {
    			if(i < intList.get(k)) {
    				intList.add(k, i);
    				added = true;
    				break;
    			}
    		}
    		if(!added){
    			intList.add(i);
    		}
    		return intList;
    	}
    };
    
    Function2<List<Integer>, List<Integer>, List<Integer>> mergeCombiner = new Function2<List<Integer>, List<Integer>, List<Integer>>() {
    	public List<Integer> call(List<Integer> list1, List<Integer> list2) {
    		int i=0, j=0, k=0;
    		List<Integer> list3 = new ArrayList<Integer>();
    		while(i < list1.size() && j < list2.size()) {
	    		if(list1.get(i) < list2.get(j)) {
	    			list3.add(list1.get(i++));
	    		}
	    		else {
	    			list3.add(list2.get(j++));
	    		}
    		}
    		while(i < list1.size()) {
    			list3.add(list1.get(i++));
    		}
    		while(j < list2.size()) {
    			list3.add(list2.get(j++));
    		}
    		return list3;
    	}
    };
    

    
    JavaPairRDD<Integer, List<Integer>> idTimePairs = pairRdd.combineByKey(combiner, merger, mergeCombiner);
    
    List<Tuple2<Integer, List<Integer>>> idTimeTuple = idTimePairs.collect();
    for (Tuple2 tuple : idTimeTuple) {
    	System.out.println(tuple._1 + ": " + tuple._2);
    }

    Function<Tuple2<Integer, List<Integer>>, Boolean> getFake = new Function<Tuple2<Integer, List<Integer>>, Boolean>() {
    	public Boolean call(Tuple2<Integer, List<Integer>> pairs) {
    		for(int i = 0; i < pairs._2.size()-1; i++) {
    			if(pairs._2.get(i+1) - pairs._2.get(i) <= INTERVAL_THRESHOLD) {
    				return true;
    			}
    		}
    		return false;
    	}
    };
    
    JavaPairRDD<Integer,List<Integer>> fakeNumbers = idTimePairs.filter(getFake);
   
    List<Tuple2<Integer, List<Integer>>> result = fakeNumbers.collect();
        
       
    for (Tuple2 tuple : result) {
    	System.out.println(tuple._1 + ": " + tuple._2);
    }
    
  }
  
  public static void main(String[] args) throws IOException {
	  new Test().start();
	  System.exit(0);
  }
}