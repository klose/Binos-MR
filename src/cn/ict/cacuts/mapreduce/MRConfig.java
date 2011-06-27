package cn.ict.cacuts.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;


/**
 * configure the arguments of map reduce.
 * @author jiangbing
 *
 */
public class MRConfig {
	/*there are default values*/
	public static int mapTaskMem = 100*1024*1024;
	public static int mapTaskNum = 1;
	public static int reduceTaskMem = 100*1024*1024;
	public static int reduceTaskNum = 1;
	public static long splitFileSize = 64*1024*1024; //use a hdfs block size as default value.
	public static Class mapContextKeyClass = Integer.class;
	public static Class mapContextValueClass = String.class;
	private  static Class<? extends Mapper> mapClass;
	
	static {
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			splitFileSize = fs.getDefaultBlockSize();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	public static int getMapTaskMem() {
		return mapTaskMem;
	}
	public static void setMapTaskMem(int mapTaskMem) {
		MRConfig.mapTaskMem = mapTaskMem;
	}
	public static int getMapTaskNum() {
		return mapTaskNum;
	}
	public static void setMapTaskNum(int mapTaskNum) {
		MRConfig.mapTaskNum = mapTaskNum;
	}
	public static int getReduceTaskMem() {
		return reduceTaskMem;
	}
	public static void setReduceTaskMem(int reduceTaskMem) {
		MRConfig.reduceTaskMem = reduceTaskMem;
	}
	public static int getReduceTaskNum() {
		return reduceTaskNum;
	}
	public static void setReduceTaskNum(int reduceTaskNum) {
		MRConfig.reduceTaskNum = reduceTaskNum;
	}
	public static long getSplitFileSize() {
		return splitFileSize;
	}
	public static void setSplitFileSize(long splitFileSize) {
		MRConfig.splitFileSize = splitFileSize;
	}
	public  static Class getMapContextKeyClass() {
		return mapContextKeyClass;
	}
	public static void setMapContextKeyClass(Class mapContextKeyClass) {
		MRConfig.mapContextKeyClass = mapContextKeyClass;
	}
	public static Class getMapContextValueClass() {
		return mapContextValueClass;
	}
	public static void setMapContextValueClass(Class mapContextValueClass) {
		MRConfig.mapContextValueClass = mapContextValueClass;
		
	}
	public static Class<? extends Mapper> getMapClass() {
		return mapClass;
	}
	public static void setMapClass(Class<? extends Mapper> mapClass) {
		MRConfig.mapClass =  mapClass;
	}
}
