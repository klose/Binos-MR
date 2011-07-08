package cn.ict.cacuts.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.ict.cacuts.mapreduce.mapcontext.KVList;
import cn.ict.cacuts.mapreduce.reduce.FinalKVPair;


/**
 * configure the arguments of map reduce.
 * @author jiangbing
 *
 */
public class MRConfig {
	/*there are default values*/
	public static String jobName = "testJob";
	public static int mapTaskMem = 100*1024*1024;
	public static int mapTaskNum = 1;
	public static int reduceTaskMem = 100*1024*1024;
	public static int reduceTaskNum = 1;
	public static long splitFileSize = 64*1024*1024; //use a hdfs block size as default value.
	public static Class mapContextKeyClass = Integer.class;
	public static Class mapContextValueClass = String.class;
	private  static Class<? extends DataSplit> splitClass; //////////////////////////need to check
	private  static Class<? extends Mapper> mapClass;
	private  static Class<? extends Reducer> reduceClass;
	private  static Class<? > finalOutputKeyValueTypeClass = FinalKVPair.class;
	private  static Class<?> mapOutputKeyValueTypeClass = KVList.class;
	private  static Path workingDirectory;

	static String tempMapOutFilesPathPrefix;
	
	private String[] inputFileName;
	private String[] outputFileName;

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
	
	public static String getJobName() {
		return jobName;
	}
	public static void setJobName(String jobName) {
		MRConfig.jobName = jobName;
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
	public static String getTempMapOutFilesPathPrefix() {
		return tempMapOutFilesPathPrefix;
	}
	public static void setTempMapOutFilesPathPrefix(String tempMapOutFilesPathPrefix) {
		tempMapOutFilesPathPrefix = tempMapOutFilesPathPrefix;
	}
	public static Class<? extends Reducer> getReduceClass() {
		return reduceClass;
	}
	public static void setReduceClass(Class<? extends Reducer> reduceClass) {
		MRConfig.reduceClass = reduceClass;
	}
	public static Path getWorkingDirectory() {
		return workingDirectory;
	}
	public static void setWorkingDirectory(Path workingDirectory) {
		MRConfig.workingDirectory = workingDirectory;
	}
	public static Class<?> getFinalOutputKeyValueTypeClass() {
		return finalOutputKeyValueTypeClass;
	}
	public static void setFinalOutputKeyValueTypeClass(
			Class<?> finalOutputKeyValueTypeClass) {
		MRConfig.finalOutputKeyValueTypeClass = finalOutputKeyValueTypeClass;
	}
	public static Class<?> getMapOutputKeyValueTypeClass() {
		return mapOutputKeyValueTypeClass;
	}
	public static void setMapOutputKeyValueTypeClass(
			Class<?> mapOutputKeyValueTypeClass) {
		MRConfig.mapOutputKeyValueTypeClass = mapOutputKeyValueTypeClass;
	}
	public String[] getInputFileName() {
		return inputFileName;
	}
	public void setInputFileName(String[] inputFileName) {
		this.inputFileName = inputFileName;
	}
	public String[] getOutputFileName() {
		return outputFileName;
	}
	public void setOutputFileName(String[] outputFileName) {
		this.outputFileName = outputFileName;
	}

	
}
