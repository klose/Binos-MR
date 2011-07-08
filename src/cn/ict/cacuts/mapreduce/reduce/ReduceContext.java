package cn.ict.cacuts.mapreduce.reduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.ict.cacuts.mapreduce.FileSplitIndex;
import cn.ict.cacuts.mapreduce.HdfsFileLineReader;
import cn.ict.cacuts.mapreduce.MapContext;
import cn.ict.cacuts.mapreduce.mapcontext.DealMapOutUtil;

public class ReduceContext <KEY, VALUE>{


	private final static Log LOG = LogFactory.getLog(MapContext.class);
	private static Configuration conf = new Configuration();	
	private static FileSystem fs;
	private DealReduceInputUtil receive ;
	private DealReduceOutputUtil outPut = new DealReduceOutputUtil();

	//private FileSplitIndex splitIndex = new FileSplitIndex();
	private HdfsFileLineReader lineReader = new HdfsFileLineReader();   /////line reader should not be hdfs reader
	
	String[] reduceRemoteReadFiles;
	String[] tmpLocalFilePath;
	private String[] outputPath;
	
	static {
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error("Cannot open HDFS.");
		}
	}
	
	public ReduceContext() {

	}
	public ReduceContext(String[] reduceRemoteReadFiles,String[] tmpLocalFilePath,String[] outputPath) {
		this.reduceRemoteReadFiles = reduceRemoteReadFiles;
		this.tmpLocalFilePath = tmpLocalFilePath;
		this.outputPath = outputPath;
	}
	/**
	 * read remote file and save them
	 * */
	public void init(){
		receive = new DealReduceInputUtil(reduceRemoteReadFiles,tmpLocalFilePath[0]);
		receive.prepared();
		
	}
	/**
	 * need to modify*************
	 * */
	public ReduceContext(Path inputPath) throws IOException {
		//this.spiltIndexPath = inputPath;
		FSDataInputStream in = fs.open(inputPath);
		//splitIndex.readFields(in);//////////////////////
		//lineReader.initialize(splitIndex);
	}
	public boolean hasNextLine() throws IOException {
		//TODO need initialize lineReader///////////////////////////////////////////////
		return lineReader.nextKeyValue();
	}


	public String getNextLine() {
		return lineReader.getCurrentValue().toString();
	}

	public void output(KEY key, VALUE value) {
		//System.out.println("key : " + key);
		//System.out.println("value : " + value);
		outPut.receive(key, value);
	}
	

	
	public String[] getOutputPath() {
		return outputPath;
	}
	
	public void setOutputPath(String[] outPutPath) {
		this.outputPath = outPutPath;
		this.outPut.setOutputPath(outPutPath);
	}
	
	public String[] getReduceRemoteReadFiles() {
		return reduceRemoteReadFiles;
	}
	public void setReduceRemoteReadFiles(String[] reduceRemoteReadFiles) {
		this.reduceRemoteReadFiles = reduceRemoteReadFiles;
	}
	public String[] getTmpLocalFilePath() {
		return tmpLocalFilePath;
	}
	public void setTmpLocalFilePath(String[] tmpLocalFilePath) {
		this.tmpLocalFilePath = tmpLocalFilePath;
	}
	public void flush() {
		outPut.FinishedReceive();
	}
	
	public void controlReadWhichFile(){
		
	}
	
	public void readSpecificFile(){
		
	}
	
	
	public void writeTempleFile(){
		
	}
	
	public void dealTempleFile(){
		
	}

}
