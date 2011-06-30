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
	private DealReduceInputUtil receive = new DealReduceInputUtil();
	private FileSplitIndex splitIndex = new FileSplitIndex();
	private HdfsFileLineReader lineReader = new HdfsFileLineReader();
	private String[] outputPath;
	static {
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error("Cannot open HDFS.");
		}
	}
	public ReduceContext(Path inputPath) throws IOException {
		//this.spiltIndexPath = inputPath;
		FSDataInputStream in = fs.open(inputPath);
		splitIndex.readFields(in);
		lineReader.initialize(splitIndex);
	}
	public boolean hasNextLine() throws IOException {
		return lineReader.nextKeyValue();
	}
	public ReduceContext() {

	}

	public String getNextLine() {
		return lineReader.getCurrentValue().toString();
	}

	public void output(KEY key, VALUE value) {
		//System.out.println("key : " + key);
		//System.out.println("value : " + value);
		receive.receive(key, value);
	}
	
	public String[] getOutputPath() {
		return outputPath;
	}
	public void setInputPath(String[] inPutPath) {
		this.outputPath = outputPath;
		this.receive.setInputFilePath(inPutPath);
	}
	public void flush() {
		// TODO Auto-generated method stub
		receive.FinishedReceive();
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
