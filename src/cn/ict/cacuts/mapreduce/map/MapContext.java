package cn.ict.cacuts.mapreduce.map;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.transformer.compiler.DataState;
import com.transformer.compiler.JobConfiguration;

import cn.ict.binos.transmit.BinosDataClient;
import cn.ict.binos.transmit.BinosURL;
import cn.ict.cacuts.mapreduce.FileSplitIndex;
import cn.ict.cacuts.mapreduce.HdfsFileLineReader;

public class MapContext<KEY, VALUE> {

	private final static Log LOG = LogFactory.getLog(MapContext.class);
	private static Configuration conf = new Configuration();	
	private static FileSystem fs;
	private DealMapOutUtil outPut;
	private DataState dataState;//record the type of handling data
	private FileSplitIndex splitIndex = new FileSplitIndex();
	private HdfsFileLineReader lineReader = new HdfsFileLineReader();
	private String tempMapOutPathPrefix; // =  "/opt/jiangbing/CactusTest/"
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
	public MapContext(String inputPath, String[] outputPath, String workingDir, String taskId) throws Exception {
		//this.spiltIndexPath = inputPath;
		
		this.outputPath  = outputPath;
		//setOutputPath(outputPath);
		this.setTempMapOutFilesPathPrefix(workingDir, taskId);
		this.outPut = new DealMapOutUtil(this.outputPath, this.tempMapOutPathPrefix, this.dataState);
		InputStream ins = BinosDataClient.getInputStream(new BinosURL(new Text(inputPath)));
		//FSDataInputStream in = fs.open(new Path(inputPath));
		splitIndex.readFields(ins);
		lineReader.initialize(splitIndex);
		
	}
	
	//set the path as to  different transmit type.
	public void setTempMapOutFilesPathPrefix(String workDir, String taskid) {
		
		if (this.outputPath[0].startsWith("MESSAGE")) { 
			this.tempMapOutPathPrefix = JobConfiguration.getMsgHeader() + taskid+ "tmpMapOut";
			this.dataState = DataState.MESSAGE_POOL;
		}
		else {
			//support local file path
			this.tempMapOutPathPrefix = workDir + "/tmpMapOut";
			this.dataState = DataState.LOCAL_FILE;
		}
	}

	public boolean hasNextLine() throws IOException {
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
//	public void setOutputPath(String[] outputPath) {
//		this.outputPath = outputPath;
//		this.outPut.setOutputPath(outputPath);
//	}
	public void flush() {
		// TODO Auto-generated method stub
		outPut.FinishedReceive();
	}
	public String getTempMapOutFilesPathPrefix() {
		return tempMapOutPathPrefix;
	}
	
	

}
