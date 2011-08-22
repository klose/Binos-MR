package cn.ict.cacuts.mapreduce;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import cn.ict.binos.transmit.BinosDataClient;
import cn.ict.binos.transmit.BinosURL;
import cn.ict.cacuts.mapreduce.mapcontext.DealMapOutUtil;
import cn.ict.cacuts.mapreduce.mapcontext.HashPartitioner;

public class MapContext<KEY, VALUE> {

	private final static Log LOG = LogFactory.getLog(MapContext.class);
	private static Configuration conf = new Configuration();	
	private static FileSystem fs;
	private DealMapOutUtil outPut;
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
	public MapContext(String inputPath, String[] outputPath) throws Exception {
		//this.spiltIndexPath = inputPath;
		this.outputPath = outputPath;
		this.outPut = new DealMapOutUtil(this.outputPath);
		InputStream ins = BinosDataClient.getInputStream(new BinosURL(new Text(inputPath)));
		//FSDataInputStream in = fs.open(new Path(inputPath));
		splitIndex.readFields(ins);
		lineReader.initialize(splitIndex);
		
	}
	public boolean hasNextLine() throws IOException {
		return lineReader.nextKeyValue();
	}
	public MapContext() {

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
	public void setOutputPath(String[] outputPath) {
		this.outputPath = outputPath;
		this.outPut.setOutputPath(outputPath);
	}
	public void flush() {
		// TODO Auto-generated method stub
		outPut.FinishedReceive();
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String[] keys = { "key1", "key2", "key3", "key4", "key5", "key6" , "key7"};
		int[] values = { 1, 2, 3, 4, 5, 6 ,7};
		MapContext tt = new MapContext();
		for (int i = 0; i < keys.length; i++) {
			tt.output(keys[i], values[i]);
		}
	//	tt.Finished();
		
	}
	

}
