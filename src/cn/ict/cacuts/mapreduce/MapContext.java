package cn.ict.cacuts.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MapContext<KEY, VALUE> {
	
	private final static Log LOG = LogFactory.getLog(MapContext.class);
	private static Configuration conf = new Configuration();	
	private static FileSystem fs;
	//private Path spiltIndexPath;
	private FileSplitIndex splitIndex = new FileSplitIndex();
	private HdfsFileLineReader lineReader = new HdfsFileLineReader();
	static {
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error("Cannot open HDFS.");
		}
	}
	public MapContext(Path inputPath) throws IOException {
		//this.spiltIndexPath = inputPath;
		FSDataInputStream in = fs.open(inputPath);
		splitIndex.readFields(in);
		lineReader.initialize(splitIndex);
	}
	public boolean hasNextLine() throws IOException {
		return lineReader.nextKeyValue();
	}
	public String getNextLine() {
		return lineReader.getCurrentValue().toString();
	}
	public void output(KEY key, VALUE value) {
		
	}
}
