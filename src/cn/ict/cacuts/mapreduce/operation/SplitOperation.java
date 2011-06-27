package cn.ict.cacuts.mapreduce.operation;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import cn.ict.cacuts.mapreduce.DataSplit;
import cn.ict.cacuts.mapreduce.FileSplitIndex;
import cn.ict.cacuts.mapreduce.MRConfig;

import com.transformer.compiler.Operation;

public class SplitOperation implements Operation{
	private static final Log LOG = LogFactory.getLog(SplitOperation.class);
	private static Configuration hdfsConf = new Configuration();
	private static FileSystem fs;
	static {
		try {
			fs = FileSystem.get(hdfsConf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error("Cannot open HDFS");
		}
	}
	@Override
	public void operate(String[] inputPath, String[] outputPath) {
		// TODO Auto-generated method stub
		if (inputPath.length != 1) {
			LOG.error("the number of input path : " + inputPath.length);
		}
		try {
			MRConfig conf = new MRConfig();
			final DataSplit split = new DataSplit(new Path(inputPath[0]));
			List<FileSplitIndex> list = split.getSplits(conf);
			if (list.size() != outputPath.length) {
				LOG.error("The number of map task conflicts with the number of output path.");
			}
			for(int i = 0; i < outputPath.length; i++) {
				FSDataOutputStream out = fs.create(new Path(outputPath[i]));
				list.get(i).write(out);
				out.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}