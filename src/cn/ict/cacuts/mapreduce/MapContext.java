package cn.ict.cacuts.mapreduce;

import org.apache.hadoop.conf.Configuration;

public class MapContext<KEY, VALUE> {
	Configuration conf = new Configuration ();	
	public MapContext() {
		
	}
	public boolean hasNextLine() {
		return true;
	}
	public String getNextLine() {
		return "";
	}
	public void output(KEY key, VALUE value) {
		
	}
}
