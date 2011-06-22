package cn.ict.cacuts.mapreduce;

public class MapContext<KEY, VALUE> {
	
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
