package cn.ict.cacuts.mapreduce.mapcontext;

import java.io.Serializable;

public class DataStruct<KEY,VALUE> implements Serializable{


	public KEY key;
	public VALUE value;
	public int partionNum;
	
	DataStruct(KEY key,VALUE value,int partionNum){
		this.key = key;
		this.value = value;
		this.partionNum = partionNum;
	}

	public KEY getKey() {
		return key;
	}
	public void setKey(KEY key) {
		this.key = key;
	}
	public VALUE getValue() {
		return value;
	}
	public void setValue(VALUE value) {
		this.value = value;
	}
	public int getPartionNum() {
		return partionNum;
	}
	public void setPartionNum(int partionNum) {
		this.partionNum = partionNum;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String key = "hello";
		int value = 2;
		int partionNum = 1;
		DataStruct tt = new DataStruct(key,value,partionNum);
		System.out.println(tt.getKey());
	}
}
