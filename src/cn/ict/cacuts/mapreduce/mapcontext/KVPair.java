
package cn.ict.cacuts.mapreduce.mapcontext;

import java.io.Serializable;

public class KVPair<KEY,VALUE> implements Serializable{


	public KEY key;
	public VALUE value;
	public int partitionNum;
	
	public KVPair(){}
	public KVPair(KEY key,VALUE value,int partitionNum){
		this.key = key;
		this.value = value;
		this.partitionNum = partitionNum;
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
	public int getpartitionNum() {
		return partitionNum;
	}
	public void setpartitionNum(int partitionNum) {
		this.partitionNum = partitionNum;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String key = "hello";
		int value = 2;
		int partitionNum = 1;
		KVPair tt = new KVPair(key,value,partitionNum);
		System.out.println(tt.getKey());
	}
}
