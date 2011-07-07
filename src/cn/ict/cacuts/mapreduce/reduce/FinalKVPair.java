package cn.ict.cacuts.mapreduce.reduce;
import java.io.Serializable;

public class FinalKVPair<KEY,VALUE> implements Serializable{


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private KEY key;
	private VALUE value;
	
	public FinalKVPair(){}
	
	public FinalKVPair(KEY key,VALUE value){
		this.key = key;
		this.value = value;
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
	public String toString() {
		return key + ":" + value ;
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String key = "hello";
		int value = 2;
		FinalKVPair tt = new FinalKVPair(key,value);
		System.out.println(tt.getKey());
	}
}
