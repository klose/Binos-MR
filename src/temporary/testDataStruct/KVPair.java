package temporary.testDataStruct;

import java.io.Serializable;

public class KVPair<K,V> implements Serializable{

	/**
	 * 
	 */
	
	
	private K Key;
	private V value;
	private int partition;
	public KVPair() {
		
	}
	public KVPair(K key, V value, int partition) {
		this.Key = key;
		this.value = value;
		this.partition = partition;
	}
	public K getKey() {
		return Key;
	}
	public String toString() {
		return Key + ":" + value + ":" +partition;
		
	}
}
