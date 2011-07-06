package cn.ict.cacuts.mapreduce.mapcontext;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Vector;

/*the format of map-out file and reduce-in file*/
 public class KVList<K,V> implements Iterable<V> ,Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private K key;
	private final Vector<V> value;
	private int partition;

	public KVList(K key, V value, int partition) {
		this.key = key;
		this.value = new Vector<V>();
		this.value.add(value);
		this.partition = partition;
	}
	public KVList(KVPair<K,V> pair) {
		this.key = pair.getKey();
		this.value = new Vector<V>();
		this.value.add(pair.getValue());
		this.partition = pair.getpartitionNum();
	}
	public void addVal(V value) {
		this.value.add(value);
	}
	public K getKey() {
		return key;
	}
	public Vector<V> getValue() {
		return value;
	}
	public String toString() {
		String res = key + ":";
		for (V v: this.value) {
			res += v + ":";
		}
		return res + partition;
	}
	@Override
	public Iterator<V> iterator() {
		// TODO Auto-generated method stub
		return value.iterator();
	}
}
