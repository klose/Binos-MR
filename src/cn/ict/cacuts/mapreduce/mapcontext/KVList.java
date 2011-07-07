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

	public KVList(K key, V value) {
		this.key = key;
		this.value = new Vector<V>();
		this.value.add(value);
	}
	public KVList(KVPair<K,V> pair) {
		this.key = pair.getKey();
		this.value = new Vector<V>();
		this.value.add(pair.getValue());
	}
	public void addVal(V value) {
		this.value.add(value);
	}
	/*add all the elements in the vec to the value*/
	public void appendVec(Vector<V> vec) {
		this.value.addAll(vec);
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
		return res;

	}
	@Override
	public Iterator<V> iterator() {
		// TODO Auto-generated method stub
		return value.iterator();
	}
}
