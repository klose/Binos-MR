package cn.ict.cacuts.mapreduce.mapcontext;

public class HashPartitioner<K, V> {
	public int getPartition(K key, V value, int numReduceTasks) {
		return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}

	public int getPartition(K key, int numReduceTasks) {
		return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
	
	
	public static void main(String[] args) {
		String[] keys = { "key1", "key2", "key3", "key4", "key5", "key6" , "key7"};
		int[] values = { 1, 2, 3, 4, 5, 6 ,7};
		
		HashPartitioner tt = new HashPartitioner();
//		for(int i = 0 ; i < keys.length ; i ++){
//			System.out.println(tt.getPartition(keys[i], values[i], 3));
//		}		
		
		for(int i = 0 ; i < keys.length ; i ++){
			System.out.println(tt.getPartition(keys[i],  3));
		}
	}
}
