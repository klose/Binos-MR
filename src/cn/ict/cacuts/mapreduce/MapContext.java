package cn.ict.cacuts.mapreduce;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import cn.ict.cacuts.mapreduce.mapcontext.HashPartitioner;

public class MapContext<KEY, VALUE> {

//	ByteBuffer bb = ByteBuffer.allocate(1024 * 1024);
	int numberOfReduce = 2;
	public int size = 1024 * 1024;
	ArrayList inputPairs = new ArrayList();
	ArrayList backupInputPairs = new ArrayList();
	ArrayList[] lists= new ArrayList[numberOfReduce];
	
	
	boolean inputFull = false;
	boolean writeInputPairs = true;
	boolean finishedWrite = false;
	boolean finishedWriteInputPairs = false;
	boolean finishedWriteBackupInputPairs = false;
	
	public MapContext() {

	}

	public boolean hasNextLine() {
		return true;
	}

	public String getNextLine() {
		return "";
	}

	/**
	 * here suppose that two ArrayList both with the size of 1024*1024 is enough
	 * */
	public void output(KEY key, VALUE value) {

		if (!finishedWrite) {
			if (writeInputPairs) {
				inputPairs.add(key + "," + value);

				if (inputPairs.size() == size) {
					writeInputPairs = false;
					finishedWriteInputPairs = false;
					hashInputPairs(inputPairs);					
					inputPairs.clear();
					finishedWriteInputPairs = true;
				}
			} else {
				backupInputPairs.add(key + "," + value);
				if (backupInputPairs.size() == size) {
					if (finishedWriteInputPairs) {
						writeInputPairs = true;
					}
					hashInputPairs(backupInputPairs);
					backupInputPairs.clear();
					finishedWriteBackupInputPairs = true;
				}

			}
		} else {
			if (!inputPairs.isEmpty()) {
				hashInputPairs(inputPairs);
			}
			if (!backupInputPairs.isEmpty()) {
				hashInputPairs(backupInputPairs);
			}
		}
	}

	public void hashInputPairs(ArrayList inputpairs){
		KEY key ;
		VALUE value;
		for(int i = 0 ; i < numberOfReduce ; i ++){
			lists[i] = new ArrayList();
		}
		
		HashPartitioner partioner = new HashPartitioner();
		for(int i = 0 ; i < inputpairs.size() ; i ++ ){
			key = (KEY) inputpairs.get(i).toString().split(",")[0];
			lists[partioner.getPartition(key, numberOfReduce)].add(inputpairs.get(i));
		}
		
		
		for(int i = 0 ; i < numberOfReduce ; i ++){
			System.out.println(lists[i]);
		}
	}
	

	public void Finished() {
		this.finishedWrite = true;
		if (!inputPairs.isEmpty()) {
			hashInputPairs(inputPairs);
			inputPairs.clear();
		}

		if (!backupInputPairs.isEmpty()) {
			hashInputPairs(backupInputPairs);
			backupInputPairs.clear();
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String[] keys = { "key1", "key2", "key3", "key4", "key5", "key6" , "key7"};
		int[] values = { 1, 2, 3, 4, 5, 6 ,7};
		MapContext tt = new MapContext();
		for (int i = 0; i < keys.length; i++) {
			tt.output(keys[i], values[i]);
		}
		tt.Finished();
		
	}

}
