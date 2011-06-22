package cn.ict.cacuts.mapreduce.mapcontext;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class DealSingleMapOut<KEY, VALUE> {

	public int size = 1024 * 1024;
	String interResultPathPrefix;

	boolean writePairs = true;
	boolean finishedWritePairs = false;
	boolean finishedWriteBackupPairs = false;
	boolean finishedWrite = false;

	ArrayList pairs = new ArrayList();
	ArrayList backupPairs = new ArrayList();

	public void output(KEY key, VALUE value) {

		if (!finishedWrite) {
			if (writePairs) {
				pairs.add(key + "," + value);

				if (pairs.size() == size) {
					finishedWritePairs = false;
					writePairs = false;
					writeFile(pairs);					
					pairs.clear();
					finishedWritePairs = true;
				}
			} else {
				backupPairs.add(key + "," + value);

				if (backupPairs.size() == size) {
					if (finishedWritePairs) {
						writePairs = true;
					}
					writeFile(backupPairs);
					backupPairs.clear();
					finishedWriteBackupPairs = true;
				}
			}

			// if (pairsWriteable) {
			// pairs.add(key + "," + value);
			//
			// if (pairs.size() == size) {
			// pairsWriteable = false;
			// writeIntoFile(pairs);
			// pairs.clear();
			// pairsWriteable = true;
			// }
			// } else {
			// backupPpairsWriteable = true;
			// if (backupPpairsWriteable) {
			// backupPairs.add(key + "," + value);
			//
			// if (backupPairs.size() == size) {
			// backupPpairsWriteable = false;
			// writeIntoFile(backupPairs);
			// backupPairs.clear();
			// }
			// }
			//
			// }
		} else {
			if (!pairs.isEmpty()) {
				writeFile(pairs);
			}
			if (!backupPairs.isEmpty()) {
				writeFile(backupPairs);
			}
		}
	}

	public void writeFile(ArrayList pairs) {
		WriteIntoFile writeSingleFile = new WriteIntoFile("xxxxxxxx");
		writeSingleFile.writeIntoFile(pairs);
	}

	public void FinishedWrite() {
		this.finishedWrite = true;
		if (!pairs.isEmpty()) {
			writeFile(pairs);
			pairs.clear();
		}

		if (!backupPairs.isEmpty()) {
			writeFile(backupPairs);
			backupPairs.clear();
		}
	}

	public void setSize(int size){
		this.size = size;
	}
	public int getSize(){
		return size;
	}
	public void setInterResultPathPrefix(String interResultPathPrefix){
		this.interResultPathPrefix = interResultPathPrefix;
	};
	public String getInterResultPathPrefix(){
		return interResultPathPrefix;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String[] keys = { "key1", "key2", "key3", "key4", "key5", "key6","key7" };
		int[] values = { 1, 2, 3, 4, 5, 6, 7 };
		DealSingleMapOut tt = new DealSingleMapOut();

		for (int i = 0; i < keys.length; i++) {
			tt.output(keys[i], values[i]);
		}
		tt.FinishedWrite();
	}
}
