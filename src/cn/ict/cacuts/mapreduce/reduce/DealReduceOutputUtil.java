package cn.ict.cacuts.mapreduce.reduce;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.cacuts.mapreduce.MRConfig;
import cn.ict.cacuts.mapreduce.MapContext;
import cn.ict.cacuts.mapreduce.mapcontext.KVList;
import cn.ict.cacuts.mapreduce.mapcontext.WriteIntoFile;

public class DealReduceOutputUtil<KEY, VALUE> {

	private final static Log LOG = LogFactory
			.getLog(DealReduceOutputUtil.class);
	// //public int size = 1024 * 1024;
	public int size = 100;
	ArrayList inputPairs = new ArrayList();
	ArrayList backupInputPairs = new ArrayList();
	String fileName;
	FinalKVPair element;

	boolean inputFull = false;
	boolean writeInputPairs = true;
	boolean finishedReceive = false;
	boolean finishedWriteInputPairs = false;
	boolean finishedWriteBackupInputPairs = false;

	public DealReduceOutputUtil() {
	}

	public DealReduceOutputUtil(String[] outputPath) {
		setOutputPath(outputPath);
	}

	public void receive(KEY key, VALUE value) {

		if (!finishedReceive) {
			if (writeInputPairs) {
				element = new FinalKVPair(key, value);
				inputPairs.add(element);
				if (inputPairs.size() == size) {
					System.out.println("inputPairs.size() == size "
							+ inputPairs.size());
					writeInputPairs = false;
					finishedWriteInputPairs = false;
					SaveDatas(inputPairs);
					inputPairs.clear();
					finishedWriteInputPairs = true;
				}
			} else {
				element = new FinalKVPair(key, value);
				backupInputPairs.add(element);
				if (backupInputPairs.size() == size) {
					System.out.println("backupInputPairs.size() == size "
							+ backupInputPairs.size());
					finishedWriteBackupInputPairs = false;
					if (finishedWriteInputPairs) {
						writeInputPairs = true;
					}
					SaveDatas(backupInputPairs);
					backupInputPairs.clear();
					finishedWriteBackupInputPairs = true;
				}

			}
		}
	}

	public void SaveDatas(ArrayList pairs) {
		WriteIntoFile tt = new WriteIntoFile();
		tt.setFileName(fileName);
		tt.writeIntoFile(pairs);
	}

	public void FinishedReceive() {
		this.finishedReceive = true;
		if (!inputPairs.isEmpty()) {
			SaveDatas(inputPairs);
			// inputPairs.clear();
		}
		if (!backupInputPairs.isEmpty()) {
			SaveDatas(backupInputPairs);
			// backupInputPairs.clear();
		}
		System.out.println("***********************over********************");
	}

	public void setOutputPath(String[] outputPath) {
		this.fileName = outputPath[0];
		if (outputPath == null) {
			LOG.error("You should set map output path.");
		}
	}

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) {
		String[] keys = { "pear", "banana", "orange", "cat", "apple", "moon",
				"egg" };
		int[] values = { 1, 7, 5, 10, 2, 4, 11 };
		int[] partitions = { 3, 2, 1, 3, 2, 1, 2 };
		String[] fileName = {System.getProperty("user.home")+ "/CactusTest/" + "tmpReduceOut"};
		DealReduceOutputUtil tt = new DealReduceOutputUtil();
		tt.setOutputPath(fileName);
		 for (int i = 0; i < keys.length; i++) {
		 tt.receive(keys[i], values[i]);
		 }
//		for (int i = 0; i < 500; i++) {
//			tt.receive(keys[i % 6], i);
//		}
		tt.FinishedReceive();
	}
}
