package cn.ict.cacuts.mapreduce.reduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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
	public int size =  1024;
	CopyOnWriteArrayList inputPairs = new CopyOnWriteArrayList();
	ArrayList backupInputPairs = new ArrayList();
	String fileName;
	FinalKVPair element;
	
	/*boolean inputFull = false;
	boolean writeInputPairs = true;
	boolean finishedReceive = false;
	boolean finishedWriteInputPairs = false;
	boean finishedWriteBackupInputPairs = false;*/
	private volatile boolean writeFinished = false;
	private volatile boolean writeInputPairs = false;
	//private volatile boolean allWaiting = false;
	private volatile boolean isAllHandle = false;
	private Object writeAction = new Object();
	//private Object waitingAction = new Object();

	public DealReduceOutputUtil() {
	}


	public DealReduceOutputUtil(String[] outputPath) {
		setOutputPath(outputPath);
		new writePairsThread().start();
	}
	public void receive(KEY key, VALUE value)  {	
		LOG.info("receive key=" + key + " value=" + value);
		element = new FinalKVPair(key, value);
		if (!writeInputPairs) {
			inputPairs.add(element);
		}
		else {
			backupInputPairs.add(element);
//			if (backupInputPairs.size() == size) {
//				allWaiting = true;
//				synchronized(waitingAction) {
//					while (allWaiting) {
//						try {
//							waitingAction.wait();
//						} catch (InterruptedException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//					}
//				}
//			}
			if (writeFinished) {
				inputPairs.clear();
				inputPairs.addAll(backupInputPairs);
				backupInputPairs.clear();				
				writeFinished = false;
			}
		}
		if (inputPairs.size() >= size) {
			//notify the thread to write the inputPairs to File	
			synchronized(writeAction) {
				writeInputPairs = true;
				writeAction.notify();
			}
		}	
	}
	class writePairsThread extends Thread {
		public void run() {
			while (!isAllHandle) {
				synchronized (writeAction) {
					while (!writeInputPairs) {
						try {
							writeAction.wait();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				SaveDatas(inputPairs);
				writeInputPairs = false;
				writeFinished = true;
//				if (allWaiting) {
//					synchronized (waitingAction) {
//						allWaiting = false;
//						waitingAction.notify();
//					}
//				}
			}
		}
	}
	/*
	public void receive(KEY key, VALUE value) {
		LOG.info("receive key=" + key + " value=" + value);
		if (!finishedReceive) {
			LOG.info("test1");
			if (writeInputPairs) {
				LOG.info("test2");
				element = new FinalKVPair(key, value);
				inputPairs.add(element);
				if (inputPairs.size() == size) {
					LOG.info("test3");
					System.out.println("inputPairs.size() == size "
							+ inputPairs.size());
					writeInputPairs = false;
					finishedWriteInputPairs = false;
					SaveDatas(inputPairs);
					inputPairs.clear();
					finishedWriteInputPairs = true;
				}
			} else {
				LOG.info("test4");
				element = new FinalKVPair(key, value);
				backupInputPairs.add(element);
				if (backupInputPairs.size() == size) {
					LOG.info("test5");
					System.out.println("backupInputPairs.size() == size "
							+ backupInputPairs.size());
					finishedWriteBackupInputPairs = false;
					if (finishedWriteInputPairs) {
						LOG.info("test6");
						writeInputPairs = true;
					}
					LOG.info("test7");
					SaveDatas(backupInputPairs);
					backupInputPairs.clear();
					finishedWriteBackupInputPairs = true;
				}
			}
		}
	}*/
	

	public void SaveDatas(List pairs) {
		System.out.println("Binos-test: save reduce output length:" + pairs.size());
		WriteIntoFile tt = new WriteIntoFile();
		tt.setFileName(fileName);
		tt.writeIntoFile(pairs);
	}

	public void FinishedReceive() {
		isAllHandle = true;
		synchronized(writeAction) {
			if(!writeInputPairs)
				writeAction.notify();
		}
//		if (inputPairs.size() != 0) {
//			SaveDatas(inputPairs);
//		}
//		if (backupInputPairs.size() != 0) {
//			SaveDatas(backupInputPairs);
//		}
		System.out.println("***********************over********************");
	}


	public void setOutputPath(String[] outputPath) {	
		this.fileName = outputPath[0];
		LOG.info("store the output path at:" + this.fileName);
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
