package cn.ict.cacuts.mapreduce.reduce;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.transformer.compiler.DataState;

import cn.ict.cacuts.mapreduce.WriteIntoDataBus;
public class DealReduceOutputUtil<KEY, VALUE> {

	private final static Log LOG = LogFactory
			.getLog(DealReduceOutputUtil.class);
	// //public int size = 1024 * 1024;
	private DataState state;
	public int size =  1024 * 1024;
	CopyOnWriteArrayList inputPairs = new CopyOnWriteArrayList();
	ArrayList backupInputPairs = new ArrayList();
	String fileName;
	FinalKVPair element;
	WriteIntoDataBus writeTool;
	private  AtomicBoolean writeFinished = new AtomicBoolean(false);
	private  AtomicBoolean writeInputPairs = new AtomicBoolean(false);
	//private volatile boolean allWaiting = false;
	private volatile boolean isAllHandle = false;
	private Object writeAction = new Object();
	//private Object waitingAction = new Object();

	public DealReduceOutputUtil() {
	}


	public DealReduceOutputUtil(String[] outputPath, DataState state) {
		setOutputPath(outputPath);
		this.state = state;
		this.writeTool = new WriteIntoDataBus(fileName);
		new writePairsThread().start();
	}
	public void receive(KEY key, VALUE value)  {	
		LOG.info("receive key=" + key + " value=" + value);
		element = new FinalKVPair(key, value);
		if (!writeInputPairs.get()) {
			inputPairs.add(element);
			if (inputPairs.size() >= size) {
				//notify the thread to write the inputPairs to File	
				synchronized(writeAction) {
					writeInputPairs.set(true);
					writeAction.notify();
				}
			}	
		}
		else {
			backupInputPairs.add(element);
//			System.out.println("***************backupInputPairs.add(element);");
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
			if (writeFinished.get()) {
//				System.out.println("***************writeFinished");	
				inputPairs.addAll(backupInputPairs);
				backupInputPairs.clear();				
				writeFinished.set(false);
			}
		}
	}
	class writePairsThread extends Thread {
		public void run() {
			while (!isAllHandle) {
				synchronized (writeAction) {
					while (!writeInputPairs.get()) {
						try {
//							System.out.println("$$$$$$$$$$$$$$ writeAction.wait()");
							writeAction.wait();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
//					System.out.println("***************writeInputPairs =true");
					if (inputPairs.size() > 0) {
						SaveDatas(inputPairs);
						inputPairs.clear();
						System.out.println("kkkkkkkk");
					} else if (backupInputPairs.size() > 0) {
						SaveDatas(backupInputPairs);
						System.out.println("vvvvvvvvv");
					}
					writeInputPairs.set(false);
					writeFinished.set(true);
					
				}
			}
			writeTool.executeClose();
		}
	}

	
	public void SaveDatas(List pairs) {
//		System.out.println("Binos-test: save reduce output length:" + pairs.size());		
		writeTool.executeWrite(pairs);
	}

	public void FinishedReceive() {
		while (writeInputPairs.get()) {
			// wait for last write to over.
		}
		synchronized (writeAction) {
			// System.out.println("$$$$$$$$$$$$$$ enter into FinishedReceive");
			if (!writeInputPairs.get()) {
				writeInputPairs.set(true);
				isAllHandle = true;
				writeAction.notify();
			}
		}
		inputPairs.clear();
		backupInputPairs.clear();

	}


	public void setOutputPath(String[] outputPath) {	
		this.fileName = outputPath[0];
		LOG.info("store the output path at:" + this.fileName);
		if (outputPath == null) {
			LOG.error("You should set reduce output path.");
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
