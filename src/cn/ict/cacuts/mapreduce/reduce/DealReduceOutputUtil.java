package cn.ict.cacuts.mapreduce.reduce;
import java.util.ArrayList;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.transformer.compiler.DataState;

import cn.ict.cacuts.mapreduce.KeyValue.KVPairInt;
import cn.ict.cacuts.mapreduce.WriteIntoDataBus;
public class DealReduceOutputUtil<KEY, VALUE> {

	private final static Log LOG = LogFactory
			.getLog(DealReduceOutputUtil.class);
	public final static int size = 1000 * 100;
	private int capacity = 0;
	private CopyOnWriteArrayList<KVPairInt> inputPairs = new CopyOnWriteArrayList<KVPairInt>();
	private final DataState state;
	String fileName;
	KVPairInt element;
	WriteIntoDataBus writeTool;
	
	private AtomicBoolean writeInputPairs = new AtomicBoolean(false);
	private KVPairInt[] objects = null;
	private volatile boolean isAllHandle = false;
	private Object writeAction = new Object();
	private Thread writeThread;

	public DealReduceOutputUtil(String[] outputPath, DataState state) {
		setOutputPath(outputPath);
		this.state = state;
		this.writeTool = new WriteIntoDataBus(fileName);
		this.writeThread = new writePairsThread();
		this.writeThread.start();
	}
	public void receive(KEY key, VALUE value) {
		element = KVPairInt.newBuilder().setKey(key.toString()).setValue(Integer.parseInt(value.toString())).build();
		capacity += element.getSerializedSize();
		if (!writeInputPairs.get() && capacity >= size) {
			synchronized(writeAction) {
				System.out.println("notify");
				writeInputPairs.set(true);
				capacity = 0;
				objects = inputPairs.toArray(new KVPairInt[0]);
				inputPairs.clear();
				writeAction.notify();
			}
		}
		inputPairs.add(element);
	}
	/*public void receive(KEY key, VALUE value)  {	
		//LOG.info("receive key=" + key + " value=" + value);
		element = KVPairInt.newBuilder().setKey(key.toString()).setValue(Integer.parseInt(value.toString())).build();
		capacity += element.getSerializedSize();
		if (!writeInputPairs.get()) {
			inputPairs.add(element);
			if (capacity >= size) {
				//notify the thread to write the inputPairs to File	
				synchronized(writeAction) {
					writeInputPairs.set(true);
					capacity = 0;
				
					writeAction.notify();
				}
			}	
		}
		else {
			backupInputPairs.add(element);
			if (writeFinished.get()) {
//				System.out.println("***************writeFinished");
				inputPairs.clear();
				inputPairs.addAll(backupInputPairs);
				backupInputPairs.clear();
				//writeInputPairs.set(false);
				writeFinished.set(false);
			}
		}
	}*/
	class writePairsThread extends Thread {
		public void run() {
			while (!isAllHandle) {
				synchronized (writeAction) {
					while (!writeInputPairs.get()) {
						try {
							writeAction.wait();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					if (objects != null) 
						SaveDatas(objects);
					writeInputPairs.set(false);	
				}
				
			}
			writeTool.close();
		}
	}

	
	public void SaveDatas(KVPairInt[] objects) {
//		System.out.println("Binos-test: save reduce output length:" + pairs.size());		
		writeTool.writeKVPairIntArray(objects);
	}

	public void FinishedReceive() {
		while(writeInputPairs.get()) {
			//wait for last write to over.
		}
		synchronized(writeAction) {
//			System.out.println("$$$$$$$$$$$$$$ enter into FinishedReceive");
			if(!writeInputPairs.get()) {
				if (inputPairs.size() > 0) {
					objects = inputPairs.toArray(new KVPairInt[0]);
					inputPairs.clear();
				}
				else {
					objects = null;
				}
				writeInputPairs.set(true);
				isAllHandle = true;
				writeAction.notify();
			}
		}
//		inputPairs.clear();
//		backupInputPairs.clear();
	
//		System.out.println("***********************over********************");
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

		String[] fileName = {"/tmp/binos-tmp/final-test"};

		DealReduceOutputUtil tt = new DealReduceOutputUtil(fileName, DataState.LOCAL_FILE);
		tt.setOutputPath(fileName);
		for (int i = 0; i < 1000; i++) 
		for (int j = 0; j < keys.length; j++) {
			 tt.receive(keys[j], values[j]);
		 }
//		for (int i = 0; i < 500; i++) {
//			tt.receive(keys[i % 6], i);
//		}
		tt.FinishedReceive();
	}
}
