package cn.ict.cacuts.mapreduce.mapcontext;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.cacuts.mapreduce.MRConfig;
import cn.ict.cacuts.mapreduce.MapContext;

public class DealMapOutUtil<KEY, VALUE> {

//	public String filePrefix = System.getProperty("user.home") + "/CactusTest/";
//	public String taskId = "map_1";
	private final static Log LOG = LogFactory.getLog(DealMapOutUtil.class);
	int numberOfReduce = MRConfig.getReduceTaskNum();
	public int size = 1024 * 1024;
	ArrayList inputPairs = new ArrayList();
	ArrayList backupInputPairs = new ArrayList();
	ArrayList[] lists = new ArrayList[numberOfReduce];
	String[] fileName;

	boolean inputFull = false;
	boolean writeInputPairs = true;
	boolean finishedReceive = false;
	boolean finishedWriteInputPairs = false;
	boolean finishedWriteBackupInputPairs = false;

	
	public  DealMapOutUtil() {}
	public DealMapOutUtil(String[] outputPath) {
		setOutputPath(outputPath);
		this.numberOfReduce = outputPath.length;
	}
	public void receive(KEY key, VALUE value)  {
		if (!finishedReceive) {
			if (writeInputPairs) {
				inputPairs.add(key + " , " + value);
				if (inputPairs.size() == size) {
					System.out.println("inputPairs.size() == size "  + inputPairs.size());
					writeInputPairs = false;
					finishedWriteInputPairs = false;
					hashInputPairs(inputPairs);
					inputPairs.clear();
					finishedWriteInputPairs = true;
					dealHashed();
				}
			} else {
				backupInputPairs.add(key + " , " + value);
				if (backupInputPairs.size() == size) {
					System.out.println("backupInputPairs.size() == size "  + backupInputPairs.size());
					finishedWriteBackupInputPairs = false;
					if (finishedWriteInputPairs) {
						writeInputPairs = true;
					}
					hashInputPairs(backupInputPairs);
					backupInputPairs.clear();
					finishedWriteBackupInputPairs = true;

					dealHashed();
				}

			}
		}
		// else {
		// if (!inputPairs.isEmpty()) {
		// hashInputPairs(inputPairs);
		// dealHashed();
		// }
		// if (!backupInputPairs.isEmpty()) {
		// hashInputPairs(backupInputPairs);
		// dealHashed();
		// }
		// }
	}

	public void FinishedReceive()  {
		this.finishedReceive = true;
		System.out.println("***********************over********************");
		if (!inputPairs.isEmpty()) {
			hashInputPairs(inputPairs);
			dealHashed();
			inputPairs.clear();
		}
		if (!backupInputPairs.isEmpty()) {
			hashInputPairs(backupInputPairs);
			dealHashed();
			backupInputPairs.clear();
		}
	}

	@SuppressWarnings("unchecked")
	public void hashInputPairs(ArrayList inputpairs) {
		KEY key;
		for (int i = 0; i < numberOfReduce; i++) {
			lists[i] = new ArrayList();
		}

		HashPartitioner partioner = new HashPartitioner();
		// System.out.println(" inputpairs.size();" + inputpairs.size());
		for (int i = 0; i < inputpairs.size(); i++) {
			key = (KEY) inputpairs.get(i).toString().split(" , ")[0];
			lists[partioner.getPartition(key, numberOfReduce)].add(inputpairs
					.get(i));
		}
		
	}

	/**
	 * suppose that the hashed result is even ----> while(!lists[0].isEmpty())
	 * @throws InterruptedException 
	 * */
	public void dealHashed()  {
		System.out.println("begin to deal hash   " + !lists[0].isEmpty());
		if (!lists[0].isEmpty()) {
			DealSingleMapOut1[] dealThreadi = new DealSingleMapOut1[lists.length];
			// System.out.println(" lists.length "+ lists.length);
			for (int i = 0; i < lists.length; i++) {
				// System.out.println("fileName[i]  " + fileName[i]);
				// System.out.println("lists[i]  " + lists[i]);
				dealThreadi[i] = new DealSingleMapOut1(fileName[i], lists[i]);
				dealThreadi[i].start();
				try {
					dealThreadi[i].join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		for (int i = 0; i < lists.length; i++) {
			lists[i].clear();
		}
	}
	public void setOutputPath(String[] outputPath) {
		this.fileName = outputPath;
		if (outputPath.length <= 0) {
			LOG.error("You should set map output path.");
		}
	}
	public int getNumberReduce() {
		return numberOfReduce;
	}

	
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) {
		String[] keys = { "pear", "banana", "orange", "cat", "apple", "moon",
				"egg" };
		int[] values = { 1, 7, 5, 10, 2, 4, 11 };

		DealMapOutUtil tt = new DealMapOutUtil();
		for (int i = 0; i < keys.length; i++) {
			tt.receive(keys[i], values[i]);
		}
		tt.FinishedReceive();
	}
}
