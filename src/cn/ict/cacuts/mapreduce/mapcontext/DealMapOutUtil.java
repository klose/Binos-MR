package cn.ict.cacuts.mapreduce.mapcontext;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.cacuts.mapreduce.MRConfig;
import cn.ict.cacuts.mapreduce.MapContext;

public class DealMapOutUtil<KEY, VALUE> {

	// public String filePrefix = System.getProperty("user.home") +
	// "/CactusTest/";
	// public String taskId = "map_1";
	private final static Log LOG = LogFactory.getLog(DealMapOutUtil.class);
	int numberOfReduce = MRConfig.getReduceTaskNum();
	public int size = 1024 * 1024;
	ArrayList inputPairs = new ArrayList();
	ArrayList backupInputPairs = new ArrayList();
	ArrayList[] lists = new ArrayList[numberOfReduce];
	String[] fileName;
	String[] mapOutFileIndex;
	int[] innerFilePartionIndex;
	DataStruct element;

	boolean inputFull = false;
	boolean writeInputPairs = true;
	boolean finishedReceive = false;
	boolean finishedWriteInputPairs = false;
	boolean finishedWriteBackupInputPairs = false;
	int partionedNum;
	int tmpFileNum = 0;

	HashPartitioner hashPartitioner = new HashPartitioner();
	String tempMapOutFilesPathPrefix = MRConfig.getTempMapOutFilesPathPrefix()
			+ "tmpMapOut_";

	public DealMapOutUtil() {
	}

	public DealMapOutUtil(String[] outputPath) {
		setOutputPath(outputPath);
		this.numberOfReduce = outputPath.length;
	}

	public void receive(KEY key, VALUE value) {

		if (!finishedReceive) {
			if (writeInputPairs) {
				innerFilePartionIndex = new int[numberOfReduce];
				partionedNum = hashPartitioner
						.getPartition(key, numberOfReduce);
				innerFilePartionIndex[partionedNum]++;
				element = new DataStruct(key, value, partionedNum);
				inputPairs.add(element);
				if (inputPairs.size() == size) {
					System.out.println("inputPairs.size() == size "
							+ inputPairs.size());
					writeInputPairs = false;
					finishedWriteInputPairs = false;
					dealReceivedUtil(inputPairs, innerFilePartionIndex);
					inputPairs.clear();
					finishedWriteInputPairs = true;
				}
			} else {
				innerFilePartionIndex = new int[numberOfReduce];
				partionedNum = hashPartitioner
						.getPartition(key, numberOfReduce);
				innerFilePartionIndex[partionedNum]++;
				element = new DataStruct(key, value, partionedNum);
				backupInputPairs.add(element);
				if (backupInputPairs.size() == size) {
					System.out.println("backupInputPairs.size() == size "
							+ backupInputPairs.size());
					finishedWriteBackupInputPairs = false;
					if (finishedWriteInputPairs) {
						writeInputPairs = true;
					}
					dealReceivedUtil(backupInputPairs, innerFilePartionIndex);
					backupInputPairs.clear();
					finishedWriteBackupInputPairs = true;
				}

			}
		}
	}

	public void dealReceivedUtil(ArrayList inputPairs,int[] innerFilePartionIndex) {
		tmpFileNum++;
		dealFileIndex(innerFilePartionIndex);
		sortAndSaveDatas(inputPairs);
	}

	public void dealFileIndex(int[] innerFilePartionIndex) {
		String indexString = null;
		for (int i = 0; i < innerFilePartionIndex.length; i++) {
			indexString += innerFilePartionIndex[i] + " , ";
		}
		mapOutFileIndex[tmpFileNum] = indexString.substring(0, indexString.length() - 3);
	}



	public void sortAndSaveDatas(ArrayList inputPairs) {
		String fileName;

		fileName = tempMapOutFilesPathPrefix + tmpFileNum;
		SaveDatas(sortDatas(inputPairs), fileName);
	}

	public Object[] sortDatas(ArrayList inputPairs) {
		Object[] ss = inputPairs.toArray();
		Arrays.sort(ss, SortStructedData.getComparator());
		return ss;
	}

	public void SaveDatas(Object[] sorted, String fileName) {
		WriteIntoFile tt = new WriteIntoFile();
		tt.setFileName(fileName);
		tt.writeIntoFile(sorted);
	}

	public void FinishedReceive() {
		this.finishedReceive = true;
		System.out.println("***********************over********************");
		if (!inputPairs.isEmpty()) {
			// hashInputPairs(inputPairs);
			dealReceivedUtil(inputPairs, innerFilePartionIndex);
			inputPairs.clear();
		}
		if (!backupInputPairs.isEmpty()) {
			// hashInputPairs(backupInputPairs);
			dealReceivedUtil(backupInputPairs, innerFilePartionIndex);
			backupInputPairs.clear();
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
