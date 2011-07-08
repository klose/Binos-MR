package cn.ict.cacuts.mapreduce.mapcontext;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import cn.ict.cacuts.mapreduce.MRConfig;
import cn.ict.cacuts.mapreduce.MapContext;

public class DealMapOutUtil<KEY, VALUE> {

	private final static Log LOG = LogFactory.getLog(DealMapOutUtil.class);
	////int numberOfReduce = MRConfig.getReduceTaskNum();
	int numberOfReduce = 3;
	////public int size = 1024 * 1024;
	public int size = 100;
	ArrayList inputPairs = new ArrayList();
	ArrayList backupInputPairs = new ArrayList();
	ArrayList[] lists = new ArrayList[numberOfReduce];
	String[] fileName;
	public String[] mapOutFileIndex;//suppose there are no more than 100 interfile
	int[] innerFilePartionIndex =  new int[numberOfReduce];
	KVPair element;

	boolean inputFull = false;
	boolean writeInputPairs = true;
	boolean finishedReceive = false;
	boolean finishedWriteInputPairs = false;
	boolean finishedWriteBackupInputPairs = false;
	int partionedNum;
	int tmpFileNum = 0;

	String indexString = "";
	
	HashPartitioner hashPartitioner = new HashPartitioner();
////	String tempMapOutFilesPathPrefix = MRConfig.getTempMapOutFilesPathPrefix()
//			+ "tmpMapOut_";
	String tempMapOutFilesPathPrefix = System.getProperty("user.home")+ "/CactusTest/"
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
				partionedNum = hashPartitioner
						.getPartition(key, numberOfReduce);
				innerFilePartionIndex[partionedNum]++;
				element = new KVPair(key, value, partionedNum);
				inputPairs.add(element);
				if (inputPairs.size() == size) {
					System.out.println("inputPairs.size() == size "
							+ inputPairs.size());
					writeInputPairs = false;
					finishedWriteInputPairs = false;
					dealReceivedUtil(inputPairs, innerFilePartionIndex);
					innerFilePartionIndex =  new int[numberOfReduce];
					inputPairs.clear();
					finishedWriteInputPairs = true;
				}
			} else {
				partionedNum = hashPartitioner
						.getPartition(key, numberOfReduce);
				innerFilePartionIndex[partionedNum]++;
				element = new KVPair(key, value, partionedNum);
				backupInputPairs.add(element);
				if (backupInputPairs.size() == size) {
					System.out.println("backupInputPairs.size() == size "
							+ backupInputPairs.size());
					finishedWriteBackupInputPairs = false;
					if (finishedWriteInputPairs) {
						writeInputPairs = true;
					}
					dealReceivedUtil(backupInputPairs, innerFilePartionIndex);
					innerFilePartionIndex =  new int[numberOfReduce];
					backupInputPairs.clear();
					finishedWriteBackupInputPairs = true;
				}

			}
		}
	}



	public void dealReceivedUtil(ArrayList inputPairs,int[] innerFilePartionIndex) {
		tmpFileNum++;
		dealFileIndexContext(innerFilePartionIndex);
		sortAndSaveDatas(inputPairs);
	}

	public void dealFileIndexContext(int[] innerFilePartionIndex) {
		for (int i = 0; i < innerFilePartionIndex.length; i++) {
			indexString += innerFilePartionIndex[i] + ",";			
		}
		indexString +=";";
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
		if (!inputPairs.isEmpty()) {
			dealReceivedUtil(inputPairs, innerFilePartionIndex);
			inputPairs.clear();
		}
		if (!backupInputPairs.isEmpty()) {
			dealReceivedUtil(backupInputPairs, innerFilePartionIndex);
			backupInputPairs.clear();
		}
		dealFileIndex();
		System.out.println("***********************over********************");
	}
	
	public void dealFileIndex(){
		indexString = indexString.substring(0, indexString.length() - 1);
		mapOutFileIndex = indexString.split(";");
		for(int i = 0 ; i < mapOutFileIndex.length ; i ++){
			mapOutFileIndex[i] = mapOutFileIndex[i].substring(0, mapOutFileIndex[i].length() - 1);
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
		String[] keys = { "pear", "banana", "orange", "cat", "apple", "moon","egg" };
		int[] values = { 1, 7, 5, 10, 2, 4, 11 };
		int[] partitions = { 3,2,1,3,2,1 ,2};

		
		DealMapOutUtil tt = new DealMapOutUtil();
//		for (int i = 0; i < keys.length; i++) {
//			tt.receive(keys[i], values[i]);
//		}
		
		for (int i = 0; i < 500; i++) {
			tt.receive(keys[i%6], i);
		}
		

		tt.FinishedReceive();
		for(int i = 0 ; i < tt.mapOutFileIndex.length ; i ++ ){
			System.out.println( tt.mapOutFileIndex[i]);
		}
	}
}
