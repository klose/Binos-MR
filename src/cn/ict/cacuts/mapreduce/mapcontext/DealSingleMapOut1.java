package cn.ict.cacuts.mapreduce.mapcontext;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class DealSingleMapOut1 extends Thread{

	public int size = 1024 * 1024;
	String filePrefix;
	public int numberReduce = 2;
	public String taskId = "map_1";

	boolean useList = true;
	// boolean finishedWritePairs = false;
	// boolean finishedWriteBackupPairs = false;
	// boolean finishedWrite = false;

	ArrayList list = new ArrayList();
	ArrayList backupList = new ArrayList();
	boolean finishedReceive = false;
	boolean finishedRecive = false;
	boolean finishedDealList = false;
	boolean finishedDealBackupList = false;

	boolean finishedDeal = false;
	
	String fileName;
//	public void deal(ArrayList dealList) {
//		Sort testSort = new Sort(dealList);
//		Object[] sortedResult = testSort.getSortedResult();
//		String fileName = null;
//		// combine();
//		// writeFile();
//		writeFile(fileName, sortedResult);
//		System.out.println(useList);
//		System.out.println(list.size());
//	}

	DealSingleMapOut1(String fileName , ArrayList receiveList){
		this.fileName = fileName;
		this.list = receiveList;
	}
	
	public void run(){	
		finishedDeal = false;
		 Sort testSort = new Sort(list);
		 Object[] sortedResult = testSort.beginSort();
		// combine();
		 writeFile(fileName, sortedResult);
		 finishedDeal = true;
		 
	}
//	private void receive(ArrayList receiveList) {
//		if (!finishedRecive) {
//			if (useList) {
//				list.addAll(receiveList);
//
//				if (list.size() == size) {
//					finishedDealList = false;
//					useList = false;
//					deal(list);
//					list.clear();
//					finishedDealList = true;
//				}
//			} else {
//				backupList.addAll(receiveList);
//				if (backupList.size() == size) {
//					if (finishedDealList) {
//						useList = true;
//					}
//					deal(backupList);
//					backupList.clear();
//					finishedDealBackupList = true;
//				}
//			}
//
//		} else {
//			if (!list.isEmpty()) {
//				deal(list);
//			}
//			if (!backupList.isEmpty()) {
//				deal(backupList);
//			}
//		}
//
//	}

	public void writeFile(String fileName, ArrayList pairs) {
		WriteIntoFile writeSingleFile = new WriteIntoFile(fileName);
		writeSingleFile.writeIntoFile(pairs);
	}

	public void writeFile(String fileName, Object[] pairs) {
		WriteIntoFile writeSingleFile = new WriteIntoFile(fileName);
		writeSingleFile.writeIntoFile(pairs);
	}

//	public void FinishedReceive() {
//		this.finishedReceive = true;
//		if (!list.isEmpty()) {
//			deal(list);
//			list.clear();
//		}
//
//		if (!backupList.isEmpty()) {
//			deal(backupList);
//			backupList.clear();
//		}
//	}

//	public void genericFileName() {
//		String[] fileName = new String[numberReduce];
//		for (int i = 0; i < numberReduce; i++) {
//			fileName[i] = filePrefix + taskId + "_out_" + i;
//			System.out.println(fileName[i]);
//		}
//	}

	public void setSize(int size) {
		this.size = size;
	}

	public int getSize() {
		return size;
	}

	public void setFilePrefix(String filePrefix) {
		this.filePrefix = filePrefix;
	};

	public String getFilePrefix() {
		return filePrefix;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String fileName =  System.getProperty("user.home") + "/CactusTest/"  + "aaaa";
		ArrayList receiveList = new ArrayList();
		receiveList.add("key5 , 5");
		receiveList.add("key1 , 1");
		receiveList.add("cae , 7");
		receiveList.add("key6 , 6");
		receiveList.add("key2 , 2");
		receiveList.add("key8 , 8");
		receiveList.add("good , 4");
		receiveList.add("key3 , 3");
		receiveList.add("key4 , 6");
		receiveList.add("key4 , 4");
		receiveList.add("bda , 5");
		receiveList.add("hello, 5");
		
		DealSingleMapOut1 tt = new DealSingleMapOut1(fileName ,receiveList);


	//	tt.receive(receiveList);
	//	tt.FinishedReceive();
	}
}
