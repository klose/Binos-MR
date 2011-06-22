package cn.ict.cacuts.mapreduce.mapcontext;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class DealSingleMapOut1<KEY, VALUE> {

	public int size = 1024 * 1024;
	String interResultPathPrefix;

	boolean useList = true;
//	boolean finishedWritePairs = false;
//	boolean finishedWriteBackupPairs = false;
//	boolean finishedWrite = false;

	ArrayList list = new ArrayList();
	ArrayList backupList = new ArrayList();
	boolean finishedReceive = false;
	boolean finishedRecive = false;
	boolean finishedDealList = false;
	boolean finishedDealBackupList = false;

	public void deal(ArrayList dealList) {
		Sort tt = new Sort(dealList);
	//	combine();
	//	writeFile();
		System.out.println("prepared to deal");
		System.out.println(useList);
		System.out.println(list.size());
	}



	private void receive(ArrayList receiveList) {
		if (!finishedRecive) {
			if (useList) {
				list.addAll(receiveList);

				if (list.size() == size) {
					finishedDealList = false;
					useList = false;
					deal(list);
					list.clear();
					finishedDealList = true;
				}
			} else {
				backupList.addAll(receiveList);
				if (backupList.size() == size) {
					if (finishedDealList) {
						useList = true;
					}
					deal(backupList);
					backupList.clear();
					finishedDealBackupList = true;
				}
			}

		} else {
			if (!list.isEmpty()) {
				deal(list);
			}
			if (!backupList.isEmpty()) {
				deal(backupList);
			}
		}

	}

	public void writeFile(ArrayList pairs) {
		WriteIntoFile writeSingleFile = new WriteIntoFile("xxxxxxxx");
		writeSingleFile.writeIntoFile(pairs);
	}

	public void FinishedReceive() {
		this.finishedReceive = true;
		if (!list.isEmpty()) {
			deal(list);
			list.clear();
		}

		if (!backupList.isEmpty()) {
			deal(backupList);
			backupList.clear();
		}
	}

	public void setSize(int size) {
		this.size = size;
	}

	public int getSize() {
		return size;
	}

	public void setInterResultPathPrefix(String interResultPathPrefix) {
		this.interResultPathPrefix = interResultPathPrefix;
	};

	public String getInterResultPathPrefix() {
		return interResultPathPrefix;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		String[] keys = { "key1", "key2", "key3", "key4", "key5", "key6",
//				"key7" };
//		int[] values = { 1, 2, 3, 4, 5, 6, 7 };
		DealSingleMapOut1 tt = new DealSingleMapOut1();

//		for (int i = 0; i < keys.length; i++) {
//			tt.output(keys[i], values[i]);
//		}
		
		
		ArrayList receiveList= new ArrayList();
		receiveList.add("key1 , 1");
		receiveList.add("key2 , 2");
		receiveList.add("key3 , 3");
		receiveList.add("key4 , 4");
		receiveList.add("key5 , 5");
		receiveList.add("key6 , 6");
		receiveList.add("key7 , 7");
		receiveList.add("key8 , 8");
		tt.receive(receiveList);
		tt.FinishedReceive();
	}
}
