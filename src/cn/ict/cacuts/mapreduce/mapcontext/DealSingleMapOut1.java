package cn.ict.cacuts.mapreduce.mapcontext;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class DealSingleMapOut1 extends Thread {

	ArrayList list = new ArrayList();
	boolean finishedDeal = false;
	String fileName;

	DealSingleMapOut1(String fileName, ArrayList receiveList) {
		this.fileName = fileName;
		this.list = receiveList;
	}

	public void run() {
		finishedDeal = false;
		Sort testSort = new Sort(list);
		Object[] sortedResult = testSort.beginSort();
		// combine();
		writeFile(fileName, sortedResult);
		finishedDeal = true;

	}

	public void writeFile(String fileName, ArrayList pairs) {
		WriteIntoFile writeSingleFile = new WriteIntoFile(fileName);
		writeSingleFile.writeIntoFile(pairs);
	}

	public void writeFile(String fileName, Object[] pairs) {
		WriteIntoFile writeSingleFile = new WriteIntoFile(fileName);
		writeSingleFile.writeIntoFile(pairs);
	}

	public void setfileName(String fileName) {
		this.fileName = fileName;
	};

	public String getfileName() {
		return fileName;
	}
}
