package cn.ict.cacuts.mapreduce.reduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import cn.ict.cacuts.mapreduce.mapcontext.Sort;
import cn.ict.cacuts.mapreduce.mapcontext.WriteIntoFile;

public class DealReduceInputUtil<KEY, VALUE> {

	public int reduceNum = 2;
	public String[] reduceInputFilePath;
	ArrayList dealed = new ArrayList();
	public String reduceOutPutFileName;
	public Map<KEY, ArrayList<VALUE>> keyValues;
	private boolean finishedReceive = false;

	public void beginToReduce() {
		prepared();
		reduceClass(dealed);
		writeIntoFile();
	}

	private void writeIntoFile() {
		WriteIntoFile write = new WriteIntoFile();
		write.writeIntoFile(dealed, reduceOutPutFileName);

	}

	public void prepared() {
		// ArrayList dealed = new ArrayList();
		dealed = readFiles(reduceInputFilePath[0]);
		for (int i = 1; i < reduceInputFilePath.length; i++) {
			dealed = readMergeSort(dealed, reduceInputFilePath[i]);
		}
		makeKeyValues(dealed);
	}

	public ArrayList readMergeSortReduce(ArrayList dealed, String fileName) {
		// ArrayList dealed = new ArrayList();
		ArrayList justReaded = new ArrayList();
		justReaded = readFiles(fileName);
		dealed.addAll(justReaded);
		dealed = sort(dealed);
		dealed = reduceClass(dealed);
		return dealed;
	}

	public ArrayList readMergeSort(ArrayList dealed, String fileName) {
		// ArrayList dealed = new ArrayList();
		ArrayList justReaded = new ArrayList();
		justReaded = readFiles(fileName);
		dealed.addAll(justReaded);
		dealed = sort(dealed);
		return dealed;
	}

	@SuppressWarnings("unchecked")
	public void makeKeyValues(ArrayList sorted) {
		keyValues = new HashMap();
		ArrayList<VALUE> values;
		ArrayList<VALUE> backupValues = new ArrayList<VALUE>();
		KEY key;
		for (int i = 0; i < sorted.size(); i++) {
			key = (KEY) sorted.get(i).toString().split(" , ")[0].trim();
			if (keyValues.containsKey(key)) {
				backupValues = keyValues.get(key);
				backupValues.add((VALUE) sorted.get(i).toString().split(" , ")[1].trim());
				keyValues.remove(key);
				keyValues.put(key, backupValues);
			} else {
				values = new ArrayList<VALUE>();
				values.add((VALUE) sorted.get(i).toString().split(" , ")[1].trim());
				keyValues.put(key, values);
			}
		}

	}

	public ArrayList readFiles(String fileName) {
		ReadFile readFile = new ReadFile(fileName);
		ArrayList readed = readFile.readMethod2();
		return readed;
	}

	public ArrayList reduceClass(ArrayList sorted) {
		ArrayList combined = new ArrayList();
		// TODO combine
		return combined;
	}

	public ArrayList sort(ArrayList dealed) {
		ArrayList sorted = new ArrayList();
		Sort sort = new Sort(dealed);
		sorted = sort.beginSort1();
		return sorted;
	}

	public void setReduceNum(int reduceNum) {
		this.reduceNum = reduceNum;
	}

	public int getReduceNum() {
		return this.reduceNum;
	}

	public void setInputFilePath(String[] reduceInputPath) {
		this.reduceInputFilePath = reduceInputPath;
	}

	public String[] getInputFilePath() {
		return this.reduceInputFilePath;
	}

	public void setReduceOutFileName(String reduceOutPutFileName) {
		this.reduceOutPutFileName = reduceOutPutFileName;
	}

	public String getReduceOutFileName() {
		return this.reduceOutPutFileName;
	}


	public void FinishedReceive() {
		this.finishedReceive  = true;
		//////////////////////////////////////////need to deal////////////////////////
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String[] inputPath = {
				System.getProperty("user.home") + "/CactusTest/map_1_out_0",
				System.getProperty("user.home") + "/CactusTest/map_1_out_1" };
		String reduceOutPutFileName = System.getProperty("user.home")
				+ "/CactusTest/" + "reduce_out";
		DealReduceInputUtil tt = new DealReduceInputUtil();
		tt.setInputFilePath(inputPath);
		tt.setReduceOutFileName(reduceOutPutFileName);
		tt.beginToReduce();

		Iterator it = tt.keyValues.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			Object key1 = entry.getKey();
			Object value1 = entry.getValue();
			System.out.println("key : " + key1);
			System.out.println("value : " + value1);
		}
	}


}
