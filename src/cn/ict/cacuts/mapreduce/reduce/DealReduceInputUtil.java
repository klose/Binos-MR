package cn.ict.cacuts.mapreduce.reduce;

import java.util.ArrayList;

import cn.ict.cacuts.mapreduce.mapcontext.Sort;
import cn.ict.cacuts.mapreduce.mapcontext.WriteIntoFile;

public class DealReduceInputUtil {

	public int reduceNum = 2;
	public String[] reduceInputFilePath;
	ArrayList dealed = new ArrayList();
	public String reduceOutPutFileName;
	
	public void beginToReduce(){
		prepared();
		writeIntoFile();
	}
	
	private void writeIntoFile() {
		WriteIntoFile write = new WriteIntoFile();
		write.writeIntoFile(dealed, reduceOutPutFileName);
		
	}

	public void prepared() {
	//	ArrayList dealed = new ArrayList();
		dealed = readFiles(reduceInputFilePath[0]);
		for (int i = 1; i < reduceInputFilePath.length; i++) {
			dealed = readMergeSort(dealed, reduceInputFilePath[i]);
		}
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

	public ArrayList readFiles(String fileName) {
		ReadFile readFile = new ReadFile(fileName);
		ArrayList readed = readFile.readMethod2();
		return readed;
	}

	// public ArrayList mergeFiles(ArrayList dealed,ArrayList justReaded){
	// ArrayList newArray = new ArrayList();
	// newArray = dealed.addAll(justReaded);
	//
	// }

	public ArrayList reduceClass(ArrayList sorted) {
		ArrayList combined = new ArrayList();
		//TODO combine
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
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String[] inputPath = {
				System.getProperty("user.home") + "/CactusTest/map_1_out_0",
				System.getProperty("user.home") + "/CactusTest/map_1_out_1" };
		String reduceOutPutFileName = System.getProperty("user.home") + "/CactusTest/" + "reduce_out";
		DealReduceInputUtil tt = new DealReduceInputUtil();
		tt.setInputFilePath(inputPath);
		tt.setReduceOutFileName(reduceOutPutFileName);
		tt.beginToReduce();
		System.out.println(tt.dealed.size());
		for(int i = 0 ; i < tt.dealed.size(); i ++){
			System.out.println(tt.dealed.get(i));
		}
	}

}
