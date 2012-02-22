package cn.ict.cacuts.mapreduce.reduce;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import cn.ict.binos.transmit.BinosURL;
import cn.ict.cacuts.mapreduce.Merger;



public class DealReduceInputUtil<KEY, VALUE> {

	public String[] reduceInputFilePath;
	public BinosURL[] binosURLInput;
	
	//public String reduceOutPutFileName;
//	public Map<KEY, Vector<VALUE>> keyValue;
	private boolean finishedReceive = false;
	// read remote files to save into local disk
	String tmpLocalFilePath;
	String[] readedRemoteReadFiles;
	String mergedTmpFileName;

	public DealReduceInputUtil(){}
	public DealReduceInputUtil(String[] reduceInputFilePath,String tmpLocalFilePath, String mergedTmpFileName) {
		this.reduceInputFilePath = reduceInputFilePath;
		this.tmpLocalFilePath = tmpLocalFilePath;
		this.mergedTmpFileName = mergedTmpFileName;
	
	}
	
	public void prepared(){
		readFiles();
		merge();
	}
	


	public void readFiles() {
		ReadRemoteFile readRemoteFile = null;
		try {
			readRemoteFile = new ReadRemoteFile(reduceInputFilePath,
					tmpLocalFilePath);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		readRemoteFile.launchFetchFiles();
		this.readedRemoteReadFiles = readRemoteFile.getReduceRemoteReadFiles();
	}

	public void merge() {
		Merger merge = new Merger();
		try {
			
			if (null == readedRemoteReadFiles) {
				System.out.println("null == readedRemoteReadFiles");
			}
			for (String tmp: readedRemoteReadFiles) {
				System.out.println("readedRemoteReadFiles:"+tmp);
			}
			merge.merge(readedRemoteReadFiles, mergedTmpFileName, false) ;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void setInputFilePath(String[] reduceInputPath) {
		this.reduceInputFilePath = reduceInputPath;
	}

	public String[] getInputFilePath() {
		return this.reduceInputFilePath;
	}

	public String getTmpLocalFilePath() {
		return tmpLocalFilePath;
	}
	
	public void setTmpLocalFilePath(String tmpLocalFilePath) {
		this.tmpLocalFilePath = tmpLocalFilePath;
	}
	public void FinishedReceive() {
		this.finishedReceive = true;
		// ////////////////////////////////////////need to
		// deal////////////////////////
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
		String mergeFilePath = System.getProperty("user.home") +
				 "/CactusTest/merger_final";
		DealReduceInputUtil tt = new DealReduceInputUtil(inputPath,
				reduceOutPutFileName, mergeFilePath);
		tt.prepared();
	}

}
