package cn.ict.cacuts.mapreduce.map;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.http.entity.SerializableEntity;

import com.transformer.compiler.DataState;

import cn.ict.binos.transmit.BinosURL;
import cn.ict.cacuts.mapreduce.MRConfig;
import cn.ict.cacuts.mapreduce.Merger;
import cn.ict.cacuts.mapreduce.WriteIntoDataBus;

public class DealMapOutUtil<KEY, VALUE> {

	private final static Log LOG = LogFactory.getLog(DealMapOutUtil.class);
	////int numberOfReduce = MRConfig.getReduceTaskNum();
	private final int numberOfReduce ;
	private final DataState dataState ;
	////public int size = 1024 * 1024;
	public final long size = 1024 * 1024 * 100; // set the memory used by map task	
	private ArrayList inputPairs = new ArrayList();
	private ArrayList backupInputPairs = new ArrayList();
	private final ArrayList[] lists;
	private String[] fileName;
	public  String[] mapOutFileIndex = null;//suppose there are no more than 100 interfile
	private final int[] innerFilePartionIndex ;
	KVPair element;
	
	private static long capacity = 0; // current capacity
	boolean inputFull = false;
	boolean writeInputPairs = true;
	boolean finishedReceive = false;
	boolean finishedWriteInputPairs = false;
	boolean finishedWriteBackupInputPairs = false;
	int partionedNum;
	int tmpDataNum = 0;
	

	String indexString = "";
	
	HashPartitioner hashPartitioner = new HashPartitioner();
////	String tempMapOutFilesPathPrefix = MRConfig.getTempMapOutFilesPathPrefix()
//			+ "tmpMapOut_";
	//private final String tempMapOutFilesPathPrefix = System.getProperty("user.home")+ "/CactusTest/"
	//+ "tmpMapOut_";
	private final String tempMapOutFilesPathPrefix;

	public DealMapOutUtil(String[] outputPath, String tempMapOutFilesPathPrefix, DataState state) {
		setOutputPath(outputPath);
		this.dataState = state;
		this.tempMapOutFilesPathPrefix = tempMapOutFilesPathPrefix;
		this.numberOfReduce = outputPath.length;
		lists = new ArrayList[this.numberOfReduce];
		innerFilePartionIndex = new int[this.numberOfReduce];
	}
	
	
	private static byte[] getBytes(Object obj) {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		ObjectOutputStream out;
		byte[] bytes = null;
		try {
			out = new ObjectOutputStream(bout);
			out.writeObject(obj);
			out.flush();
			bytes = bout.toByteArray();
			bout.close();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return bytes;
	}
  
	public void receive(KEY key, VALUE value) {

		if (!finishedReceive) {
			if (writeInputPairs) {
				partionedNum = hashPartitioner
						.getPartition(key, numberOfReduce);
				innerFilePartionIndex[partionedNum]++;
				element = new KVPair(key, value, partionedNum);
				
				inputPairs.add(element);
				capacity += getBytes(element).length;
				//System.out.println(element.toString() + " length:" + getBytes(element).length);
				if (capacity >= size) {
					System.out.println("inputPairs.size() == size capacity:"+
							capacity + " "
							+ inputPairs.size());
					writeInputPairs = false;
					finishedWriteInputPairs = false;
					dealReceivedUtil(inputPairs, innerFilePartionIndex);
					for(int i = 0; i < this.numberOfReduce; i++) {
						innerFilePartionIndex[i] = 0;
					}
					inputPairs.clear();
					capacity = 0;
					finishedWriteInputPairs = true;
				}
			} else {
				partionedNum = hashPartitioner
						.getPartition(key, numberOfReduce);
				innerFilePartionIndex[partionedNum]++;
				element = new KVPair(key, value, partionedNum);
				backupInputPairs.add(element);
				//System.out.println(element.toString() + " length:" + getBytes(element).length);
				capacity += getBytes(element).length;
				
				if (capacity >= size) {
					System.out.println("backupInputPairs.size() == size replaced by capacity:" +
							capacity + " "
							+ backupInputPairs.size());
					finishedWriteBackupInputPairs = false;
					if (finishedWriteInputPairs) {
						writeInputPairs = true;
					}
					dealReceivedUtil(backupInputPairs, innerFilePartionIndex);
					for(int i = 0; i < this.numberOfReduce; i++) {
						innerFilePartionIndex[i] = 0;
					}
					backupInputPairs.clear();
					capacity = 0;
					finishedWriteBackupInputPairs = true;
				}

			}
		}
	}



	public void dealReceivedUtil(ArrayList inputPairs,int[] innerFilePartionIndex) {
		tmpDataNum++;
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
		String dataName = tempMapOutFilesPathPrefix + tmpDataNum;
		SaveDatas(sortDatas(inputPairs), dataName);
	}

	public Object[] sortDatas(ArrayList inputPairs) {
		Object[] ss = inputPairs.toArray();
		Arrays.sort(ss, SortStructedData.getComparator());
		return ss;
	}

	public void SaveDatas(Object[] sorted, String fileName) {
		WriteIntoDataBus tt = new WriteIntoDataBus(fileName);
		tt.executeWrite(sorted);
	}
	/**
	 * the last phase of map process is to merge.
	 */
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
		
		/*Merge the small file into the number of file.*/
		
		Merger merge = new Merger();
		Path[] inputPath;
		Path[] outputPath;
		if (tmpDataNum > 0) {
			inputPath = new Path[tmpDataNum];
			for (int i = 0; i < tmpDataNum; i++) {
				inputPath[i] = new Path(tempMapOutFilesPathPrefix + (i+1));
			}
			if (null != fileName) {
				outputPath = new Path[fileName.length];
				for (int i = 0; i < fileName.length; i++) {
					outputPath[i] = new Path( BinosURL.getPath(
							new BinosURL(new Text(fileName[i]) )));
				}
				if (null != mapOutFileIndex)
					try {
						merge.merge(inputPath, mapOutFileIndex, outputPath, true,this.dataState);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
			else {
				LOG.error("MapPhase: Merge process occurs a error: \n" 
						+ "There are no files configured!");
				System.exit(-1);
			}
		}
		else {
			LOG.error("MapPhase: Merge process occurs a error: \n" 
					+ "There are " + this.tmpDataNum + "intermediate files.");
			System.exit(-1);
		}
		
		System.out.println("***********************over********************");
	}
	
	public void dealFileIndex(){
		System.out.println("dealFileIndex:" + indexString);
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
//		String[] keys = { "pear", "banana", "orange", "cat", "apple", "moon","egg" };
//		int[] values = { 1, 7, 5, 10, 2, 4, 11 };
//		int[] partitions = { 3,2,1,3,2,1 ,2};
//
//		
//		DealMapOutUtil tt = new DealMapOutUtil();
////		for (int i = 0; i < keys.length; i++) {
////			tt.receive(keys[i], values[i]);
////		}
//		
//		for (int i = 0; i < 500; i++) {
//			tt.receive(keys[i%6], i);
//		}
		

//		tt.FinishedReceive();
//		for(int i = 0 ; i < tt.mapOutFileIndex.length ; i ++ ){
//			System.out.println( tt.mapOutFileIndex[i]);
//		}
		KVPair element = new KVPair("helloword1", "2", 1);
		byte[] data = getBytes(element);
		System.out.println(data.length);
		Merger merger = new Merger();
		String pathPrefix = System.getProperty("user.home")+ "/CactusTest/"
		+ "tmpMapOut_";
		String[] index = {"208373,299854", "208353,299874", "208190,300036","19454,28254"};
		Path [] input = new Path[4];
		for (int i = 0; i < input.length; i++) {
			input[i] = new Path(pathPrefix + (i+1));
		}
		Path [] output = new Path[2];
		output[0] = new Path("/tmp/testoutput0");
		output[1] = new Path("/tmp/testoutput1");
		try {
			merger.merge(input, index, output, false, DataState.LOCAL_FILE);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
