package cn.ict.cacuts.mapreduce.map;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.http.entity.SerializableEntity;

import com.transformer.compiler.DataState;

import cn.ict.binos.transmit.BinosURL;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairInt;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntData;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntPar;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntParData;
import cn.ict.cacuts.mapreduce.reduce.FinalKVPair;
import cn.ict.cacuts.mapreduce.MRConfig;
import cn.ict.cacuts.mapreduce.Merger;
import cn.ict.cacuts.mapreduce.WriteIntoDataBus;

public class DealMapOutUtil<KEY, VALUE> {

	private final static Log LOG = LogFactory.getLog(DealMapOutUtil.class);
	////int numberOfReduce = MRConfig.getReduceTaskNum();
	private final int numberOfReduce ;
	private final DataState dataState ;
	////public int size = 1024 * 1024;
	public final long size = 1024 *1024 * 100; // set the memory used by map task	
	
	private KVPairIntParData.Builder inputPairs = KVPairIntParData.newBuilder();
	private KVPairIntParData.Builder backupInputPairs = KVPairIntParData.newBuilder();
	private final ArrayList[] lists;
	private String[] fileName;
	public  String[] mapOutFileIndex = null;//suppose there are no more than 100 interfile
	private final int[] innerFileIndex ;
	private final int[] writeFileIndex;
	KVPairIntPar element;
	KVPairIntPar[] sortedArray;
	private static long capacity = 0; // current capacity
	boolean inputFull = false;
	//boolean writeInputPairs = true;
	int partionedNum;
	int tmpDataNum = 0;	
	String indexString = "";
	
	int whileNum = 0;
	
	HashPartitioner hashPartitioner = new HashPartitioner();
	//private volatile boolean writeFinished = false;
	private AtomicBoolean writeFinished = new AtomicBoolean(false);
	//private volatile boolean writeInputPairs = false;
	private AtomicBoolean writeInputPairs = new AtomicBoolean(false);
	//private volatile boolean allWaiting = false;
	private volatile boolean isAllHandle = false;
	
	private Object writeAction = new Object();
	private Thread writeThread;
	
	private final String tempMapOutFilesPathPrefix;

	public DealMapOutUtil(String[] outputPath, String tempMapOutFilesPathPrefix, DataState state) {
		setOutputPath(outputPath);
		this.dataState = state;
		this.tempMapOutFilesPathPrefix = tempMapOutFilesPathPrefix;
		this.numberOfReduce = outputPath.length;
		lists = new ArrayList[this.numberOfReduce];
		innerFileIndex = new int[this.numberOfReduce];
		writeFileIndex = new int[this.numberOfReduce];
		this.writeThread = new writePairsThread();
		this.writeThread.start();
	}
	

//	public void receive(KEY key, VALUE value) {
//		partionedNum = hashPartitioner.getPartition(key, numberOfReduce);
//		innerFileIndex[partionedNum]++;
//		element = KVPairIntPar.newBuilder().setKey(key.toString()).
//				setValue(Integer.parseInt(value.toString())).setPartition(partionedNum).build();	
//		inputPairs.addKvset(element);
//	//	capacity += element.getSerializedSize();
//		capacity += 1;
//		if (capacity >= size) {
//			capacity = 0;
//			
//			System.arraycopy(innerFileIndex, 0, writeFileIndex, 0, numberOfReduce);
//			dealReceivedUtil(inputPairs);
//			for (int i = 0; i < numberOfReduce; i++) {
//				innerFileIndex[i] = 0;
//			}
//			inputPairs.clearKvset();
//		}
//	}
//	public void FinishedReceive() {
//		if (inputPairs.getKvsetCount() > 0) {
//			System.arraycopy(innerFileIndex, 0, writeFileIndex, 0, numberOfReduce);
//			dealReceivedUtil(inputPairs);
//		}
//	}
	public void receive(KEY key, VALUE value) {
		
//		if (key.toString().equals("while")) {
//			++whileNum;
//		}
		//LOG.info("receive key=" + key + " value=" + value);
		partionedNum = hashPartitioner.getPartition(key, numberOfReduce);
		innerFileIndex[partionedNum]++;
		element = KVPairIntPar.newBuilder().setKey(key.toString()).
				setValue(Integer.parseInt(value.toString())).setPartition(partionedNum).build();	
		if (!writeInputPairs.get()) {
			inputPairs.addKvset(element);
			capacity += element.getSerializedSize();
//			capacity += 1;
			if (capacity >= size) {
				//notify the thread to write the inputPairs to File	
				System.out.println("$$$$$$$$$$$$$$ writeAction.signal()");
				synchronized(writeAction) {
					writeInputPairs.set(true);
					capacity = 0;
					System.arraycopy(innerFileIndex, 0, writeFileIndex, 0, numberOfReduce);
					sortedArray = sortDatas(inputPairs);
					for (int i = 0; i < numberOfReduce; i++) {
						innerFileIndex[i] = 0;
					}
					writeAction.notify();
				}
			}	
		}
		else {						
			backupInputPairs.addKvset(element);
			capacity += element.getSerializedSize();
			if (writeFinished.get()) {
				inputPairs.addAllKvset(backupInputPairs.getKvsetList());
				backupInputPairs.clear();
			}
		}
	}
	class writePairsThread extends Thread {
		public void run() {
			while (!isAllHandle) {
				synchronized (writeAction) {
					while (!writeInputPairs.get()) {
						try {
							writeAction.wait();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					System.out.println("$$$$$$$$$$$$$$ writeAction");
					dealReceivedUtil();
					inputPairs.clearKvset();					
					writeFinished.set(true);
					writeInputPairs.set(false);
				}
				
			}
		}
	}
	public void dealReceivedUtil() {
		tmpDataNum++;
		dealFileIndexContext(writeFileIndex);
		saveDatas();
	}

	private void dealFileIndexContext(int[] innerFilePartionIndex) {
		for (int i = 0; i < writeFileIndex.length; i++) {
			indexString += writeFileIndex[i] + ",";			
		}
		indexString +=";";
	}


	private KVPairIntPar[] sortDatas(KVPairIntParData.Builder inputPairs) {
		long start = System.currentTimeMillis();
		KVPairIntPar[] ss = inputPairs.getKvsetList().toArray(new KVPairIntPar[0]);	
		Arrays.sort(ss, 
				SortStructedData.getComparator());
		System.out.println("sort:" + (System.currentTimeMillis() - start) + "ms");
		return ss;
	}

	private void saveDatas() {
		String dataName = tempMapOutFilesPathPrefix + tmpDataNum;
		long start = System.currentTimeMillis();
		WriteIntoDataBus writer = new WriteIntoDataBus(dataName);
		writer.executeWrite(sortedArray);
		writer.close();
		System.out.println("write:" + (System.currentTimeMillis() - start) + "ms");
	}
	
	private void dealFileIndex(){
		System.out.println("dealFileIndex:" + indexString);
		indexString = indexString.substring(0, indexString.length() - 1);
		mapOutFileIndex = indexString.split(";");
		for(int i = 0 ; i < mapOutFileIndex.length ; i ++){
			mapOutFileIndex[i] = mapOutFileIndex[i].substring(0, mapOutFileIndex[i].length() - 1);
		}	
	}
	public void FinishedReceive() {
		
		while (writeInputPairs.get()) {
			//if the process of writing is running, wait the last write to over.
		}
		this.writeThread.stop();
		if (inputPairs.getKvsetCount() > 0) {
			System.arraycopy(innerFileIndex, 0, writeFileIndex, 0, numberOfReduce);
			sortedArray = sortDatas(inputPairs);
			dealReceivedUtil();
		}
		if (backupInputPairs.getKvsetCount() > 0) {
			System.arraycopy(innerFileIndex, 0, writeFileIndex, 0, numberOfReduce);
			sortedArray = sortDatas(backupInputPairs);
			dealReceivedUtil();
		}
		dealFileIndex();
		
		
//		System.out.println("Map output Merge before: whileNum:" + whileNum);
		
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
//					outputPath[i] = new Path(fileName[i]);
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
	 * @throws IOException 
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException {
		
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
		/*merge test example*/
		/*KVPair element = new KVPair("helloword1", "2", 1);
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
		}*/
		
		//DealMapOutUtil test
		String [] outputPath = {"/tmp/output1", "/tmp/output2", "/tmp/output3"}; 
		long start = System.currentTimeMillis();
		DealMapOutUtil util = new DealMapOutUtil(outputPath, "/tmp/binos-tmp/", DataState.LOCAL_FILE);
		for (int i = 0; i < 1024000; i++) {
			util.receive(String.valueOf(i), i);
			util.receive(String.valueOf(i), i);
		}
		util.FinishedReceive();
		System.out.println((System.currentTimeMillis() - start) + "ms");
	
//		for (int i = 0; i< util.tmpDataNum; i++) {
//			FileInputStream fis = new FileInputStream("/tmp/binos-tmp/" + (i+1));
//			KVPairIntParData.Builder builder = KVPairIntParData.newBuilder();
//			
//			builder = builder.mergeFrom(fis);
//			
//			System.out.println(builder.getKvsetCount());
//			List<KVPairIntPar> list = builder.getKvsetList();
//			for (KVPairIntPar tmp: list) {
//				System.out.println(tmp.getKey() + ":" + tmp.getValue() + ":" + tmp.getPartition());
//			}
//		}
		
		
	}
}
