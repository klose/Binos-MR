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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

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
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntParData.Builder;
import cn.ict.cacuts.mapreduce.reduce.FinalKVPair;
import cn.ict.cacuts.mapreduce.KeyValue;
import cn.ict.cacuts.mapreduce.MRConfig;
import cn.ict.cacuts.mapreduce.Merger;
import cn.ict.cacuts.mapreduce.WriteIntoDataBus;

public class DealMapOutUtil<KEY, VALUE> {

	private final static Log LOG = LogFactory.getLog(DealMapOutUtil.class);
	////int numberOfReduce = MRConfig.getReduceTaskNum();
	private final int numberOfReduce ;
	private final DataState dataState ;
	////public int size = 1024 * 1024;
	public final long size = 1024 *1024 * 10; // set the memory used by map task	
	
	private KVPairIntParData.Builder inputPairs = KVPairIntParData.newBuilder();

	private final ArrayList[] lists;
	private String[] fileName;
	public  String[] mapOutFileIndex = null;//suppose there are no more than 100 interfile
//	private final int[] innerFileIndex;
	//private final int[] writeFileIndex;
//	private AtomicIntegerArray innerFileIndex;
	private AtomicIntegerArray writeFileIndex;
	KVPairIntPar element;
	private volatile KVPairIntPar[] sortedArray;
	private static long capacity = 0; // current capacity
	boolean inputFull = false;
	//boolean writeInputPairs = true;
	int partionedNum;
	String indexString = "";
	int whileNum = 0;
	long allstart;
	HashPartitioner hashPartitioner = new HashPartitioner();
	private AtomicInteger tmpDataNum = new AtomicInteger(0);
	//private volatile boolean writeFinished = false;
	private volatile boolean isAllHandle = false;
	private List <Thread> allThreads = new ArrayList<Thread>(); 
	private final String tempMapOutFilesPathPrefix;
	
	public DealMapOutUtil(String[] outputPath, String tempMapOutFilesPathPrefix, DataState state) {
		setOutputPath(outputPath);
		this.dataState = state;
		this.tempMapOutFilesPathPrefix = tempMapOutFilesPathPrefix;
		this.numberOfReduce = outputPath.length;
		lists = new ArrayList[this.numberOfReduce];
		writeFileIndex = new AtomicIntegerArray(this.numberOfReduce);
		for (int i = 0; i < this.numberOfReduce; i++) {
			writeFileIndex.set(i, 0);
		}
		allstart = System.currentTimeMillis();
	}

	public void receive(KEY key, VALUE value) {
		
		//LOG.info("receive key=" + key + " value=" + value);
		partionedNum = hashPartitioner.getPartition(key, numberOfReduce);
		writeFileIndex.addAndGet(partionedNum, 1);
		element = KVPairIntPar.newBuilder().setKey(key.toString()).
				setValue(Integer.parseInt(value.toString())).setPartition(partionedNum).build();
		capacity += element.getSerializedSize();
		inputPairs.addKvset(element);
		if (capacity >= size) {
			// notify the thread to write the inputPairs to File
			launchSortAndWrite(inputPairs.clone());
			capacity = 0;
			inputPairs.clear();
			dealFileIndexContext();
			for (int i = 0; i < numberOfReduce; i++) {
				writeFileIndex.set(i, 0);
			}
		}	
	}
	private void launchSortAndWrite(KVPairIntParData.Builder value) {
		Thread tmp = new SortWriteThread(value);
		allThreads.add(tmp);
		tmp.start();
	}
	class SortWriteThread extends Thread {
		KVPairIntParData.Builder value;
		public SortWriteThread(KVPairIntParData.Builder value) {
			this.value = value;
		}
		public void run() {	
			dealReceivedUtil(this.value);
		}
	
	}
	public void dealReceivedUtil(KVPairIntParData.Builder value) {
		saveDatas(sortDatas(value));
	}

	private void dealFileIndexContext() {
		for (int i = 0; i < writeFileIndex.length(); i++) {
			indexString += writeFileIndex.get(i) + ",";			
		}
		indexString +=";";
	}


	private KVPairIntPar[] sortDatas(KVPairIntParData.Builder value) {
		long start = System.currentTimeMillis();
		KVPairIntPar[] ss = value.getKvsetList().toArray(new KVPairIntPar[0]);	
		System.out.println("inputPairs.toArray: + size=" + ss.length + " "+ (System.currentTimeMillis() - start) + "ms");
		Arrays.sort(ss, 
				SortStructedData.getComparator());
		LOG.info("sort:" + (System.currentTimeMillis() - start) + "ms");
		return ss;
	}

	private void saveDatas(KVPairIntPar[] sortedArray) {
		String dataName = tempMapOutFilesPathPrefix + tmpDataNum.incrementAndGet();
		long start = System.currentTimeMillis();
		WriteIntoDataBus writer = new WriteIntoDataBus(dataName);
		writer.executeWrite(sortedArray);
		writer.close();
		LOG.info("write:" + (System.currentTimeMillis() - start) + "ms");
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
		
		if (tmpDataNum.get() == 0 && inputPairs.getKvsetCount() > 0) {
			LOG.info("mapper process only one intermediate file");
			//System.arraycopy(innerFileIndex, 0, writeFileIndex, 0, numberOfReduce);
			dealReceivedUtil(inputPairs);
		}
		for (Thread tmp: allThreads) {
			try {
				if (tmp.isAlive()) {
					tmp.join();
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		LOG.info("before merge: Mapper process uses" + (System.currentTimeMillis() - allstart) + "ms");
		
		dealFileIndex();
		
		
		
		/*Merge the small file into the number of file.*/
		long start = System.currentTimeMillis();
		Merger merge = new Merger();
		Path[] inputPath;
		Path[] outputPath;
		if (tmpDataNum.get() > 0) {
			inputPath = new Path[tmpDataNum.get()];
			for (int i = 0; i < tmpDataNum.get(); i++) {
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
						LOG.info("merge:" + (System.currentTimeMillis() - start) + "ms");
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
//		
//		AtomicIntegerArray aa new AtomicIntegerArray(3);
//		System.out.println(aa.get(0));
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
