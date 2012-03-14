package cn.ict.cacuts.mapreduce.map;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
import cn.ict.cacuts.mapreduce.Combiner;
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
	
	//private final AtomicIntegerArray writeFileIndex;
	//private final AtomicIntegerArray currentBytesIndex;
	private final AtomicIntegerArray currentObjectsIndex;
	//private final String[] writeBytesIndex; //each record represents as ***,***,***, meaning that the files separate.
	//private final String[] writeObjectsIndex;// each record represents as ***,***,***, meaning that the objects separate.

	private KVPairIntPar element;
	private volatile KVPairIntPar[] sortedArray;
	private static long capacity = 0; // current capacity
	boolean inputFull = false;
	//boolean writeInputPairs = true;
	private int partitionedNum;
	private Class<? extends Combiner> combineClass;
	long allstart;
	HashPartitioner hashPartitioner = new HashPartitioner();
	private AtomicInteger tmpDataNum = new AtomicInteger(0);
	//private volatile boolean writeFinished = false;
	private volatile boolean isAllHandle = false;
	private List <Thread> allThreads = new CopyOnWriteArrayList<Thread>(); 
	private final String tempMapOutFilesPathPrefix;
	
	public DealMapOutUtil(String[] outputPath, String tempMapOutFilesPathPrefix, DataState state) {
		this(outputPath, tempMapOutFilesPathPrefix,state, null);
	}
	public DealMapOutUtil(String[] outputPath, String tempMapOutFilesPathPrefix, 
			DataState state, String combineClassName) {
		setOutputPath(outputPath);
		this.dataState = state;
		this.tempMapOutFilesPathPrefix = tempMapOutFilesPathPrefix;
		this.numberOfReduce = outputPath.length;
		this.lists = new ArrayList[this.numberOfReduce];
		this.currentObjectsIndex = new AtomicIntegerArray(this.numberOfReduce);
		for (int i = 0; i < this.numberOfReduce; i++) {
			currentObjectsIndex.set(i, 0);
		}
		if (combineClassName != null) {
			try {
				this.combineClass = (Class<? extends Combiner>) Class.forName(combineClassName);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
		else {
			this.combineClass = null;
		}
		allstart = System.currentTimeMillis();
	}
	public void receive(KEY key, VALUE value) {
		
		//LOG.info("receive key=" + key + " value=" + value);
		partitionedNum = hashPartitioner.getPartition(key, numberOfReduce);
		currentObjectsIndex.addAndGet(partitionedNum, 1);
		element = KVPairIntPar.newBuilder().setKey(key.toString()).
				setValue(Integer.parseInt(value.toString())).setPartition(partitionedNum).build();
		int elementSize = element.getSerializedSize();
		//currentBytesIndex.addAndGet(partitionedNum, elementSize);
		capacity += element.getSerializedSize();
		inputPairs.addKvset(element);
		if (capacity >= size) {
			// notify the thread to write the inputPairs to File
			launchSortAndWrite(inputPairs.clone(), tmpDataNum.incrementAndGet(), currentObjectsIndex);
			capacity = 0;
			inputPairs.clear();
			//dealFileIndexContext();
			for (int i = 0; i < numberOfReduce; i++) {
				currentObjectsIndex.set(i, 0);
			}
		}	
	}
	private void launchSortAndWrite(KVPairIntParData.Builder value, int num, AtomicIntegerArray index) {
		/*copy the number of objects in each section.*/
		int [] tmpIndex = new int[this.numberOfReduce];
		for (int i = 0; i < this.numberOfReduce; i++) {
			tmpIndex[i] = index.get(i);
		}
		Thread tmp = new SortWriteThread(value, num, tmpIndex);
		allThreads.add(tmp);
		tmp.start();
	}
	class SortWriteThread extends Thread {
		KVPairIntParData.Builder value;
		int tmpNum;
		int[] tmpIndex;
		public SortWriteThread(KVPairIntParData.Builder value, int num, int[] tmpIndex) {
			this.value = value;
			this.tmpNum = num;
			this.tmpIndex = tmpIndex;
		}
		public void run() {	
			dealReceivedUtil(this.value, this.tmpNum, this.tmpIndex);
		}
	
	}
	public void dealReceivedUtil(KVPairIntParData.Builder value, int tmpFileNum, int[] tmpIndex) {
		saveDatas(sortDatas(value), tmpFileNum, tmpIndex);
	}

//	private void dealFileIndexContext() {
//		for (int i = 0; i < this.numberOfReduce; i++) {
//			writeBytesIndex[i] += currentBytesIndex.get(i) + ",";
//			writeObjectsIndex[i] += currentObjectsIndex.get(i) + ",";			
//		}
//	}


	private KVPairIntPar[] sortDatas(KVPairIntParData.Builder value) {
		long start = System.currentTimeMillis();
		KVPairIntPar[] ss = value.getKvsetList().toArray(new KVPairIntPar[0]);	
		System.out.println("inputPairs.toArray: + size=" + ss.length + " "+ (System.currentTimeMillis() - start) + "ms");
		Arrays.sort(ss, 
				SortStructedData.getComparator());
		LOG.info("sort:" + (System.currentTimeMillis() - start) + "ms");
		return ss;
	}

	private void saveDatas(KVPairIntPar[] sortedArray, int tmpNum, int[] tmpIndex) {
		String dataName = tempMapOutFilesPathPrefix + tmpNum;
		long start = System.currentTimeMillis();
		int startIndex = 0;
		for (int i = 0 ; i < this.numberOfReduce; i++) {
			WriteIntoDataBus writer = new WriteIntoDataBus(dataName+"-" + i);
			writer.executeWrite(sortedArray, startIndex, startIndex + tmpIndex[i]);
			writer.close();
			startIndex += tmpIndex[i];
		}
		LOG.info("write:" + (System.currentTimeMillis() - start) + "ms");
	}
	
	class MergeThread extends Thread{
		private Path[] inputPath;
		private Path outputPath;
		private final int count; // the number of intermediate files
		private final int id;
		private final String outPath;
		MergeThread(int count, int id, String path) {
			this.count = count;
			this.id = id;
			this.outPath = path;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			/*Merge the small file into the number of file.*/
			long start = System.currentTimeMillis();
			Merger merge = new Merger(combineClass);
			if (this.count > 0) {
				this.inputPath = new Path[this.count];
				for (int i = 0; i < this.count; i++) {
					inputPath[i] = new Path(tempMapOutFilesPathPrefix + (i+1) + "-" + this.id);
					LOG.info(inputPath[i].toString());
				}
				if (null != outPath) {
					this.outputPath = new Path( BinosURL.getPath(
							new BinosURL(new Text(this.outPath))));
					LOG.info(this.outputPath.toString());
					LOG.info(dataState);
					try {
						merge.merge(this.inputPath,  this.outputPath, true, dataState, KVPairIntPar.class);
						LOG.info("merge:" + (System.currentTimeMillis() - start) + "ms");
						LOG.info("combine in merge: " + merge.combineUsedTime + "ms");
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
						+ "There are " + this.count + "intermediate files.");
				System.exit(-1);
			}
			System.out.println("***********************over********************");
		}
		
	}
//	private void dealFileIndex(){
//		System.out.println("dealFileIndex:" + indexString);
//		indexString = indexString.substring(0, indexString.length() - 1);
//		mapOutFileIndex = indexString.split(";");
//		for(int i = 0 ; i < mapOutFileIndex.length ; i ++){
//			mapOutFileIndex[i] = mapOutFileIndex[i].substring(0, mapOutFileIndex[i].length() - 1);
//		}	
//	}
	public void FinishedReceive() {
		
		if (inputPairs.getKvsetCount() > 0) {
			LOG.info("mapper process only one intermediate file");
			int [] tmpIndex = new int[this.numberOfReduce];
			for (int i = 0; i < this.numberOfReduce; i++) {
				tmpIndex[i] = currentObjectsIndex.get(i);
			}
			dealReceivedUtil(inputPairs, tmpDataNum.incrementAndGet(), tmpIndex);
		}
		for (Thread tmp: allThreads) {
			try {
				if (tmp.isAlive()) {
					tmp.join();
				}
				allThreads.remove(tmp);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//LOG.info("before merge: Mapper process uses" + (System.currentTimeMillis() - allstart) + "ms");		
		/*Merge the small file into the number of file.*/
		long start = System.currentTimeMillis();
		for (int i = 0; i < this.numberOfReduce; i++) {
			MergeThread thread = new MergeThread(tmpDataNum.get(), i, fileName[i]);
			thread.start();
			allThreads.add(thread);
		}
		
		for (Thread tmp: allThreads) {
			try {
				if (tmp.isAlive()) {
					tmp.join();
				}
				allThreads.remove(tmp);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		LOG.info("all merge use:" + (System.currentTimeMillis() - start) + "ms");
		LOG.info("Mapper process use " + (System.currentTimeMillis() - allstart) + "ms totally.");
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
