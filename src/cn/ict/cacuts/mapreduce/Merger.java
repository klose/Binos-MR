package cn.ict.cacuts.mapreduce;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import com.transformer.compiler.DataState;

import cn.ict.binos.transmit.MessageClientChannel;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairInt;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntList;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntPar;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntParData;
import cn.ict.cacuts.mapreduce.map.KVList;
import cn.ict.cacuts.mapreduce.map.KVPair;
import cn.ict.cacuts.mapreduce.map.MapContext;

/**
 * Merge small files into large file. 
 * @author Bing Jiang
 *
 */
public class Merger extends PriorityQueue{
	
	
	private final static Log LOG = LogFactory.getLog(Merger.class);
	
	private Class<? extends Combiner> combinerClass;
	private Combiner combinerInstance = null; 
	public long combineUsedTime = 0;
	public Merger() {
		this(null);
	}

	public Merger(Class<? extends Combiner> combinerCls) {
		this.combinerClass = combinerCls;
		if (this.combinerClass != null) {
			try {
				Constructor<Combiner> meth = (Constructor<Combiner>) this.combinerClass
						.getConstructor(new Class[0]);
				meth.setAccessible(true);
				this.combinerInstance = meth.newInstance();
				System.out.println("Right Merge combine class:" + this.combinerClass.getName());
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Merge the intermediate file that map() generate.The function will generate the array of output path whose 
	 * length equals the number of reduce tasks.  
	 * @param input: the input path of file.
	 * @param index: every String uses the format "4,5,7,...", in order to represent the different part of the input file,
	 * 					and each part represents data that reduce task needs to handle accordingly.    
	 * @param output: generate final file that Map phase generate.
	 * @param isDelete: whether or not to delete the file as the map phase ends.
	 * @param state: the type of handling the intermediate data
	 * @throws IOException
	 */
	public <K extends Object, V extends Object> 
		void merge(Path[] input, String[] index, Path[] output, boolean isDelete, DataState state) throws IOException {
		if (state == DataState.LOCAL_FILE) {
			mergeOnLocalFile(input, index, output, isDelete);
		}
		else {
			mergeOnMsgPool(input, index, output, isDelete);
		}
	}
	
	private <K extends Object, V extends Object> 
		void mergeOnMsgPool(Path[] input, String[] index, Path[] output, boolean isDelete) throws IOException {
		int length = input.length;
		ReadFromDataBus[] reader = new ReadFromDataBus[length];
		int [][] readCount = new int[input.length][output.length];
//		MessageClientChannel mcc = new MessageClientChannel();
		for (int i = 0; i < length; i++) {
			String[] tmp = index[i].split(",");
			for (int j = 0; j < output.length; j++) {
				readCount[i][j] = Integer.parseInt(tmp[j]);
			}
			reader[i] = new ReadFromDataBus(input[i].toString());
			System.out.println(input[i].toString());
		}
		for (int k = 0 ; k < output.length; k++) {
			System.out.println(output[k].toString());
		}
		
		/*record that the number of each part whose records' number equals to 0. 
		 * In the situation, set the value of isSkipPath[i] to true*/
		int skipPathNum = 0;
		boolean [] isSkipPath = new boolean[length];
		int allocateNum = 0;
		for (int k = 0 ; k < output.length; k++) {
			skipPathNum = 0;
			allocateNum = 0;
			for (int i = 0; i < length; i++) {
				allocateNum += readCount[i][k]; 
				if (readCount[i][k] == 0) {
					skipPathNum ++;
					isSkipPath[i] = true;
				}
				else {
					isSkipPath[i] = false;
				}
			}
			initialize(allocateNum);
			for (int i = 0; i < length; i++) {
				if (isSkipPath[i]) {
					continue;
				}
				for (int j = 0; j < readCount[i][k]; j++)
					insert((KVPairIntPar)(reader[i].getOneKVPairIntPar()));	
			}
			KVPairIntPar pair = (KVPairIntPar) pop();
			String originKey = pair.getKey();
			KVPairIntList.Builder builder = KVPairIntList.newBuilder();
			KVPairIntList list;
			builder.setKey(originKey).addVlist(pair.getValue());
			WriteIntoDataBus writer = new WriteIntoDataBus(output[k].toString());
			if (allocateNum == 1) {
				list = builder.build();
				writer.appendKVPairIntList(list);
			}
			else {
				for (int i = 1; i < allocateNum; i++) {
					pair = (KVPairIntPar) pop();
					String key = pair.getKey();
					if (!key.equals(originKey)) {
						list = builder.build();
						writer.appendKVPairIntList(list);
						builder = KVPairIntList.newBuilder();
						builder.setKey(key).addVlist(pair.getValue());
						originKey = key;
					}
					else {
						builder.addVlist(pair.getValue());
					}
				}
				writer.appendKVPairIntList(builder.build());
			}
			writer.close();
		}
		clear();
		if (isDelete) {
			MessageClientChannel mcc = new MessageClientChannel();
			for (int i = 0; i < length; i++) {
				mcc.FreeAllData(input[i].toString());
			}
		}
		for (int i = 0; i < length; i++) {
			reader[i].close();
		}	
		
	}
	private <K extends Object, V extends Object>  
		void mergeOnLocalFile(Path[] input, String[] index, Path[] output, boolean isDelete) throws FileNotFoundException, IOException {
		int length = input.length;
		ReadFromDataBus [] reader = new ReadFromDataBus[length];
		int [][] readCount = new int[input.length][output.length];
		
		/*store KVPairs from each input */
		//KVPair<K, V> [] curPair = new KVPair[length];
		
		/*store the index position that KVPair go through.*/
		//int [] curIndex = new int[length];
		
		/*get the record number of each part in every input path.
		 * and initialize the value of correlative variables*/
		for (int i = 0; i < length; i++) {
			reader[i] = new ReadFromDataBus(input[i].toString());
			
			String[] tmp = index[i].split(",");
			for (int j = 0; j < output.length; j++) {
				readCount[i][j] = Integer.parseInt(tmp[j]);
			}
		}
		
		/*record that the number of each part whose records' number equals to 0. 
		 * In the situation, set the value of isSkipPath[i] to true*/
		int skipPathNum = 0;
		boolean [] isSkipPath = new boolean[length];
		
		int allocateNum = 0;
		for (int k = 0 ; k < output.length; k++) {
			long start = System.currentTimeMillis();
			skipPathNum = 0;
			allocateNum = 0;
			for (int i = 0; i < length; i++) {
				allocateNum += readCount[i][k]; 
				if (readCount[i][k] == 0) {
					skipPathNum ++;
					isSkipPath[i] = true;
				}
				else {
					isSkipPath[i] = false;
				}
			}
			initialize(allocateNum);
			for (int i = 0; i < length; i++) {
				if (isSkipPath[i]) {
					continue;
				}
				for (int j = 0; j < readCount[i][k]; j++)
					insert((reader[i].readKVPairIntPar()));
			}
			
			LOG.info("insert data use:" + (System.currentTimeMillis() - start) + "ms");
			start = System.currentTimeMillis();
			
			KVPairIntPar pair = (KVPairIntPar) pop();
			String originKey = pair.getKey();
			KVPairIntList.Builder builder = KVPairIntList.newBuilder();
			KVPairIntList list;
			builder.setKey(originKey).addVlist(pair.getValue());
			WriteIntoDataBus writer = new WriteIntoDataBus(output[k].toString());
			if (allocateNum == 1) {
				list = builder.build();
				writer.writeKVPairIntList(list);
			}
			else {
				for (int i = 1; i < allocateNum; i++) {
					pair = (KVPairIntPar) pop();
					String key = pair.getKey();
					if (!key.equals(originKey)) {
						list = builder.build();	
						writer.writeKVPairIntList(list);
						builder = KVPairIntList.newBuilder();
						builder.setKey(key).addVlist(pair.getValue());
						originKey = key;
					}
					else {
						builder.addVlist(pair.getValue());
					}
				}
				writer.writeKVPairIntList(builder.build());
			}
			writer.close();
			clear();
			
			LOG.info("combine and write data use:" + (System.currentTimeMillis() - start) + "ms");
		}
		
		if (isDelete) {
			for (int i = 0; i < length; i++) {
				new File(input[i].toString()).delete();
			}
		}
		for (int i = 0; i < length; i++) {
			reader[i].close();
		}
	}
	
	
	
	
	/**
	 * Merge a array of files into a large sequential file
	 * @param input: the array of file 
	 * @param output: the file that merge some files into.
	 * @param isDelete: whether to delete the file at the end of successful operation.
	 * @param objectClass: the data class in the input.
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	
	public  <K extends Object, V extends Object> void merge(Path[] input, Path output, boolean isDelete, 
				DataState state, Class objectClass) throws FileNotFoundException, IOException {
		if (state == DataState.LOCAL_FILE) {
			mergeOnLocalFile(input, output, isDelete, objectClass);
		}
		else if (state == DataState.MESSAGE_POOL){
			mergeOnMsgPool(input,output,isDelete,objectClass);
		}
	}
	private void mergeOnMsgPool(Path[] input, Path output, boolean isDelete,
			Class objectClass) {
		if (objectClass == KVPairIntPar.class) {
			mergeKVPairIntParOnMsgPool(input, output,isDelete);
		}
		else if (objectClass == KVPairIntList.class) {
			mergeKVPairIntListOnMsgPool(input, output, isDelete);
		}
	}
	private void mergeOnLocalFile(Path[] input, Path output, boolean isDelete,Class objectClass) {
		if (objectClass == KVPairIntPar.class) {
			mergeKVPairIntParOnLocalFile(input, output,isDelete);
		}
		else if (objectClass == KVPairIntList.class) {
			try {
				mergeKVPairIntListOnLocalFile(input, output, isDelete);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private void mergeKVPairIntParOnLocalFile(Path[] input, Path output, boolean isDelete) {
		int length = input.length;
		ReadFromDataBus[] reader = new ReadFromDataBus[length];
		File[] inputFile = new File[length];
		boolean[] isSkipPath = new boolean[length];
		int initialSize = length;
		for (int i = 0; i < length; i++) {
			String path = input[i].toString();
			inputFile[i] = new File(path);
			reader[i] = new ReadFromDataBus(path);
			
			/*eliminate the file without data.*/
			if (inputFile[i].length() >0) {
				isSkipPath[i] = false;
			}
			else {
				isSkipPath[i] = true;
				initialSize --;
			}
		}
		
		/*handle special situation: each file has no data.*/
		if (initialSize <= 0) {
			return;
		}
		
		/*initialize the heap size*/
		initialize(initialSize);
		
		/*searchPathIndex is used to store the KVList in the Priority Queue and correlated index of input stream. 
		 */
		
		
		//Map<KVPairIntList, Integer> searchPathIndex = new WeakHashMap<KVPairIntList,Integer>(initialSize);
		//Map<Integer, Integer> searchPathIndex = new WeakHashMap<Integer,Integer>(initialSize);
		/*initialize the heap with first record from every file.*/
		for (int i = 0; i < length && !isSkipPath[i]; i++) {
				KVPairIntPar tmp = reader[i].readKVPairIntPar();
//				System.out.println("index=" + i +  "hashcode:" + tmp.hashCode());
				if(tmp != null) {
					
					insert(new KVPairIntParObject(tmp,i));
					//searchPathIndex.put(tmp.hashCode(), i);
				}
				else {
					isSkipPath[i] = true;
					initialSize --;
				}
		}
		WriteIntoDataBus writer = new WriteIntoDataBus(output.toString());
		System.out.println(output.toString());
		String originKey = null;
		//KVPairIntList curList = null;
		KVPairIntList.Builder builder = null;
		while (true) {
			if (initialSize == 0) {
				break;
			}
			KVPairIntParObject tmpObject = (KVPairIntParObject)pop();
			KVPairIntPar tmp = tmpObject.getKVPairIntPar();
			//System.out.println("while: pop()"+ tmp.toString());
			if (tmp == null) {
				break;
			}
			if(null != originKey) {
				if (originKey.equals(tmp.getKey())) {
					builder.addVlist(tmp.getValue());
				}
				else {
					if (this.combinerInstance != null) {
						long start = System.currentTimeMillis();
						List combineVlist = this.combinerInstance.combine(originKey,builder.getVlistList());
						LOG.debug("before combiner:" + builder.toString());
						builder.clearVlist();
						builder.addAllVlist(combineVlist);
						LOG.debug("after combiner:" + builder.toString());
						combineUsedTime += System.currentTimeMillis() - start;
					}
					writer.writeKVPairIntList(builder.build());
					builder = KVPairIntList.newBuilder();
					originKey = tmp.getKey();
					builder.setKey(originKey).addVlist(tmp.getValue());
				}
			}
			else {
				originKey =  tmp.getKey();
				builder = KVPairIntList.newBuilder();
				builder.setKey(originKey).addVlist(tmp.getValue());
			}
				
			int i = tmpObject.getFileIndex();
			tmp = reader[i].readKVPairIntPar();
			//System.out.println(tmp.toString());
			if (null != tmp) {
					insert(new KVPairIntParObject(tmp,i));	
			}
			else {
				if (!isSkipPath[i]) {
					initialSize --;
					isSkipPath[i] = true;
				}
			}
		}
		/*ensure that the last curList is writen to file*/
		if (builder.isInitialized())
		     writer.writeKVPairIntList(builder.build());
		clear();
		writer.close();
		for (int i = 0; i < length; i++) {
			reader[i].close();
		}
		if (isDelete) {
			for (int i = 0; i < length; i++) {
				inputFile[i].delete();
			}
		}
	}
	private void mergeKVPairIntListOnLocalFile(Path[] input, Path output, boolean isDelete) throws IOException {
		int length = input.length;
		ReadFromDataBus[] reader = new ReadFromDataBus[length];
		File[] inputFile = new File[length];
		boolean[] isSkipPath = new boolean[length];
		int initialSize = length;
		for (int i = 0; i < length; i++) {
			String path = input[i].toString();
			inputFile[i] = new File(path);
			reader[i] = new ReadFromDataBus(path);
			
			/*eliminate the file without data.*/
			if (inputFile[i].length() >0) {
				isSkipPath[i] = false;
			}
			else {
				isSkipPath[i] = true;
				initialSize --;
			}
		}
		
		/*handle special situation: each file has no data.*/
		if (initialSize <= 0) {
			return;
		}
		
		/*initialize the heap size*/
		initialize(initialSize);
		
		/*searchPathIndex is used to store the KVList in the Priority Queue and correlated index of input stream. 
		 */
		
		
		//Map<KVPairIntList, Integer> searchPathIndex = new WeakHashMap<KVPairIntList,Integer>(initialSize);
		//Map<Integer, Integer> searchPathIndex = new WeakHashMap<Integer,Integer>(initialSize);
		/*initialize the heap with first record from every file.*/
		for (int i = 0; i < length && !isSkipPath[i]; i++) {
				KVPairIntList tmp = reader[i].readKVPairIntList();
//				System.out.println("index=" + i +  "hashcode:" + tmp.hashCode());
				if(tmp != null) {
					
					insert(new KVPairIntListObject(tmp,i));
					//searchPathIndex.put(tmp.hashCode(), i);
				}
				else {
					isSkipPath[i] = true;
					initialSize --;
				}
		}
		WriteIntoDataBus writer = new WriteIntoDataBus(output.toString());
		LOG.info("The output path is " + output.toString());
		String originKey = null;
		//KVPairIntList curList = null;
		KVPairIntList.Builder builder = null;
		while (true) {
			if (initialSize == 0) {
				break;
			}
			KVPairIntListObject tmpObject = (KVPairIntListObject)pop();
			KVPairIntList tmp = tmpObject.getKVPairIntList();
			//System.out.println("while: pop()"+ tmp.toString());
			if (tmp == null) {
				break;
			}
			if(null != originKey) {
				if (originKey.equals(tmp.getKey())) {
					builder.addAllVlist(tmp.getVlistList());
				}
				else {
					if (this.combinerInstance != null) {
						long start = System.currentTimeMillis();
						List combineVlist = this.combinerInstance.combine(originKey,builder.getVlistList());
						LOG.debug("before combiner:" + builder.toString());
						builder.clearVlist();
						builder.addAllVlist(combineVlist);
						LOG.debug("after combiner:" + builder.toString());
						combineUsedTime += System.currentTimeMillis() - start;
					}
					writer.writeKVPairIntList(builder.build());
					builder = KVPairIntList.newBuilder();
					originKey = tmp.getKey();
					builder.setKey(originKey).addAllVlist(tmp.getVlistList());
				}
			}
			else {
				
				originKey =  tmp.getKey();
				builder = KVPairIntList.newBuilder();
				builder.setKey(originKey).addAllVlist(tmp.getVlistList());
			}
				
			int i = tmpObject.getFileIndex();
			tmp = reader[i].readKVPairIntList();
			if (null != tmp) {
					insert(new KVPairIntListObject(tmp,i));	
			}
			else {
				if (!isSkipPath[i]) {
					initialSize --;
					isSkipPath[i] = true;
				}
			}
		}
		/*ensure that the last curList is writen to file*/
		if (builder.isInitialized())
		     writer.writeKVPairIntList(builder.build());
		clear();
		writer.close();
		for (int i = 0; i < length; i++) {
			reader[i].close();
		}
		if (isDelete) {
			for (int i = 0; i < length; i++) {
				inputFile[i].delete();
			}
		}
	}
	private void mergeKVPairIntParOnMsgPool(Path[] input, Path output, boolean isDelete) {
		int length = input.length;
		ReadFromDataBus[] reader = new ReadFromDataBus[length];
		File[] inputFile = new File[length];
		boolean[] isSkipPath = new boolean[length];
		int initialSize = length;
		for (int i = 0; i < length; i++) {
			String path = input[i].toString();
			inputFile[i] = new File(path);
			reader[i] = new ReadFromDataBus(path);
			
			/*eliminate the file without data.*/
			if (inputFile[i].length() >0) {
				isSkipPath[i] = false;
			}
			else {
				isSkipPath[i] = true;
				initialSize --;
			}
		}
		
		/*handle special situation: each file has no data.*/
		if (initialSize <= 0) {
			return;
		}
		
		/*initialize the heap size*/
		initialize(initialSize);
		
		/*searchPathIndex is used to store the KVList in the Priority Queue and correlated index of input stream. 
		 */
		
		
		//Map<KVPairIntList, Integer> searchPathIndex = new WeakHashMap<KVPairIntList,Integer>(initialSize);
		//Map<Integer, Integer> searchPathIndex = new WeakHashMap<Integer,Integer>(initialSize);
		/*initialize the heap with first record from every file.*/
		for (int i = 0; i < length && !isSkipPath[i]; i++) {
				KVPairIntPar tmp = reader[i].readKVPairIntPar();
				System.out.println("index=" + i +  "hashcode:" + tmp.hashCode());
				if(tmp != null) {
					
					insert(new KVPairIntParObject(tmp,i));
					//searchPathIndex.put(tmp.hashCode(), i);
				}
				else {
					isSkipPath[i] = true;
					initialSize --;
				}
		}
		WriteIntoDataBus writer = new WriteIntoDataBus(output.toString());
		System.out.println(output.toString());
		String originKey = null;
		//KVPairIntList curList = null;
		KVPairIntList.Builder builder = null;
		while (true) {
			if (initialSize == 0) {
				break;
			}
			KVPairIntParObject tmpObject = (KVPairIntParObject)pop();
			KVPairIntPar tmp = tmpObject.getKVPairIntPar();
			//System.out.println("while: pop()"+ tmp.toString());
			if (tmp == null) {
				break;
			}
			if(null != originKey) {
				if (originKey.equals(tmp.getKey())) {
					builder.addVlist(tmp.getValue());
				}
				else {
					writer.writeKVPairIntList(builder.build());
					builder = KVPairIntList.newBuilder();
					originKey = tmp.getKey();
					builder.setKey(originKey).addVlist(tmp.getValue());
				}
			}
			else {
				originKey =  tmp.getKey();
				builder = KVPairIntList.newBuilder();
				builder.setKey(originKey).addVlist(tmp.getValue());
			}
				
			int i = tmpObject.getFileIndex();
			tmp = reader[i].readKVPairIntPar();
			if (null != tmp) {
					insert(new KVPairIntParObject(tmp,i));	
			}
			else {
				if (!isSkipPath[i]) {
					initialSize --;
					isSkipPath[i] = true;
				}
			}
		}
		/*ensure that the last curList is writen to file*/
		if (builder.isInitialized())
		     writer.writeKVPairIntList(builder.build());
		clear();
		writer.close();
		for (int i = 0; i < length; i++) {
			reader[i].close();
		}
		if (isDelete) {
			MessageClientChannel mcc = new MessageClientChannel();
			// take priority to make room for insert new data.
			for (int i = 0; i < length; i++) {
				mcc.FreeAllData(input[i].toString());
			}
		}
	}
	private <K extends Object, V extends Object> void mergeKVPairIntListOnMsgPool(
			Path[] input, Path output, boolean isDelete)  {

		int length = input.length;
		ReadFromDataBus[] reader = new ReadFromDataBus[length];
		boolean[] isSkipPath = new boolean[length];
		int initialSize = length;
		for (int i = 0; i < length; i++) {
			String path = input[i].toString();
			reader[i] = new ReadFromDataBus(path);
			isSkipPath[i] = false;

		}

		/* handle special situation: each file has no data. */
		if (initialSize <= 0) {
			return;
		}

		/* initialize the heap size */
		initialize(initialSize);

		/*
		 * searchPathIndex is used to store the KVList in the Priority Queue and
		 * correlated index of input stream.
		 */
		// Map<KVList, Integer> searchPathIndex = new
		// WeakHashMap<KVList,Integer>(initialSize);

		for (int i = 0; i < length && !isSkipPath[i]; i++) {
			KVPairIntList tmp = reader[i].getOneKVPairIntList();
			// System.out.println("index=" + i + "hashcode:" + tmp.hashCode());
			if (tmp != null) {

				insert(new KVPairIntListObject(tmp, i));
				// searchPathIndex.put(tmp.hashCode(), i);
			} else {
				isSkipPath[i] = true;
				initialSize--;
			}
		}
		WriteIntoDataBus writer = new WriteIntoDataBus(output.toString());
		String originKey = null;
		// KVPairIntList curList = null;
		KVPairIntList.Builder builder = null;
		while (true) {
			if (initialSize == 0) {
				break;
			}
			KVPairIntListObject tmpObject = (KVPairIntListObject) pop();
			KVPairIntList tmp = tmpObject.getKVPairIntList();
			// System.out.println("while: pop()"+ tmp.toString());
			if (tmp == null) {
				break;
			}
			if (null != originKey) {
				if (originKey.equals(tmp.getKey())) {
					builder.addAllVlist(tmp.getVlistList());
				} else {
					writer.appendKVPairIntList(builder.build());
					builder = KVPairIntList.newBuilder();
					originKey = tmp.getKey();
					builder.setKey(originKey).addAllVlist(tmp.getVlistList());
				}
			} else {
				originKey = tmp.getKey();
				builder = KVPairIntList.newBuilder();
				builder.setKey(originKey).addAllVlist(tmp.getVlistList());
			}

			int i = tmpObject.getFileIndex();
			tmp = reader[i].getOneKVPairIntList();
			if (null != tmp) {
				insert(new KVPairIntListObject(tmp, i));
			} else {
				if (!isSkipPath[i]) {
					initialSize--;
					isSkipPath[i] = true;
				}
			}
		}
		/* ensure that the last curList is writen to MessagePool */
		if (builder.isInitialized())
			writer.writeKVPairIntList(builder.build());
		clear();
		writer.close();
		for (int i = 0; i < length; i++) {
			reader[i].close();
		}
		if (isDelete) {
			MessageClientChannel mcc = new MessageClientChannel();
			// take priority to make room for insert new data.
			for (int i = 0; i < length; i++) {
				mcc.FreeAllData(input[i].toString());
			}
		}
	}

	public <K extends Object, V extends Object> 
		void merge(String[] input, String output, boolean isDelete, DataState state, Class objectClass) throws FileNotFoundException, IOException {
		Path[] inputPath = new Path[input.length];
		for (int i = 0; i < input.length; i++) {
			inputPath[i] = new Path(input[i]);
		}
		Path  outputPath = new Path(output);
		merge(inputPath, outputPath, isDelete, state, objectClass); 
	}
	
	public <K extends Object, V extends Object> 
		void merge(String[] input, String[] index, String[] output, boolean isDelete, DataState state) throws IOException {
	Path[] inputPath = new Path[input.length];
	for (int i = 0; i < input.length; i++) {
		inputPath[i] = new Path(input[i]);
	}
	Path[]  outputPath = new Path[output.length];
	for (int i = 0; i < output.length; i++) {
		outputPath[i] = new Path(output[i]);
	}
	merge(inputPath, index, outputPath, isDelete, state); 
}
	
	/**
	 * Merge the intermediate file that map() generate.The function will generate the array of output path whose 
	 * length equals the number of reduce tasks.  
	 * @param keyClass: specify the class of Key
	 * @param valueClass: specify the class of Value
	 * @param input: the input path of file.
	 * @param index: every String uses the format "4,5,7,...", in order to represent the different part of the input file,
	 * 					and each part represents data that reduce task needs to handle accordingly.    
	 * @param output: generate final file that Map phase generate.
	 * @param isDelete: whether or not to delete the file as the map phase ends.
	 * 
	 * @throws IOException
	 */
	private  <K extends Object, V extends Object> 
		void merge(Class<K> keyClass, Class<V> valueClass, Path[] input, String[] index, 
				Path[] output, boolean isDelete) throws FileNotFoundException, IOException{
		int length = input.length;
		//FileInputStream[] in = new FileInputStream[length];
		ObjectInputStream[] ois = new ObjectInputStream[length];
		File[] inputFile = new File[length];
		int [][] readCount = new int[input.length][output.length];
		
		/*store KVPairs from each input */
		//KVPair<K, V> [] curPair = new KVPair[length];
		
		/*store the index position that KVPair go through.*/
		//int [] curIndex = new int[length];
		
		/*get the record number of each part in every input path.
		 * and initialize the value of correlative variables*/
		for (int i = 0; i < length; i++) {
			inputFile[i] = new File(input[i].toString());
			ois[i] = new ObjectInputStream( 
					new FileInputStream(inputFile[i]));
			String[] tmp = index[i].split(",");
			for (int j = 0; j < output.length; j++) {
				readCount[i][j] = Integer.parseInt(tmp[j]);
			}
		}
		
		/*record that the number of each part whose records' number equals to 0. 
		 * In the situation, set the value of isSkipPath[i] to true*/
		int skipPathNum = 0;
		boolean [] isSkipPath = new boolean[length];
		
		int allocateNum = 0;
		for (int k = 0 ; k < output.length; k++) {
			skipPathNum = 0;
			allocateNum = 0;
			for (int i = 0; i < length; i++) {
				allocateNum += readCount[i][k]; 
				if (readCount[i][k] == 0) {
					skipPathNum ++;
					isSkipPath[i] = true;
				}
				else {
					isSkipPath[i] = false;
				}
			}
			initialize(allocateNum);
			for (int i = 0; (i < length) && (!isSkipPath[i]); i++) {
				for (int j = 0; j < readCount[i][k]; j++)
					try {
						insert((KVPair)(ois[i].readObject()));
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
			ByteArrayOutputStream bout = new ByteArrayOutputStream();   
			ObjectOutputStream oos = new ObjectOutputStream(bout);
			FileOutputStream fout = new FileOutputStream(output[k].toString());
			
			for (int i = 0; i < allocateNum; i++) {
				oos.writeObject(pop());
			}
			oos.flush();
			fout.write(bout.toByteArray());
			fout.close();
			oos.close();
			bout.close();
			/*clear the Priority Queue*/
			clear();
		}
		for (ObjectInputStream oisTmp: ois) {
			oisTmp.close();
			
		}
		if (isDelete) {
			for (int i = 0; i < length; i++) {
				inputFile[i].delete();
			}
		}
	}
	/**
	 * In order to avoid confusing hashtable, that one KVPairIntList Object equals another one.
	 * Add the information of file index. 
	 * @author jiangbing
	 *
	 */
	class KVPairIntListObject {
		final KVPairIntList list;
		final int fileIndex;
		KVPairIntListObject(KVPairIntList list, int fileIndex) {
			this.list = list;
			this.fileIndex = fileIndex;
		}
		KVPairIntList getKVPairIntList() {
			return this.list;
		}
		int getFileIndex() {
			return this.fileIndex;
		}
	}
	class KVPairIntParObject {
		final KVPairIntPar value;
		final int fileIndex;
		KVPairIntParObject(KVPairIntPar value, int fileIndex) {
			this.value = value;
			this.fileIndex = fileIndex;
		}
		KVPairIntPar getKVPairIntPar() {
			return this.value;
		}
		int getFileIndex() {
			return this.fileIndex;
		}
	}
	@Override
	protected boolean lessThan(Object a, Object b) {
		// TODO Auto-generated method stub
		//KVPair a1 = (KVPair)a;
		//KVPair b1 = (KVPair)b;
		
		if (a.getClass() == KVList.class) {
			KVList a1 = (KVList)a;
			KVList b1 = (KVList)b;
			int compare;
			if (a1.getKey() instanceof String) {
				compare = (a1.getKey().toString()).compareTo
					(b1.getKey().toString());
				if (compare < 0) {
					return true;
				}
			}
			else if (a1.getKey() instanceof Integer){
				if ((Integer)a1.getKey() < (Integer)b1.getKey()) {
					return true;
				}
			}
			return false;
		}
		
		else if (a.getClass() == KVPair.class) {
			KVPair a1 = (KVPair)a;
			KVPair b1 = (KVPair)b;
			int compare;
			if (a1.getKey() instanceof String) {
				compare = (a1.getKey().toString()).compareTo
					(b1.getKey().toString());
				if (compare < 0) {
					return true;
				}
			}
			else if (a1.getKey() instanceof Integer){
				if ((Integer)a1.getKey() < (Integer)b1.getKey()) {
					return true;
				}
			}
			return false;
		}
		
		else if (a.getClass() == KVPairIntPar.class) {
			KVPairIntPar a1 = (KVPairIntPar)a;
			KVPairIntPar b1 = (KVPairIntPar)b;
			int compare;
			if (a1.getKey() instanceof String) {
				compare = (a1.getKey().toString()).compareTo
					(b1.getKey().toString());
				if (compare < 0) {
					return true;
				}
			}
			return false;
		}
		else if (a.getClass() == KVPairIntList.class) {
			KVPairIntList a1 = (KVPairIntList)a;
			KVPairIntList b1 = (KVPairIntList)b;
			int compare;
			if (a1.getKey() instanceof String) {
				compare = (a1.getKey().toString()).compareTo
					(b1.getKey().toString());
				if (compare < 0) {
					return true;
				}
			}
			return false;
		}
		else if (a.getClass() == KVPairIntListObject.class) {
			KVPairIntList a1 = (KVPairIntList)((KVPairIntListObject) a).getKVPairIntList();
			KVPairIntList b1 = (KVPairIntList)((KVPairIntListObject) b).getKVPairIntList();
			int compare;
			if (a1.getKey() instanceof String) {
				compare = (a1.getKey().toString()).compareTo
					(b1.getKey().toString());
				if (compare < 0) {
					return true;
				}
			}
			return false;
		}
		else if (a.getClass() == KVPairIntParObject.class) {
			KVPairIntPar a1 = (KVPairIntPar)((KVPairIntParObject) a).getKVPairIntPar();
			KVPairIntPar b1 = (KVPairIntPar)((KVPairIntParObject) b).getKVPairIntPar();
			int compare;
			if (a1.getKey() instanceof String) {
				compare = (a1.getKey().toString()).compareTo
					(b1.getKey().toString());
				if (compare < 0) {
					return true;
				}
			}
			return false;
		}
		return false;
	}
}
