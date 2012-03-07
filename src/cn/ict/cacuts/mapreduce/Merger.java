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
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.hadoop.fs.Path;

import com.transformer.compiler.DataState;

import cn.ict.binos.transmit.MessageClientChannel;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntList;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntPar;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntParData;
import cn.ict.cacuts.mapreduce.map.KVList;
import cn.ict.cacuts.mapreduce.map.KVPair;

/**
 * Merge small files into large file. 
 * @author Bing Jiang
 *
 */
public class Merger extends PriorityQueue{

	

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
		ObjectInputStream[] ois = new ObjectInputStream[length];
		ByteArrayInputStream[] bais = new ByteArrayInputStream[length];
		int [][] readCount = new int[input.length][output.length];
		MessageClientChannel mcc = new MessageClientChannel();
		for (int i = 0; i < length; i++) {
			String[] tmp = index[i].split(",");
			for (int j = 0; j < output.length; j++) {
				readCount[i][j] = Integer.parseInt(tmp[j]);
			}
		}
		for (int i = 0; i < length; i++) {
			bais[i] = new ByteArrayInputStream(mcc.getValue(input[i].toString()));
			ois[i] = new ObjectInputStream(bais[i]);
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
					try {
						insert((KVPair)(ois[i].readObject()));
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
			ByteArrayOutputStream bout = new ByteArrayOutputStream();   
			ObjectOutputStream oos = new ObjectOutputStream(bout);
			//FileOutputStream fout = new FileOutputStream(output[k].toUri().getPath());
			output[k].toString();
			//boolean isNewObject = true;
			K originKey = null;
			KVList list = null;
			for (int i = 0; i < allocateNum; i++) {
				KVPair<K, V> pair = (KVPair<K, V>) pop();
				K key = pair.getKey();
				if (!key.equals(originKey)) {
					//isNewObject = true;
					if (list != null) {
						oos.writeObject(list);
						list = null;
					}
					list = new KVList(pair);
				}
				else {
					//isNewObject = false;
					list.addVal(pair.getValue());
				}
				originKey = key;
			}
			if (list != null) {
				oos.writeObject(list);
				list = null;
			}
			oos.flush();
			mcc.putValue(output[k].toString(), bout.toByteArray());			
			oos.close();
			bout.close();
		}
		for (ObjectInputStream oisTmp: ois) {
			oisTmp.close();
		}
		if (isDelete) {
			for (int i = 0; i < length; i++) {
				mcc.FreeData(input[i].toString());
			}
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
			reader[i] = new ReadFromDataBus(input[i].toUri().getPath());
			
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
			for (int i = 0; i < length; i++) {
				if (isSkipPath[i]) {
					continue;
				}
				for (int j = 0; j < readCount[i][k]; j++)
					insert((reader[i].readKVPairIntPar()));
			}
			KVPairIntPar pair = (KVPairIntPar) pop();
			String originKey = pair.getKey();
			KVPairIntList.Builder builder = KVPairIntList.newBuilder();
			KVPairIntList list;
			builder.setKey(originKey).addVlist(pair.getValue());
			WriteIntoDataBus writer = new WriteIntoDataBus(output[k].toUri().getPath());
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
//						if (list.getKey().equals("while")) {
//							System.out.println("while occur:" + list.getVlistCount() + "times");
//						}
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
		}
		clear();
		if (isDelete) {
			for (int i = 0; i < length; i++) {
				new File(input[i].toUri().getPath()).delete();
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
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	
	public  <K extends Object, V extends Object> void merge(Path[] input, Path output, boolean isDelete, DataState state) throws FileNotFoundException, IOException {
		if (state == DataState.LOCAL_FILE) {
			mergeOnLocalFile(input, output, isDelete);
		}
		else if (state == DataState.MESSAGE_POOL){
			mergeOnMsgPool(input,output,isDelete);
		}
	}
	private void mergeOnLocalFile(Path[] input, Path output, boolean isDelete) throws FileNotFoundException, IOException {
		int length = input.length;
		ReadFromDataBus[] reader = new ReadFromDataBus[length];
		
//		ObjectInputStream[] ois = new ObjectInputStream[length];
//		FileInputStream[] fis = new FileInputStream[length];
		File[] inputFile = new File[length];
		boolean[] isSkipPath = new boolean[length];
		int initialSize = length;
		for (int i = 0; i < length; i++) {
			String path = input[i].toUri().getPath();
			inputFile[i] = new File(path);
//			fis[i] = new FileInputStream(path);
//			ois[i] = new ObjectInputStream(fis[i]);
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
				System.out.println("index=" + i +  "hashcode:" + tmp.hashCode());
				if(tmp != null) {
					
					insert(new KVPairIntListObject(tmp,i));
					//searchPathIndex.put(tmp.hashCode(), i);
				}
				else {
					isSkipPath[i] = true;
					initialSize --;
				}
		}
		WriteIntoDataBus writer = new WriteIntoDataBus(output.toUri().getPath());
		System.out.println(output.toUri().getPath());
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
	
	private <K extends Object, V extends Object>
		void mergeOnMsgPool(Path[] input, Path output, boolean isDelete) throws IOException {
		int length = input.length;
		ByteArrayInputStream[] bais = new ByteArrayInputStream[length];
		ObjectInputStream[] ois = new ObjectInputStream[length];
		MessageClientChannel mcc = new MessageClientChannel();
		boolean[] isSkipPath = new boolean[length];
		int initialSize = length;
		for (int i = 0; i < length; i++) {
			String path = input[i].toString();			
			byte[] data = mcc.getValue(path);
			bais[i] = new ByteArrayInputStream(data);
			ois[i] = new ObjectInputStream(bais[i]);	

			//System.out.println(ois[i].available());
			/*eliminate the file without data.*/
			if (data.length >0) {
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
		Map<KVList, Integer> searchPathIndex = new WeakHashMap<KVList,Integer>(initialSize);
		
		/*initialize the heap with first record from every file.*/
		for (int i = 0; i < length && !isSkipPath[i]; i++) {
			try {
				KVList tmp = (KVList)(ois[i].readObject());
				insert((KVList)tmp);
				searchPathIndex.put(tmp, i);
				if (bais[i].available() == 0) {
					isSkipPath[i] = true;
					initialSize --;
				}
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(
				baos);
		K originKey = null;
		KVList curList = null;
		int test_i = 0;
		while (true) {
			
			if (initialSize == 0) {
				break;
			}
			
			KVList tmp = (KVList)pop();
			
			if (curList == null) {
				curList = tmp;
				originKey = (K) curList.getKey();
			}
			else {
				if (originKey.equals(tmp.getKey())) {
					curList.appendVec(tmp.getValue());
				}
				else {
					
					oos.writeObject(curList);
					curList = tmp;
					originKey = (K)tmp.getKey();
				}
			}
			int i = searchPathIndex.get(tmp);
			
			searchPathIndex.remove(tmp);
			if (bais[i].available() <= 0) {
				if (!isSkipPath[i]) {
					initialSize --;
					isSkipPath[i] = true;
				}
			}
			else {
				try {
					tmp = (KVList)(ois[i].readObject());
					searchPathIndex.put(tmp, i);
					insert(tmp);
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		/*ensure that the last curList is writen to file*/
		if (curList != null)
			oos.writeObject(curList);
		
		KVList remainingRecTmp = null; 
		while ((remainingRecTmp = (KVList) pop()) != null) {
			oos.writeObject(remainingRecTmp);
		}
		searchPathIndex.clear();
		clear();		
		if (isDelete) {
			// take priority to make room for insert new data.
			for (int i = 0; i < length; i++) {
				mcc.FreeData(input[i].toString());
			}
		}
		oos.flush();
		mcc.putValue(output.toString(), baos.toByteArray());
		baos.close();
		oos.close();
		for (ObjectInputStream oisTmp: ois) {
			oisTmp.close();
		}
	}
	
	public <K extends Object, V extends Object> 
		void merge(String[] input, String output, boolean isDelete, DataState state) throws FileNotFoundException, IOException {
		Path[] inputPath = new Path[input.length];
		for (int i = 0; i < input.length; i++) {
			inputPath[i] = new Path(input[i]);
		}
		Path  outputPath = new Path(output);
		merge(inputPath, outputPath, isDelete, state); 
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
			inputFile[i] = new File(input[i].toUri().getPath());
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
			FileOutputStream fout = new FileOutputStream(output[k].toUri().getPath());
			
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
		return false;
	}
}
