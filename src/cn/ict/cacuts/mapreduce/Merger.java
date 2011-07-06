package cn.ict.cacuts.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import cn.ict.cacuts.mapreduce.mapcontext.KVPair;


/**
 * Merge small files into large file. 
 * @author jiangbing
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
	 * @throws IOException
	 */
	public  void merge(Path[] input, String[] index, Path[] output, boolean isDelete) throws IOException {
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
	public void merge(Path[] input, Path output, boolean isDelete) {
		
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
			inputFile[i] = new File(input[i].getName());
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
			FileOutputStream fout = new FileOutputStream(output[k].getName());
			for (int i = 0; i < allocateNum; i++) {
				oos.writeObject(pop());
			}
			oos.flush();
			fout.write(bout.toByteArray());
			fout.close();
			oos.close();
			bout.close();
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
	@Override
	protected boolean lessThan(Object a, Object b) {
		// TODO Auto-generated method stub
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
}
