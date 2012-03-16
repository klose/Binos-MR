package cn.ict.cacuts.mapreduce;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import cn.ict.binos.transmit.MessageClientChannel;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairInt;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntList;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntPar;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntParData;

import com.longyi.databus.clientapi.ChannelOutputStream;
import com.transformer.compiler.DataState;
import com.transformer.compiler.JobConfiguration;
/*if the file or path exists, it means that appends.*/
public class WriteIntoDataBus {

	public final String dataName;
	public DataState dataState;
	private ByteArrayOutputStream baos;
	private ObjectOutputStream oos;
	private FileOutputStream fout;
	private MessageClientChannel mcc;
	private ChannelOutputStream cos = null;
	public WriteIntoDataBus(String dataName) {
		this.dataName = dataName;
		confirmDataType();
		baos = new ByteArrayOutputStream(); 
		try {
			//oos = new ObjectOutputStream(bout);
			if (dataState == DataState.MESSAGE_POOL) {
				mcc = new MessageClientChannel();
			}
			else if (dataState == DataState.LOCAL_FILE) {
				fout = new FileOutputStream(dataName, true);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//dataName has its own data transmit type.
	private void confirmDataType() {
		
		if (dataName.matches(JobConfiguration.getMsgHeader() + ".*")) {
			dataState = DataState.MESSAGE_POOL;
		}
		else {
			dataState = DataState.LOCAL_FILE;
		}
		//...please add another data type here.
	}

	public String getdataName() {
		return this.dataName;
	}
	/**
	 * write data into data bus.
	 * @param pairs
	 */
	/*public void executeWrite(List pairs) {
		if (this.dataState == DataState.LOCAL_FILE) {
			writeIntoFile(pairs, this.dataName);
		}
		else if (this.dataState == DataState.MESSAGE_POOL) {
			writeIntoMsgPool(pairs, this.dataName);
		}
	}*/
	/**
	 * record the section of array. Not contains the endIndex element in the array.
	 * @param values
	 * @param beginIndex : the start index of the section in the array
	 * @param endIndex : the end index of the section in the array.
	 */
	public void executeWrite(KVPairIntPar[] values, int beginIndex, int endIndex) {
		if (this.dataState == DataState.LOCAL_FILE) {
			writeKVPairIntParArray(values, beginIndex, endIndex);
		}
		else if (this.dataState == DataState.MESSAGE_POOL) {
			putKVPairIntParArray(values, beginIndex, endIndex);
		}
	}
	
	public void executeWrite(KVPairIntPar[] values) {
		if (this.dataState == DataState.LOCAL_FILE) {
			writeKVPairIntParArray(values);
		}
		else if (this.dataState == DataState.MESSAGE_POOL) {
			putKVPairIntParArray(values);
		}
	}
	public void appendKVPairIntList(KVPairIntList value) {
		if (cos == null) {
			cos = new ChannelOutputStream(this.dataName);
		}
		cos.write(value.toByteArray());
	}
	public void appendKVPairIntPar(KVPairIntPar value){
		if (cos == null) {
			cos = new ChannelOutputStream(this.dataName);
		}
		cos.write(value.toByteArray());
	}
	public void writeKVPairIntList(KVPairIntList value) {
		try {
			//System.out.println(value.toString());
			value.writeDelimitedTo(fout);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void writeKVPairIntListArray(List<KVPairIntList> values) {
		try {
			for (KVPairIntList tmp: values) {
				tmp.writeDelimitedTo(baos);
			}
			baos.writeTo(fout);
			baos.reset();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	private void putKVPairIntArray(KVPairInt[] values) {
		
		try {
			for (KVPairInt tmp: values) {
					tmp.writeDelimitedTo(baos);
			}
			mcc.putValue(this.dataName, baos.toByteArray());
			baos.reset();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void writeKVPairIntArray(KVPairInt[] value) {
		try {
			for (KVPairInt tmp: value) {
				tmp.writeDelimitedTo(baos);
			}
			baos.writeTo(fout);
			baos.reset();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void putKVPairIntParArray(KVPairIntPar[] values) {
		for (KVPairIntPar tmp : values) {
			appendKVPairIntPar(tmp);
		}
	}
	private void putKVPairIntParArray(KVPairIntPar[] values, int begin, int end) {
		for (int i = begin; i < end; i++) {
			appendKVPairIntPar(values[i]);
		}
	}
	private void writeKVPairIntParArray(KVPairIntPar[] values) {
		try {						
			for (KVPairIntPar tmp: values) {
				tmp.writeDelimitedTo(baos);
			}
			baos.writeTo(fout);
			baos.reset();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private void writeKVPairIntParArray(KVPairIntPar[] values, int begin, int end) {
		try {
			for (int i = begin; i < end; i++) {
				values[i].writeDelimitedTo(baos);
			}
			baos.writeTo(fout);
			baos.reset();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private void writeIntoMsgPool(Object[] values, String dataName) {	
		ObjectOutputStream oos;		
		try {
			oos = new ObjectOutputStream(baos);
			for (int i = 0; i < values.length; i++) {
				oos.writeObject(values[i]);
			}
			oos.flush();
			mcc.putValue(dataName, baos.toByteArray());
			baos.reset();
			System.out.println("kkkk :writeIntoMsgPool" + dataName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	private void writeIntoMsgPool(List pairs, String dataName) {	
		try {
			for (int i = 0; i < pairs.size(); i++) {
				oos.writeObject(pairs.get(i));
			}
			oos.flush();
			mcc.putValue(dataName, baos.toByteArray());
			System.out.println("kkkk :writeIntoMsgPool" + dataName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
			
	}
	private void writeIntoFile(List pairs, String dataName) {
		try {
			for (int i = 0; i < pairs.size(); i++) {
				oos.writeObject(pairs.get(i));
			}
			oos.flush();					
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void close() {
		if (this.dataState == DataState.LOCAL_FILE) {
			try {
				fout.flush();
				fout.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("kkkk :writeIntoFile" + dataName);
		}
		else if (this.dataState == DataState.MESSAGE_POOL ){
			if (this.cos != null) {
				this.cos.close();
			}
		}
		try {
			baos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	@SuppressWarnings({ "unchecked", "rawtypes", "unused" })
	public static void main(String[] args) {
		String[] keys = { "key1", "key2", "key3", "key4", "key5", "key6" };
		ArrayList pairs = new ArrayList();
		pairs.add("key1 , 1");
		pairs.add("key2 , 2");
		String path =  "/tmp/2012001-test";
		WriteIntoDataBus tt = new WriteIntoDataBus(path);
		//tt.executeWrite(pairs);
		tt.close();
//		WriteIntoDataBus tt1 = new WriteIntoDataBus(path);
//		tt1.executeWrite(pairs);
//		// tt.writeIntoFile(pairs , "test");
//		tt.setdataName("XXX");
//		tt.writeIntoFile(pairs);
	}
}
