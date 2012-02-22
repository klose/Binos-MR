package cn.ict.cacuts.mapreduce.mapcontext;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import cn.ict.binos.transmit.MessageClientChannel;

import com.transformer.compiler.DataState;
import com.transformer.compiler.JobConfiguration;
import com.transformer.compiler.TransmitType;

public class WriteIntoDataBus {

	public final String dataName;
	public DataState dataState;
	
	
	public WriteIntoDataBus(String dataName) {
		this.dataName = dataName;
		confirmDataType();
	}
	
	//dataName has its own data transmit type.
	public void confirmDataType() {
		
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
	public void executeWrite(List pairs) {
		if (this.dataState == DataState.LOCAL_FILE) {
			writeIntoFile(pairs, this.dataName);
		}
		else if (this.dataState == DataState.MESSAGE_POOL) {
			writeIntoMsgPool(pairs, this.dataName);
		}
	}
	private void writeIntoMsgPool(List pairs, String dataName) {
		MessageClientChannel mcc = new MessageClientChannel();
		ByteArrayOutputStream bout = new ByteArrayOutputStream(); 
		ObjectOutputStream oos;		
		try {
			oos = new ObjectOutputStream(bout);

			for (int i = 0; i < pairs.size(); i++) {
				oos.writeObject(pairs.get(i));
			}
			oos.flush();
			mcc.putValue(dataName, bout.toByteArray());
			bout.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
			
	}
	private void writeIntoFile(List pairs, String dataName) {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();   
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(bout);
			FileOutputStream fout = new FileOutputStream(dataName, true);
			for (int i = 0; i < pairs.size(); i++) {
				oos.writeObject(pairs.get(i));
			}
			oos.flush();			
			fout.write(bout.toByteArray());	
//			fout.write(-1);
//			fout.write(-1);
			fout.flush();
			bout.close();
			oos.close();
			fout.close();
		} catch (IOException e) {
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
		String path = JobConfiguration.getMsgHeader() + "2012001-test";
		WriteIntoDataBus tt = new WriteIntoDataBus(path);
		tt.executeWrite(pairs);
//		// tt.writeIntoFile(pairs , "test");
//		tt.setdataName("XXX");
//		tt.writeIntoFile(pairs);
	}
}
