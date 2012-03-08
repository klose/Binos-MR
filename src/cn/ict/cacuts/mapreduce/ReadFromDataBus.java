package cn.ict.cacuts.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;

import cn.ict.binos.transmit.MessageClientChannel;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntList;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntPar;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntParData;

import com.transformer.compiler.DataState;
import com.transformer.compiler.JobConfiguration;

public class ReadFromDataBus {
	public final String dataName;
	public DataState dataState;
	private ByteArrayInputStream bais = null;
	private FileInputStream fin;
	private int readCount = 0;
	private List<byte[]> readData = null;
	private MessageClientChannel mcc;
	public ReadFromDataBus(String dataName) {
		this.dataName = dataName;
		confirmDataType();
		try {
			//oos = new ObjectOutputStream(bout);
			if (dataState == DataState.MESSAGE_POOL) {
				mcc = new MessageClientChannel();
			}
			else if (dataState == DataState.LOCAL_FILE) {
				fin = new FileInputStream(dataName);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void confirmDataType() {
		if (dataName.matches(JobConfiguration.getMsgHeader() + ".*")) {
			dataState = DataState.MESSAGE_POOL;
		}
		else{
			dataState = DataState.LOCAL_FILE;
		}
	}
	public KVPairIntPar getOneKVPairIntPar() {
//		if (this.bais == null) {
//			this.bais = new ByteArrayInputStream(mcc.fetchAllData(this.dataName));
//		}
		if (this.readData == null) {
			this.readData = mcc.fetchAllData(this.dataName);
		}
		if (this.readCount < this.readData.size()) {
			try {
				return KVPairIntPar.parseFrom(this.readData.get(readCount++));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/*KVPairIntPar.Builder builder = KVPairIntPar.newBuilder();
		try {	
			builder.mergeDelimitedFrom(this.bais);
			if (builder.isInitialized()) {
				return builder.build();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		return null; 
	}
	public KVPairIntList getOneKVPairIntList() {
		if (this.readData == null) {
			this.readData =  mcc.fetchAllData(this.dataName);
		}
		if (this.readCount < this.readData.size()) {
			try {
				return KVPairIntList.parseFrom(this.readData.get(readCount++));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null; 
	}
	public KVPairIntPar readKVPairIntPar() {
		KVPairIntPar.Builder builder = KVPairIntPar.newBuilder();
		try {
				builder.mergeDelimitedFrom(fin);
				if (builder.isInitialized()) {
					return builder.build();
				}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public KVPairIntList readKVPairIntList() {
		// TODO Auto-generated method stub
		KVPairIntList.Builder builder = KVPairIntList.newBuilder();
		try {
				builder.mergeDelimitedFrom(fin);
				if (builder.isInitialized()) {
					return builder.build();
				}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public void close() {
		if (this.dataState == DataState.LOCAL_FILE) {
			try {
				this.fin.close();
			} catch (IOException e) {
			// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (this.dataState == DataState.MESSAGE_POOL) {
			if (readData != null) {
				this.readData.clear();
			}
		}
	}
	public static void main(String[] args) {
		ReadFromDataBus reader = new ReadFromDataBus("/tmp/output1");
		KVPairIntList tmp;
		
		while ((tmp = reader.readKVPairIntList()) != null) {
			
			System.out.println(tmp.toString());
		}
		reader.close();
		//System.out.println(reader.readKVPairIntPar().toString());
	}

	
	
}
