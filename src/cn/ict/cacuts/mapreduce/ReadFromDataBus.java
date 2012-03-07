package cn.ict.cacuts.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import cn.ict.binos.transmit.MessageClientChannel;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntList;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntPar;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntParData;

import com.transformer.compiler.DataState;
import com.transformer.compiler.JobConfiguration;

public class ReadFromDataBus {
	public final String dataName;
	public DataState dataState;
	private ByteArrayOutputStream bout;
	private ObjectOutputStream oos;
	private FileInputStream fin;
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
		try {
			this.fin.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
