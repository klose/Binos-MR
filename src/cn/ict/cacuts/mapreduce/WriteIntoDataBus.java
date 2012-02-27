package cn.ict.cacuts.mapreduce;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import cn.ict.binos.transmit.MessageClientChannel;
import com.transformer.compiler.DataState;
import com.transformer.compiler.JobConfiguration;
/*if the file or path exists, it means that appends.*/
public class WriteIntoDataBus {

	public final String dataName;
	public DataState dataState;
	private ByteArrayOutputStream bout;
	private ObjectOutputStream oos;
	private FileOutputStream fout;
	private MessageClientChannel mcc;
	public WriteIntoDataBus(String dataName) {
		this.dataName = dataName;
		confirmDataType();
		bout = new ByteArrayOutputStream(); 
		try {
			oos = new ObjectOutputStream(bout);
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
	public void executeWrite(List pairs) {
		if (this.dataState == DataState.LOCAL_FILE) {
			writeIntoFile(pairs, this.dataName);
		}
		else if (this.dataState == DataState.MESSAGE_POOL) {
			writeIntoMsgPool(pairs, this.dataName);
		}
	}
	public void executeWrite(Object[] values) {
		if (this.dataState == DataState.LOCAL_FILE) {
			writeIntoFile(values, this.dataName);
		}
		else if (this.dataState == DataState.MESSAGE_POOL) {
			writeIntoMsgPool(values, this.dataName);
		}
	}
	
	private void writeIntoFile(Object[] values, String dataName) {
		try {
			for (int i = 0; i < values.length; i++) {
				oos.writeObject(values[i]);
			}
			oos.flush();			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private void writeIntoMsgPool(Object[] values, String dataName) {
		
		ByteArrayOutputStream bout = new ByteArrayOutputStream(); 
		ObjectOutputStream oos;		
		try {
			oos = new ObjectOutputStream(bout);
			for (int i = 0; i < values.length; i++) {
				oos.writeObject(values[i]);
			}
			oos.flush();
			mcc.putValue(dataName, bout.toByteArray());
			bout.close();
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
			mcc.putValue(dataName, bout.toByteArray());
			bout.close();
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
	public void executeClose() {
		if (this.dataState == DataState.LOCAL_FILE) {
			try {
				fout.write(bout.toByteArray());
				fout.flush();
				bout.close();
				oos.close();
				fout.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("kkkk :writeIntoFile" + dataName);
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
		tt.executeWrite(pairs);
		tt.executeClose();
//		WriteIntoDataBus tt1 = new WriteIntoDataBus(path);
//		tt1.executeWrite(pairs);
//		// tt.writeIntoFile(pairs , "test");
//		tt.setdataName("XXX");
//		tt.writeIntoFile(pairs);
	}
}
