package temporary;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import cn.ict.cacuts.mapreduce.reduce.FinalKVPair;

public class TestReadObject {
	private static void writeIntoFile(List pairs, String dataName) {
		
		ByteArrayOutputStream bout = new ByteArrayOutputStream();   
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(bout);
			FileOutputStream fout = new FileOutputStream(dataName, false);
			
			for (int i = 0; i < pairs.size(); i++) {
				oos.writeObject(pairs.get(i));
			}
			
			
		
            
			oos.flush();
			oos.writeObject("xxxxxxxxxxxx");
			oos.writeObject("yyyyyyyyyyyy");
			
			//fout.write(bout.toByteArray());	
			oos.writeObject("xxxxxxxxxxxx");
			oos.writeObject("yyyyyyyyyyyy");
			fout.write(bout.toByteArray());	
//			fout.write(-1);
//			fout.write(-1);
			fout.flush();
			//fout.getFD().sync();
			
			bout.close();
			oos.close();
			fout.close();
			System.out.println("kkkk :writeIntoFile" + dataName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException {
//		// TODO Auto-generated method stub
		String dataName = "/tmp/test_binos";
		ArrayList pairs = new ArrayList();
		pairs.add("abc");
		pairs.add("xyz");
		pairs.add("wux");
		pairs.add("www");
		writeIntoFile(pairs, dataName);
//		writeIntoFile(pairs, dataName);writeIntoFile(pairs, dataName);writeIntoFile(pairs, dataName);
		
		FileInputStream fis = new FileInputStream("/tmp/test_binos");
		ObjectInputStream ois = new ObjectInputStream(fis);
		String tmp = null;
		int i = 0;
		
		while (null != (tmp=(String) ois.readObject())) {
			
			System.out.println(tmp + ":" + (++i));
		}
	}

}
