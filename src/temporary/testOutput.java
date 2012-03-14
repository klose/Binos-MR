package temporary;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import cn.ict.cacuts.mapreduce.KeyValue.KVPairInt;


public class testOutput<KEY, VALUE> {
	public static void main(String[] args) throws IOException {
		try {
			FileInputStream fis = new FileInputStream("/home/jiangbing/output1");
			FileWriter writer = new FileWriter("/home/jiangbing/outputtxt");
			KVPairInt tmp = KVPairInt.parseDelimitedFrom(fis);
			while (tmp != null) {
				writer.write(tmp.getKey() + " " + tmp.getValue() + "\n");
				tmp = KVPairInt.parseDelimitedFrom(fis);
			}
			writer.flush();
			writer.close();
			fis.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
