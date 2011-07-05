package temporary.testDataStruct;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class testKV {
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		FileOutputStream fos = new FileOutputStream("/tmp/kvtest");
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		long recordNum = 100000l;
		long time = System.currentTimeMillis();
		long writeTime,readTime;
		System.out.println("Start Write");
		for (long i = 0; i < recordNum; i++) {
			KVPair<String, Integer> pair = new KVPair<String, Integer>("abcdef", 1, 1);
			oos.writeObject(pair);
		}
		oos.close();
		fos.close();
		writeTime = System.currentTimeMillis() - time;
		System.out.println("write 100000 kvpair used time:" + writeTime + "ms");
		System.out.println("Start Read");
		time = System.currentTimeMillis();
		FileInputStream fis = new FileInputStream("/tmp/kvtest");
		ObjectInputStream ois = new ObjectInputStream(fis);
		for (long j = 0; j < recordNum; j++) {
			KVPair pair = (KVPair) ois.readObject();
		}
		ois.close();
		fis.close();
		readTime = System.currentTimeMillis() - time;
		System.out.println("read 100000 kvpair used time:" + readTime + "ms");
		File file = new File("/tmp/kvtest");
		//（"abcded", 1,1） 有效数据 String要包含"\0",为 （6+1）+4+4 
		System.out.printf("usefulness ratio:%.2f", (double)(7+4+4)*recordNum/file.length());
	}
}
