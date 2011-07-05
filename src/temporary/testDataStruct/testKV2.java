package temporary.testDataStruct;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class testKV2 {
	private final static long recordNum = 100000l;
	private final static int capacity = 1024*1024;
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();   
		ObjectOutputStream oos = new ObjectOutputStream(bout);
		FileOutputStream fout = new FileOutputStream("/tmp/kvtest2");
		// 获取输入输出通道  
		//FileChannel fcin = fin.getChannel();  
		//FileChannel fcout = fout.getChannel();
		//ByteBuffer bb = ByteBuffer.allocateDirect(capacity);
		long time = System.currentTimeMillis();
		long writeTime,readTime;
		System.out.println("Start Write");
		for (long i = 0; i < recordNum; i++) {
			KVPair pair = new KVPair("abcdef", 1, 1);
			oos.writeObject(pair);
		}
		oos.flush();
//		byte[] tmp = bout.toByteArray();
//		int offset = 0, length = capacity; 
//		
//		while (length == capacity) {
//			bb.clear();
//			if (offset + length < tmp.length) {
//				bb.put(tmp, offset, length);
//			}else {
//				length = tmp.length - offset;
//				bb.put(tmp, offset, length);
//			}
//			bb.flip();
//			fcout.write(bb);
//			fcout.force(true);
//			offset += length;
//		}
	
		
		fout.write(bout.toByteArray());		
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
		File file = new File("/tmp/kvtest2");
		//（"abcded", 1,1） 有效数据 String要包含"\0",为 （6+1）+4+4 
		System.out.printf("usefulness ratio:%.2f", (double)(7+4+4)*recordNum/file.length());
	}
}
