package temporary.testDataStruct;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class testKV1 {
	private final static long recordNum = 100000l;
	public static void main(String[] args) throws IOException {
		DataOutputStream dos = new DataOutputStream(new FileOutputStream("/tmp/kvtest1"));
		long time = System.currentTimeMillis();
		long writeTime,readTime;
		System.out.println("Start Write");
		for (long i = 0; i < recordNum; i++) {
			Text.writeString((DataOutput) dos, "abcdef");
			IntWritable val = new IntWritable(1);
			val.write((DataOutput) dos);
			IntWritable partition = new IntWritable(1);
			partition.write((DataOutput) dos);
		}
		dos.close();
		writeTime = System.currentTimeMillis() - time;
		System.out.printf("write %d kvpair used time:" + writeTime + "ms\n", recordNum);
		System.out.println("Start Read");
		time = System.currentTimeMillis();
		DataInputStream dis = new  DataInputStream(new FileInputStream("/tmp/kvtest1"));
		for (long j = 0; j < recordNum; j++) {
			String k = Text.readString((DataInput) dis);
			int v = dis.read();
			int p = dis.read();
		}
		dis.close();
		readTime = System.currentTimeMillis() - time;
		System.out.printf("read %d kvpair used time:" + readTime + "ms\n", recordNum);
		File file = new File("/tmp/kvtest1");
		//（"abcded", 1,1） 有效数据 String要包含"\0",为 （6+1）+4+4 
		System.out.printf("usefulness ratio:%.2f", (double)(7+4+4)*recordNum/file.length());
	}
}
