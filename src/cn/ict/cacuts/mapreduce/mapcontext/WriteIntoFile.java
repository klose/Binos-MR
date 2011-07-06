package cn.ict.cacuts.mapreduce.mapcontext;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class WriteIntoFile {

	public String fileName;

	public WriteIntoFile() {
	}

	public WriteIntoFile(String fileName) {
		this.fileName = fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFileName() {
		return this.fileName;
	}

	public void writeIntoFile(ArrayList pairs) {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();   
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(bout);
			FileOutputStream fout = new FileOutputStream(this.fileName, true);
			for (int i = 0; i < pairs.size(); i++) {
				oos.writeObject(pairs.get(i));
			}
			oos.flush();			
			fout.write(bout.toByteArray());
			fout.write(-1);
			fout.write(-1);
			fout.flush();
			bout.close();
			oos.close();
			fout.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void writeIntoFile(ArrayList pairs, String fileName) {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();   
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(bout);
			FileOutputStream fout = new FileOutputStream(fileName, true);
			for (int i = 0; i < pairs.size(); i++) {
				oos.writeObject(pairs.get(i));
			}
			oos.flush();			
			fout.write(bout.toByteArray());	
			fout.write(-1);
			fout.write(-1);
			fout.flush();
			bout.close();
			oos.close();
			fout.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void writeIntoFile(Object[] pairs) {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();   
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(bout);
			FileOutputStream fout = new FileOutputStream(this.fileName, true);
			for (int i = 0; i < pairs.length; i++) {
				oos.writeObject(pairs[i]);
			}
			oos.flush();			
			fout.write(bout.toByteArray());	
			fout.write(-1);
			fout.write(-1);
			fout.flush();
			bout.close();
			oos.close();
			fout.close();	
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void writeIntoFile(Object[] pairs, String fileName) {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();   
		ObjectOutputStream oos;
		try {
			oos = new ObjectOutputStream(bout);
			FileOutputStream fout = new FileOutputStream(fileName, true);
			for (int i = 0; i < pairs.length; i++) {
				oos.writeObject(pairs[i]);
			}
			oos.flush();			
			fout.write(bout.toByteArray());	
			fout.write(-1);
			fout.write(-1);
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

		WriteIntoFile tt = new WriteIntoFile();
		// tt.writeIntoFile(pairs , "test");
		tt.setFileName("XXX");
		tt.writeIntoFile(pairs);
	}
}
