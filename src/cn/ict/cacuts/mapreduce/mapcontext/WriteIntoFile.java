package cn.ict.cacuts.mapreduce.mapcontext;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class WriteIntoFile {

//	public String interResultPathPrefix = System.getProperty("user.home") + "/CactusTest/";
	
	public String fileName  ;
	public WriteIntoFile(){}
	public WriteIntoFile(String fileName){
		this.fileName = fileName; 
	}
	
	public void setFileName(String fileName){
		this.fileName = fileName;
	}
	
	public String getFileName(){
		return this.fileName ;
	}

	
	public void writeIntoFile(ArrayList pairs ) { 	
		String outString = "";
		BufferedWriter out;
		try {
			out = new BufferedWriter(new FileWriter(this.fileName,true));

			for (int i = 0; i < pairs.size(); i++) {
				outString = pairs.get(i).toString();
				out.write(outString + "\n");
			}
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void writeIntoFile(ArrayList pairs ,String fileName) {
	//	this.fileName += fileName; 		
		String outString = "";
		BufferedWriter out;
		try {
			out = new BufferedWriter(new FileWriter(this.fileName,true));
			
			for (int i = 0; i < pairs.size(); i++) {
				outString = pairs.get(i).toString();
				out.write(outString + "\n");
			}
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void writeIntoFile(Object[] pairs ) { 	
		String outString = "";
		BufferedWriter out;
		try {
			out = new BufferedWriter(new FileWriter(this.fileName,true));
			//System.out.println("pairs.length " + pairs.length);
			for (int i = 0; i < pairs.length; i++) {
				outString = pairs[i].toString();
				out.write(outString + "\n");
			}
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void writeIntoFile(Object[] pairs ,String fileName) {
		//this.fileName += fileName; 		
		String outString = "";
		BufferedWriter out;
		try {
			out = new BufferedWriter(new FileWriter(this.fileName,true));
			
			for (int i = 0; i < pairs.length; i++) {
				outString = pairs[i].toString();
				out.write(outString + "\n");
			}
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String[] keys = { "key1", "key2", "key3", "key4", "key5", "key6" };
		ArrayList pairs = new ArrayList();
		pairs.add("key1 , 1");
		pairs.add("key2 , 2");
		
		WriteIntoFile tt = new WriteIntoFile();
	//	tt.writeIntoFile(pairs , "test");
		tt.setFileName("XXX");
		tt.writeIntoFile(pairs);			
	}
}
