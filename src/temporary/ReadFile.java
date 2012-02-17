package temporary;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class ReadFile {

	public static String fileName = System.getProperty("user.home")+ "/CactusTest/map_1_out_0";
	public ReadFile() {
	}

	public ReadFile(String fileName) {
		this.fileName = fileName;
	}
	/**
	 * 
	 */
	public static void readMethod1() {
		int c = 0;
		try {
			FileReader reader = new FileReader(fileName);
			c = reader.read();
			while (c != -1) {
				//System.out.print((char) c);
				c = reader.read();
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 使用BufferedReader类读文本文件
	 */
	public static ArrayList readMethod2() {
		ArrayList readed = new ArrayList();
		String line = "";
		try {
			BufferedReader in = new BufferedReader(new FileReader(fileName));
			line = in.readLine();
			while(line != null){
				//System.out.println(line);
				readed.add(line);
				line = in.readLine();
			}			
			in.close();
		//	System.out.println(readed.size());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return readed;
	}

	/**
	 * 
	 */
	public static void readMethod1(String fileName) {
		int c = 0;
		try {
			FileReader reader = new FileReader(fileName);
			c = reader.read();
			while (c != -1) {
				//System.out.print((char) c);
				c = reader.read();
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 使用BufferedReader类读文本文件
	 */
	public static ArrayList readMethod2(String fileName) {
		String line = "";
		ArrayList readed = new ArrayList();
		try {
			BufferedReader in = new BufferedReader(new FileReader(fileName));
			line = in.readLine();
			while(line != null){
				//System.out.println(line);
				readed.add(line);
				line = in.readLine();
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return readed;
	}
	public void setFileName(String fileName){
		this.fileName = fileName;
	}
	
	public String getFileName(){
		return this.fileName ;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ReadFile readFile = new ReadFile();
		ArrayList readed = readFile.readMethod2();

	}
}
