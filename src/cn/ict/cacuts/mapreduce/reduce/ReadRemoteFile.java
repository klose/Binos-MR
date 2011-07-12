package cn.ict.cacuts.mapreduce.reduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.io.Text;
import cn.ict.binos.transmit.BinosDataClient;
import cn.ict.binos.transmit.BinosURL;

public class ReadRemoteFile {
	
	
	String[] reduceInputFilePath;//Binos URL
	String[] readedRemoteReadFiles; // the file locally
	String tmpLocalDirPath; // the directory of default path
	BinosURL[] binosURLInput; 

	//read files from remote and save into the local disk as it read
	public ReadRemoteFile(String[] reduceInputFilePath, String tmpLocalDirPath) throws FileNotFoundException {
		this.reduceInputFilePath = reduceInputFilePath;
		this.tmpLocalDirPath = tmpLocalDirPath;
		initializePath();
	}
	
	/*fetch the file from HttpServer*/
	public void launchFetchFiles() {
		Thread fetchThread = new Thread() {
			int fileCount = reduceInputFilePath.length;
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				
				for (int i = 0; i < fileCount; i++) {
					try {
						InputStream in = BinosDataClient.getInputStream(binosURLInput[i]);
						OutputStream out = new FileOutputStream(tmpLocalDirPath + "/" + i);
						byte[] buffer = new byte[8192];
						int k;
			            while ((k = in.read(buffer)) != -1) {
			            	out.write(buffer, 0, k);
			            }
			            out.close();
			            in.close();
			 		} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
				}
			}
		};
		fetchThread.start();
	}

	/*change the file path to BinosURL, ensure whether tmpLocalDirPath exists.*/
	private void initializePath() throws FileNotFoundException {
		this.binosURLInput = new BinosURL[reduceInputFilePath.length];
		for (int i = 0; i < reduceInputFilePath.length; i++) {
			this.binosURLInput[i] = new BinosURL(new Text(reduceInputFilePath[i]));
		}
		File tmpDir = new File(this.tmpLocalDirPath);
		if (!tmpDir.exists() || !tmpDir.isDirectory()) {
			throw new FileNotFoundException(this.tmpLocalDirPath + " not exists.");
		}
	}

	public String[] getReduceRemoteReadFiles() {
		return readedRemoteReadFiles;
	}

	public void setReduceRemoteReadFiles(String[] reduceRemoteReadFiles) {
		this.readedRemoteReadFiles = reduceRemoteReadFiles;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args){

	}

}
