package cn.ict.cacuts.mapreduce.reduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import cn.ict.binos.transmit.BinosDataClient;
import cn.ict.binos.transmit.BinosURL;
/**
 * fetch data from thread.
 * @author jiangbing
 *
 */
public class ReadRemoteData {
	
	private static Logger LOG = Logger.getLogger(ReadRemoteData.class);
	//private volatile boolean isReadOver = false;// identify the end of the process of reading files
//	private Object lock = new Object();
	private Object waitOverLock = new Object();
	private final String[] reduceInputDataPath;//Binos URL
	private final String[] readedRemotePath; // the file locally
	private final String tmpLocalDirPath; // the directory of default path
	private final int threadNum; //the number of thread which fetches the data from the source.
	private AtomicInteger localityAccount = new AtomicInteger();
	private AtomicInteger readAccount  = new AtomicInteger();
	private AtomicBoolean isReadOver = new AtomicBoolean(false);
	private AtomicInteger controlThreadNum = new AtomicInteger();//record the number that  
	BinosURL[] binosURLInput; 

	//read files from remote and save into the local disk as it read
	public ReadRemoteData(String[] reduceInputDataPath, String tmpLocalDirPath, int threadNum) {
		this.reduceInputDataPath = reduceInputDataPath;
		this.readedRemotePath = new String[reduceInputDataPath.length];		
		this.tmpLocalDirPath = tmpLocalDirPath;
		this.threadNum = (threadNum < reduceInputDataPath.length ? threadNum: reduceInputDataPath.length);
		for (int i = 0; i < reduceInputDataPath.length; i++) {
			this.readedRemotePath[i] = new String();
		}	
		initializePath();
	}
	/*ensure whether tmpLocalDirPath exists.*/
	private void initializePath()  {
		File tmpDir = new File(this.tmpLocalDirPath);
		if (!tmpDir.exists() || !tmpDir.isDirectory()) {
			try {
				throw new FileNotFoundException(this.tmpLocalDirPath + " not exists.");
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	class FetchFileDataThread extends Thread {
		int index; // index in the readedRemotePath
		@Override
		public void run() {
			while (!isReadOver.get()) {
				//synchronized (lock) {
					index = readAccount.get();
					if (readAccount.addAndGet(1) >= reduceInputDataPath.length) {						
						isReadOver.set(true);
					}
				//}
				fetchFile(reduceInputDataPath[index]);
				if(isReadOver.get()) {
					synchronized(waitOverLock) {
						if (controlThreadNum.addAndGet(1) == threadNum){
							waitOverLock.notify();
						}
					}
				}
			}
		}
		private void fetchFile(String url) {
			BinosURL binosInputURL = new BinosURL(new Text(url));
			if ("LOCAL".equals((binosInputURL.getServiceType()))) {
				readedRemotePath[index] = new String(
						binosInputURL.getServiceOpsUrl());
				localityAccount.addAndGet(1);		
				LOG.info("Find a local file:"
						+ binosInputURL.toString());
			}
			else {
				try {
					InputStream in = BinosDataClient
						.getInputStream(binosInputURL);
					OutputStream out = new FileOutputStream(
						tmpLocalDirPath + "/" + index);
					readedRemotePath[index] = new String(
						tmpLocalDirPath + "/" + index);
					byte[] buffer = new byte[8192];
					int k;
					while ((k = in.read(buffer)) != -1) {
						out.write(buffer, 0, k);
					}
					out.close();
					in.close();
					LOG.info("fetch a remote file from "
						+ binosInputURL.toString() + " to "
						+ readedRemotePath[index]);
				} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				}
			}
		}
	}
			
	
	/*fetch the data from HttpServer*/
	public void launchFetchData() { 
		Thread [] threads = new FetchFileDataThread[threadNum];
		for (int i = 0; i < threadNum; i++) {
			threads[i] = new FetchFileDataThread();
			threads[i].start();
		}
		synchronized(waitOverLock) {
			while(!isReadOver.get()) {
				try {
					waitOverLock.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
//		new Thread(new Runnable() {
//			// wait the fetch thread to execute over.
//			@Override
//			public void run() {
//				// TODO Auto-generated method stub
//				synchronized(waitOverLock) {
//					while(!isReadOver.get()) {
//						try {
//							waitOverLock.wait();
//						} catch (InterruptedException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//					}
//				}
//			}
//			
//		}).start();
		
	}
	private void printStatistic() {
		System.out.println("locality:" + localityAccount.get());
		System.out.println("read total:" + readAccount.get());
		
	}
	

	public String[] getReadedRemotePath() {
		return readedRemotePath;
	}

//	public void setReduceInputDataPath(String[] reduceInputDataPath) {
//		this.reduceInputDataPath = reduceInputDataPath;
//	}
	/**
	 * @param args
	 */
	public static void main(String[] args){
		String tmpDir = "/home/jiangbing/test-readRemoteData";
		String inputPathPrefix = "REMOTE@Binos#read#http://127.0.0.1:36661/output?file=/opt/test/x";
		String[] inputPath = new String[100]; 
		for (int i = 0; i< inputPath.length; i++) {
			if (i < 10) {
				inputPath[i] = new String(inputPathPrefix + "0" +i);
			}
			else {
				inputPath[i] = new String(inputPathPrefix + i);
			}
		}
		long start = System.currentTimeMillis();
		ReadRemoteData rrd = new ReadRemoteData(inputPath, tmpDir, 2);
		rrd.launchFetchData();
		rrd.printStatistic();
		System.out.println("used total time:" + (System.currentTimeMillis() - start) + "ms");
		
	}

}
