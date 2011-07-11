package cn.ict.cacuts.mapreduce.reduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.ict.cacuts.mapreduce.FileSplitIndex;
import cn.ict.cacuts.mapreduce.HdfsFileLineReader;
import cn.ict.cacuts.mapreduce.MapContext;
import cn.ict.cacuts.mapreduce.mapcontext.DealMapOutUtil;
import cn.ict.cacuts.mapreduce.mapcontext.KVList;

public class ReduceContext <KEYIN, VALUEIN, KEYOUT, VALUEOUT>{


	private final static Log LOG = LogFactory.getLog(ReduceContext.class);
	private static Configuration conf = new Configuration();	
	private static FileSystem fs;
	private DealReduceInputUtil receive ;
	private DealReduceOutputUtil outPut = new DealReduceOutputUtil();

	//private FileSplitIndex splitIndex = new FileSplitIndex();
	//private HdfsFileLineReader lineReader = new HdfsFileLineReader();   /////line reader should not be hdfs reader
	private KEYIN key = null;
	private Iterable<VALUEIN> vlist = null;
	String[] reduceRemoteReadFiles;
	String tmpLocalFilePath;
	String mergeTmpFile;
	private String[] outputPath;
	private ObjectInputStream in;// this is used to read file
	

	
	public ReduceContext() {
		
	}
	public ReduceContext(String[] reduceRemoteReadFiles, String tmpLocalFilePath,
			String mergeTmpFile, String[] outputPath) {
		this.reduceRemoteReadFiles = reduceRemoteReadFiles;
		this.tmpLocalFilePath = tmpLocalFilePath;
		this.outputPath = outputPath;
		this.mergeTmpFile = tmpLocalFilePath + mergeTmpFile;
	}
	/**
	 * read remote file and save them
	 * */
	public void init(){
		receive = new DealReduceInputUtil(reduceRemoteReadFiles, tmpLocalFilePath, mergeTmpFile);
		receive.prepared();
		try {
			initStream();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * prepare the streaming for the mergeTmpFile
	 * @throws IOException 
	 */
	private void initStream() throws IOException {
		File file  =  new File(mergeTmpFile);
		if (!file.exists()) {
			throw new FileNotFoundException(mergeTmpFile);
		}
		else {
			in = new ObjectInputStream(new FileInputStream(file));
		}
	}

	public boolean nextKey()  {
		//TODO need initialize lineReader///////////////////////////////////////////////
		KVList<KEYIN,VALUEIN> curList = null;
		
		try {
			if ((curList = (KVList<KEYIN, VALUEIN>) in.readObject()) != null) {
				key = curList.getKey();
				vlist = (Iterable<VALUEIN>) curList.getValue().iterator();
				return true;
			}
		} catch (IOException e) {
			// read the file end.
			return false;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public KEYIN getCurrentKey() {
		return this.key;
	}
	
	 public Iterable<VALUEIN> getValues() {
		 return this.vlist;
	 }

	public void output(KEYOUT key, VALUEOUT value) {
		//System.out.println("key : " + key);
		//System.out.println("value : " + value);
		outPut.receive(key, value);
	}
	

	
	public String[] getOutputPath() {
		return outputPath;
	}
	
	public void setOutputPath(String[] outPutPath) {
		this.outputPath = outPutPath;
		this.outPut.setOutputPath(outPutPath);
	}
	
	public String[] getReduceRemoteReadFiles() {
		return reduceRemoteReadFiles;
	}
	public void setReduceRemoteReadFiles(String[] reduceRemoteReadFiles) {
		this.reduceRemoteReadFiles = reduceRemoteReadFiles;
	}
	public String getTmpLocalFilePath() {
		return tmpLocalFilePath;
	}
	public void setTmpLocalFilePath(String tmpLocalFilePath) {
		this.tmpLocalFilePath = tmpLocalFilePath;
	}
	public void flush() {
		outPut.FinishedReceive();
	}
	
	public void controlReadWhichFile(){
		
	}
	
	public void readSpecificFile(){
		
	}
	
	
	public void writeTempleFile(){
		
	}
	
	public void dealTempleFile(){
		
	}

}
