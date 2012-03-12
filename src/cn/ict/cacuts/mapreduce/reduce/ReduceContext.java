package cn.ict.cacuts.mapreduce.reduce;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import temporary.DealMapOutUtil;

import com.transformer.compiler.DataState;
import com.transformer.compiler.JobConfiguration;
import com.transformer.compiler.TransmitType;

import cn.ict.binos.transmit.MessageClientChannel;
import cn.ict.cacuts.mapreduce.FileSplitIndex;
import cn.ict.cacuts.mapreduce.HdfsFileLineReader;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntList;
import cn.ict.cacuts.mapreduce.ReadFromDataBus;
import cn.ict.cacuts.mapreduce.map.KVList;
import cn.ict.cacuts.mapreduce.map.MapContext;

public class ReduceContext <KEYIN, VALUEIN, KEYOUT, VALUEOUT>{


	private final static Log LOG = LogFactory.getLog(ReduceContext.class);
	private final static String mergeTmpFileName = "merge_final";
	private DealReduceInputUtil receive ;
	private DealReduceOutputUtil outPut ;
	private DataState state;
	//private FileSplitIndex splitIndex = new FileSplitIndex();
	//private HdfsFileLineReader lineReader = new HdfsFileLineReader();   /////line reader should not be hdfs reader
	private String key = null;
	private Iterable<Integer> vlist = null;
	String[] reduceRemoteReadPaths;
	String tmpLocalFilePath;
	private static String mergeTmpPath = "reduce-merge-final";
	private String[] outputPath;
	private ReadFromDataBus reader;
	public ReduceContext() {
		
	}
	public ReduceContext(String[] reduceRemoteReadPaths, String[] outputPath, String tmpLocalFilePath,
			 String taskId) {
		this.reduceRemoteReadPaths = reduceRemoteReadPaths;
		this.tmpLocalFilePath = tmpLocalFilePath;		
		setOutputPath(outputPath);
		if (!reduceRemoteReadPaths[0].matches(TransmitType.MESSAGE.toString()+ ".*")) {
			this.state = DataState.REMOTE_FILE;
			this.mergeTmpPath = tmpLocalFilePath + "/" +  mergeTmpFileName;
		}
		else {
			this.state = DataState.MESSAGE_POOL;
			this.mergeTmpPath = JobConfiguration.getMsgHeader() + taskId + mergeTmpFileName;
		}
		this.outPut = new DealReduceOutputUtil(this.outputPath, this.state);		
	}
	/**
	 * read data from Remote data to local.
	 * if state is REMOTE_FILE, use Http copier.
	 * if state is MESSAGE_POOL, use Message Pool, 
	 * and in this function, it does not make sense. 
	 * */
	public void init(){
		
		receive = new DealReduceInputUtil(reduceRemoteReadPaths, tmpLocalFilePath, mergeTmpPath, state);
		receive.prepared();
		if (this.state == DataState.REMOTE_FILE) 
			this.state = DataState.LOCAL_FILE;
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
		if (this.state == DataState.REMOTE_FILE) {
			File file  =  new File(this.mergeTmpPath);
			if (!file.exists()) {
				throw new FileNotFoundException(this.mergeTmpPath);
			}
		}
		reader = new ReadFromDataBus(this.mergeTmpPath);
		LOG.info("mergeTmpPath:" + this.mergeTmpPath + " length:" + new File(this.mergeTmpPath).length());
		
	}

	public boolean nextKey()  {
		//TODO need initialize lineReader///////////////////////////////////////////////
		KVPairIntList curList = null;	
		if (this.state == DataState.MESSAGE_POOL) {
			curList = reader.getOneKVPairIntList();
		}
		else if (this.state == DataState.LOCAL_FILE) {
			curList = (KVPairIntList) reader.readKVPairIntList();
		}
		if (curList != null) {
			key = curList.getKey();
			vlist =  curList.getVlistList();
			return true;
		}
		return false;
	}

	public String getCurrentKey() {
		return this.key;
	}
	
	 public Iterable<Integer> getValues() {
		 return this.vlist;
	 }

	public void output(KEYOUT key, VALUEOUT value) {
		outPut.receive(key, value);
	}
	

	
	public String[] getOutputPath() {
		return outputPath;
	}
	
	private void setOutputPath(String[] outPutPath) {
		this.outputPath = new String[outPutPath.length];
		for (int i = 0; i < outPutPath.length; ++i) {
			this.outputPath[i] = new String(this.tmpLocalFilePath + "/" + outPutPath[i]);
		}
		//this.outPut.setOutputPath(this.outputPath);
	}
	
	public String[] getReduceRemoteReadPaths() {
		return reduceRemoteReadPaths;
	}
	public void setReduceRemoteReadPaths(String[] reduceRemoteReadPaths) {
		this.reduceRemoteReadPaths = reduceRemoteReadPaths;
	}
	public String getTmpLocalFilePath() {
		return tmpLocalFilePath;
	}
	public void setTmpLocalFilePath(String tmpLocalFilePath) {
		this.tmpLocalFilePath = tmpLocalFilePath;
	}
	public void flush() {
		outPut.FinishedReceive();
		cleanTmpData();
	}
	
	private void cleanTmpData() {
		if (this.state == DataState.REMOTE_FILE) {
			File file  =  new File(this.mergeTmpPath);
			file.delete();
		}
		else if (this.state == DataState.MESSAGE_POOL) {
			MessageClientChannel mcc = new MessageClientChannel();
			mcc.FreeAllData(this.mergeTmpPath);
		}
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
