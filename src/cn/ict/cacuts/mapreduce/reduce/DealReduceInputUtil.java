package cn.ict.cacuts.mapreduce.reduce;


import java.io.FileNotFoundException;
import java.io.IOException;
import com.transformer.compiler.DataState;
import cn.ict.binos.transmit.BinosURL;
import cn.ict.cacuts.mapreduce.MRConfig;
import cn.ict.cacuts.mapreduce.Merger;



public class DealReduceInputUtil<KEY, VALUE> {

	public String[] reduceDataInputPath;
	public BinosURL[] binosURLInput;
	private DataState state;
	//public String reduceOutPutDataName;
//	public Map<KEY, Vector<VALUE>> keyValue;
//	private boolean finishedReceive = false;
	// read remote Datas to save into local disk
	String tmpLocalDataPath;
	String[] readedRemoteDatas;
	String mergedTmpDataName;

	public DealReduceInputUtil(String[] reduceDataInputPath,String tmpLocalDataPath, String mergedTmpDataName, DataState state) {
		this.reduceDataInputPath = reduceDataInputPath;
		this.tmpLocalDataPath = tmpLocalDataPath;
		this.mergedTmpDataName = mergedTmpDataName;
		this.state = state;
	}
	
	public void prepared(){
		readDatas();
		merge();
	}
	
	private void readDatas() {
		if (this.state == DataState.REMOTE_FILE) {
			ReadRemoteData readRemoteData = null;
			readRemoteData = new ReadRemoteData(reduceDataInputPath,
						tmpLocalDataPath, MRConfig.getFetchThreadNum());
			readRemoteData.launchFetchData();
			this.readedRemoteDatas = readRemoteData.getReadedRemotePath();
			this.state = DataState.LOCAL_FILE;//the remote data fetched locally.
		}
		else if (this.state == DataState.MESSAGE_POOL) {
			this.readedRemoteDatas  = new String[reduceDataInputPath.length];
			for (int i = 0; i < reduceDataInputPath.length; i++) {
				this.readedRemoteDatas[i] = new String(
						BinosURL.getOpsUrl(reduceDataInputPath[i]));
			}
		}
	}

	private void merge() {
		Merger merge = new Merger();	
			if (null == this.readedRemoteDatas) {
				System.out.println("null == readedRemoteDatas");
			}
			for (String tmp: readedRemoteDatas) {
				System.out.println("readedRemoteDatas:"+tmp);
			}
			try {
				merge.merge(readedRemoteDatas, mergedTmpDataName, false, this.state) ;
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 
	}

	public void setInputDataPath(String[] reduceInputPath) {
		this.reduceDataInputPath = reduceInputPath;
	}

	public String[] getInputDataPath() {
		return this.reduceDataInputPath;
	}

	public String getTmpLocalDataPath() {
		return tmpLocalDataPath;
	}
	
	public void setTmpLocalDataPath(String tmpLocalDataPath) {
		this.tmpLocalDataPath = tmpLocalDataPath;
	}
//	public void FinishedReceive() {
//		this.finishedReceive = true;
//		// ////////////////////////////////////////need to
//		// deal////////////////////////
//	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String[] inputPath = {
				System.getProperty("user.home") + "/CactusTest/map_1_out_0",
				System.getProperty("user.home") + "/CactusTest/map_1_out_1" };
		String reduceOutPutDataName = System.getProperty("user.home")
				+ "/CactusTest/" + "reduce_out";
		String mergeDataPath = System.getProperty("user.home") +
				 "/CactusTest/merger_final";
		DealReduceInputUtil tt = new DealReduceInputUtil(inputPath,
				reduceOutPutDataName, mergeDataPath, DataState.REMOTE_FILE);
		tt.prepared();
	}

}
