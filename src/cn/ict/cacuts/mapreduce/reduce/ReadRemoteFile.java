package cn.ict.cacuts.mapreduce.reduce;

public class ReadRemoteFile {

	String[] reduceInputFilePath;//Binos URL
	String[] readedRemoteReadFiles; // the file locally
	String tmpLocalDirPath; // the directory of default path


	//read files from remote and save into the local disk as it read
	public ReadRemoteFile(String[] reduceInputFilePath, String tmpLocalDirPath) {
		this.reduceInputFilePath = reduceInputFilePath;
		this.tmpLocalDirPath = tmpLocalDirPath;
		// TODO do  read files
		switchFileName();
		
	}
	
	public void launchFetchFiles() {
		
	}
	/**
	 * need to fill correspond code
	 * */
	private void switchFileName() {
		//TODO switch/////////////////////////////////////////
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
