package cn.ict.cacuts.conf;

public class JobConfiguration {
	private static String workingDirectory = System.getProperty("java.io.tmpdir");
	private static String createTime = "job";
	private static String pathHDFSPrefix = "hdfs://";
	
	public static String getPathHDFSPrefix() {
		return pathHDFSPrefix;
	}
	public static void setPathHDFSPrefix(String pathHDFSPrefix) {
		JobConfiguration.pathHDFSPrefix = pathHDFSPrefix;
	}
	public static void setWorkingDirectory(String workingDirectory) {
		JobConfiguration.workingDirectory = workingDirectory;
	}
	public static String getWorkingDirectory() {
		return workingDirectory;
	}
	
	public static String getCreateTime() {
		return createTime;
	}
	public static void setCreateTime(String createTime) {
		JobConfiguration.createTime = createTime;
	}
	
	
}
