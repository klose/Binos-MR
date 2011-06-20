package cn.ict.cacuts.generateDAG.baseelement.job;

public class Task {

	private String taskId;
	private String[] inputPath;
	private String[] outputPath;
	private String taskXmlPath;
	private String taskJarPath; // 在线框架需要吗？
	private Class <? extends BaseOperation> operCls;

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
