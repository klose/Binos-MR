package cn.ict.cacuts.generateDAG.baseelement;

public class VTask {

	private String VTaskId;
	private Task task;
	private String inputPath; // 如果只有一个Vtask，就是直接定义输入路径
	private String outputPath; // 如果只有一个Vtask，就是直接定义输出路径
	private int parallelNum = -1;// 默认等于-1，可以用户设置或者自动生成

	public void setOperationClass(Class<? extends BaseOperation> opClass) {
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}
