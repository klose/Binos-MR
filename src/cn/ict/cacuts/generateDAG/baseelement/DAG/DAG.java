package cn.ict.cacuts.generateDAG.baseelement.DAG;

import cn.ict.cacuts.generateDAG.baseelement.job.Task;

public class DAG {

	public boolean userParalledFlag;// 用户定义并行度或者自动生成并行度

	public void setInitialInput(){
	};// 最原始的操作数据

	public void addVTask(Task task){
		
	}// 自动生成并行度

	public void addVTask(Task task, int parallelNum){
		
	}

	public void addFlow(Task srcTask, Task destTask){
		
	}

	public void addFlow(Task srcTask, Task destTask, String flowType){
		
	}

	public void setFinalOutput(){
		
	}// 设置输出路径

	public void regenerateDataFlow(){
		
	}// 根据用户设置重新生成执行图（并行度）

	public void beginToExecute(){
		
	}// 开始执行

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}
