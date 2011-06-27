package cn.ict.cacuts.mapreduce.compiler;

public class PhaseStruct {
	private TaskStruct[] ts;
	private int parallelNum;
	private String phaseID;
	private ParallelLevel plevel;
	public PhaseStruct(ParallelLevel plevel) {
		
		this.plevel = plevel;
	}
	public void setPhaseID(int phaseID){
		String ph = String.valueOf(phaseID);
		this.phaseID = plevel.getLevel()+"_"+ph;
	}
	
	/**
	 * add the first level's task Struct or last level's task struct. 
	 * In the first level, define path as the inputPath.
	 * In the last level, define path as the outputPath.
	 * @param ts
	 * @param parallelNum
	 * @param inputPath
	 */
	public void addTask(TaskStruct ts, int parallelNum, String[] path) {
		this.parallelNum = parallelNum;
		this.ts = new TaskStruct[parallelNum];
		if(path.length%parallelNum != 0){
			System.out.println("the wrong input path or output path number.");
			System.exit(2);
		}
		int k = path.length/parallelNum;
		String[] tmppath = new String[k];
		//TODO how to use one taskStruct to construct parallelNum tasks.and assign each task an id.and assign input or output paths.
		for(int i = 0;i < this.parallelNum;i++){
			this.ts[i] = new TaskStruct(ts);
			this.ts[i].setTaskId(phaseID+"_"+i);
			tmppath = PartitonPath.seqPart(parallelNum, path, i);
			if(this.plevel.getLevel() == 0){
				this.ts[i].setInputPathNum(k);
				this.ts[i].setInputPath(tmppath);
			}
			else if(this.plevel.getLevel() == -1){
				this.ts[i].setOutputPathNum(k);
				this.ts[i].setOutputPath(tmppath);
			}
		}		
		
		
	}
	/**
	 * add the middle phase in job. These tasks are neither in the first level, 
	 * nor in the last level.
	 * @param ts
	 * @param parallelNum
	 */
	public void addTask(TaskStruct ts, int parallelNum) {
		if(this.phaseID == null){
			System.err.println("please add the this phase to a job first!");
			System.exit(2);
		}
		this.parallelNum = parallelNum;
		this.ts = new TaskStruct[parallelNum];
		for(int i=0; i< this.parallelNum;i++){
			this.ts[i] = new TaskStruct(ts);
			//TODO passed by reference to passed by value
			this.ts[i].setTaskId(phaseID+"_"+i);
		
			
			
		}
//		for(int i=0;i<this.parallelNum;i++){
//			System.out.println(this.ts[i].getTaskId());
//		}
	}
	
	public TaskStruct getTask(TaskStruct ts){
		TaskStruct tss = new TaskStruct();
		tss = ts;
		return tss;
	}
	public TaskStruct[] getTaskStruct(){
		return this.ts;
	}
	public int getParallelNum(){
		return this.parallelNum;
	}
}
