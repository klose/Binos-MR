package cn.ict.cacuts.mapreduce;

import java.util.ArrayList;
import java.util.Map;

import cn.ict.cacuts.test.TestWorkFlow_Work1;
import cn.ict.cacuts.test.TestWorkFlow_Work2;
import cn.ict.cacuts.test.TestWorkFlow_Work3;

import com.transformer.compiler.Channel;
import com.transformer.compiler.ChannelManager;
import com.transformer.compiler.JobCompiler;
import com.transformer.compiler.JobConfiguration;
import com.transformer.compiler.JobStruct;
import com.transformer.compiler.Operation;
import com.transformer.compiler.ParallelLevel;
import com.transformer.compiler.PhaseStruct;
import com.transformer.compiler.TaskStruct;

public class WorkFlow {
	JobStruct job = new JobStruct();
	private static final String PhaseStruct = null;
	ArrayList<Class <? extends Operation>>   works = new ArrayList();
	int mapNaumber = 2;
	int reduceNumber = 1;
	int parallelNumber = 1;
	String[] inputPath;
	String[] outputPath;
	public void setClass(){
		
	}
	
	public void addOperationClass(Class<? extends Operation> cls) {		
		works.add(cls);
	} 	
	
	public void constructWorkFlow(){
		constructWork();
		constructFlow(job.getPhaseStruct());
	}
	
	public void constructWork(){
		String pathPrefix = JobConfiguration.getPathHDFSPrefix();		
		ParallelLevel pal = new ParallelLevel(ParallelLevel.assignFirstLevel());
		PhaseStruct phase;
		TaskStruct task;
		int i;
		for(i = 0 ; i < works.size() ; i ++ ){
			if(i < works.size() - 1){
				phase  = new PhaseStruct(pal);
			}else{
				phase  = new PhaseStruct(pal.assignEndLevel());
			}	
			
			job.addPhaseStruct(phase);
			task = new TaskStruct();
			task.setOperationClass(works.get(i));
			//parallelNumber = 4;///////////////////////////////////here need to read  the user configuration
			if(i == 0 ){
				phase.addTask(task, parallelNumber, inputPath);
			}else{
				if( i < works.size() - 1){
					phase.addTask(task, parallelNumber);
				}else{
					phase.addTask(task, parallelNumber,outputPath);
				}
				
			}		
			pal = pal.nextLevel();
		}
	}
	
	/**
	 * i = 0 means split phase
	 * i = 1 means map phase
	 * i = 2 means reduce phase
	 * and there are only 3 phase in the mapReduce model
	 * */
	public void constructFlow(ArrayList<PhaseStruct> phases){
		
		if(phases.size() != 3){
			System.out.println("in the mapreduce model ,there should be three phases , please check it");
			System.exit(2);
		}
		
		PhaseStruct phase0 = phases.get(0); //split
		PhaseStruct phase1 = phases.get(1); //map
		PhaseStruct phase2 = phases.get(2); //reduce
		
		ChannelManager channelManager = new ChannelManager();
		//split to map
		Channel[] channel0_1 = new Channel[phase1.getParallelNum()];				
		for(int i = 0 ;i<phase1.getParallelNum();i++)
		{	
			channel0_1[i] = new Channel(phase0.getTaskStruct()[i],0,phase1.getTaskStruct()[i],0);			
		}
		channelManager.addChannels(channel0_1);
		//map to reduce
		Channel[] channel1_2;
		for(int i = 0 ;i<phase2.getParallelNum();i++)
		{	
			channel1_2 = new Channel[phase1.getParallelNum()];
			for(int j = 0 ;j<phase1.getParallelNum();j++){
				channel1_2[j] = new Channel(phase1.getTaskStruct()[j],i,phase2.getTaskStruct()[i],j);
			}
			channelManager.addChannels(channel1_2);
		}
		
		
		analisisCompile(channelManager);
	}
	
	
	public int getMapNaumber() {
		return mapNaumber;
	}

	public void setMapNaumber(int mapNaumber) {
		this.mapNaumber = mapNaumber;
	}

	public int getReduceNumber() {
		return reduceNumber;
	}

	public void setReduceNumber(int reduceNumber) {
		this.reduceNumber = reduceNumber;
	}

	public int getParallelNumber() {
		return parallelNumber;
	}

	public void setParallelNumber(int parallelNumber) {
		this.parallelNumber = parallelNumber;
	}

	public String[] getInputPath() {
		return inputPath;
	}

	public void setInputPath(String[] inputPath) {
		this.inputPath = inputPath;
	}

	public String[] getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String[] outputPath) {
		this.outputPath = outputPath;
	}

	public void analisisCompile(ChannelManager channelManager){
		Map<String, TaskStruct> map = channelManager.parseDep();
		JobCompiler compiler = new JobCompiler(map, job);
		compiler.compile();
	}
	
	public static void main(String[] args){
		String[] inputPath = {System.getProperty("user.home") + "/CactusTest/" + "map_1_out_0" ,
				System.getProperty("user.home") + "/CactusTest/" + "map_1_out_1"};
		String[] outputPath = {System.getProperty("user.home") + "/CactusTest/" + "reduce_out"};
		WorkFlow tt = new WorkFlow();
		tt.addOperationClass(TestWorkFlow_Work1.class);
		tt.addOperationClass(TestWorkFlow_Work2.class);
		tt.addOperationClass(TestWorkFlow_Work3.class);
		tt.setInputPath(inputPath);
		tt.setOutputPath(outputPath);
		tt.constructWorkFlow();
		//System.out.println(tt.works.size());
	}
}
