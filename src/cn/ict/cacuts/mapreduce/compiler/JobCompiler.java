package cn.ict.cacuts.mapreduce.compiler;

import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

public class JobCompiler {
	private Map<String, TaskStruct> taskMap; 
	private JobStruct job;
	public JobCompiler(Map<String, TaskStruct> taskMap, JobStruct job) {
		this.taskMap = taskMap;
		this.job = job;
	}
	public void compile() {
		try {
			CreateJob cj = new CreateJob(job, taskMap);
			cj.generateJob();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
