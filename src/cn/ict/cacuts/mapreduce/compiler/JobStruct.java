package cn.ict.cacuts.mapreduce.compiler;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class JobStruct {
	
	private ArrayList<PhaseStruct> phaseList = new ArrayList<PhaseStruct>();
	private String jobXmlPath;
	private static int phaseId=0;
	private String jobDirectoryPath;
	public JobStruct() {
		
	}
	
	public void addPhaseStruct(PhaseStruct ps) {	
		ps.setPhaseID(phaseId);
		phaseList.add(ps);
		phaseId++;
	}
	public String getJobXmlPath(){
		return this.jobXmlPath;
	}
	
	public ArrayList<PhaseStruct> getPhaseStruct(){
		return this.phaseList;
	}
}
