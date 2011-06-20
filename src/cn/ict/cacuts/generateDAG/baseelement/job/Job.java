package cn.ict.cacuts.generateDAG.baseelement.job;

import java.util.ArrayList;

import cn.ict.cacuts.generateDAG.baseelement.DAG.Phase;


public class Job {

	private ArrayList<Phase> phaseList = new ArrayList<Phase>();
	private String jobXmlPath;
	private static int phaseId=0;
	private String jobDirectoryPath;
	public Job() {
		
	}
	
	public void addPhaseStruct(Phase ps) {	
		ps.setPhaseID(phaseId);
		phaseList.add(ps);
		phaseId++;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
}
