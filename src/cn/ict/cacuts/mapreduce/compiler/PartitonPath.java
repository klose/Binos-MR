package cn.ict.cacuts.mapreduce.compiler;

public class PartitonPath {
	/*
	 * @param parallelNum   the task numbers in a phase
	 * @param path the job's input or output path array
	 * @param i 
	 * */
	protected static String[] seqPart(int parallelNum,String[] path,int i){
		int k = path.length/parallelNum;
		String[] tmppath = new String[k];
		for(int j=0;j<k;j++){
			tmppath[j] = path[i*k+j];
		}
		return tmppath;
	}
}
