package cn.ict.cacuts.userinterface;

import org.apache.hadoop.fs.Path;

import cn.ict.cacuts.mapreduce.MRConfig;
import cn.ict.cacuts.mapreduce.Mapper;
import cn.ict.cacuts.mapreduce.Reducer;
import cn.ict.cacuts.mapreduce.WorkFlow;
import cn.ict.cacuts.test.TestWorkFlow_Work1;
import cn.ict.cacuts.test.TestWorkFlow_Work2;
import cn.ict.cacuts.test.TestWorkFlow_Work3;

public class MRJob extends MRJobContext {
	public static enum JobState {
		DEFINE, RUNNING
	};

	private JobState state = JobState.DEFINE;
	public static MRConfig conf;

	public MRJob() {
		this(conf = new MRConfig());
	}

	public MRJob(MRConfig config) {
		super(config, null);
	}

	public MRJob(MRConfig config, String jobName) {
		super(config, jobName);
	}

	private void ensureState(JobState state) throws IllegalStateException {
		if (state != this.state) {
			throw new IllegalStateException("Job in state " + this.state
					+ " instead of " + state);
		}
	}

	public void setInputFileName(String[] inputFileName) {
		ensureState(JobState.DEFINE);
		conf.setInputFileName(inputFileName);
	}

	public void setOutputFileName(String[] outputFileName) {
		ensureState(JobState.DEFINE);
		conf.setOutputFileName(outputFileName);
	}

	public void setNumReduceTasks(int tasks) throws IllegalStateException {
		ensureState(JobState.DEFINE);
		conf.setReduceTaskMem(tasks);
	}

	public void setWorkingDirectory(Path dir) {
		ensureState(JobState.DEFINE);
		conf.setWorkingDirectory(dir);
	}

	/**
	 * Set the {@link Mapper} for the job.
	 * 
	 * @param cls
	 *            the <code>Mapper</code> to use
	 * @throws IllegalStateException
	 *             if the job is submitted
	 */
	public void setMapperClass(Class<? extends Mapper> cls)
			throws IllegalStateException {
		ensureState(JobState.DEFINE);
		conf.setMapClass(cls);
	}

	/**
	 * Set the combiner class for the job.
	 * 
	 * @param cls
	 *            the combiner to use
	 * @throws IllegalStateException
	 *             if the job is submitted
	 */
	public void setCombinerClass(Class<? extends Reducer> cls)
			throws IllegalStateException {
		ensureState(JobState.DEFINE);
		conf.setReduceClass(cls);
	}

	/**
	 * Set the {@link Reducer} for the job.
	 * 
	 * @param cls
	 *            the <code>Reducer</code> to use
	 * @throws IllegalStateException
	 *             if the job is submitted
	 */
	public void setReducerClass(Class<? extends Reducer> cls)
			throws IllegalStateException {
		ensureState(JobState.DEFINE);
		conf.setReduceClass(cls);
		;
	}

	public void setMapOutputKeyValueTypeClass(Class<?> theClass)
			throws IllegalStateException {
		ensureState(JobState.DEFINE);
		conf.setMapOutputKeyValueTypeClass(theClass);
	}

	public void setFinalOutputKeyValueTypeClass(Class<?> theClass)
			throws IllegalStateException {
		ensureState(JobState.DEFINE);
		conf.setFinalOutputKeyValueTypeClass(theClass);
	}

	public void setJobName(String name) throws IllegalStateException {
		ensureState(JobState.DEFINE);
		conf.setJobName(name);
	}

	// ///////////////////////////////////////////////////////////////////////////
	// public String getTrackingURL() {
	// ensureState(JobState.RUNNING);
	// return info.getTrackingURL();
	// }

	public void submit() {

		WorkFlow workFlow = new WorkFlow();
		workFlow.setInputPath(conf.getInputFileName());
		workFlow.setOutputPath(conf.getOutputFileName());
		workFlow.addMapClass(conf.getMapClass());
		workFlow.addReduceClass(conf.getReduceClass());
		workFlow.constructWorkFlow();

		state = JobState.RUNNING;
	}

}
