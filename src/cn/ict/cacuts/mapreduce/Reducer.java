package cn.ict.cacuts.mapreduce;

import java.io.IOException;
import cn.ict.cacuts.mapreduce.reduce.*;

public class Reducer<KEY, VALUE> {

	/**
	 * The <code>Context</code> passed on to the {@link Reducer}
	 * implementations.
	 */
	public abstract class Context {
	}

	/**
	 * Called once at the start of the task.
	 */
	protected void setup(ReduceContext context) throws IOException,
			InterruptedException {
		context.init();
	}

	public void reduce(String line, ReduceContext<KEY, VALUE> context) {//////////////
	}
	/**
	 * Advanced application writers can use the
	 * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
	 * control how the reduce task works.
	 */
	@SuppressWarnings("unchecked")
	public void run(ReduceContext<KEY,VALUE> context) throws IOException, InterruptedException {
		setup(context);
		while (context.hasNextLine()) {
			reduce(context.getNextLine(), context);
		}
//		context.flushInput();
//		context.flushOutput();
		context.flush();
		cleanup(context);
	}
	
	/**
	 * Called once at the end of the task.
	 */
	protected void cleanup(ReduceContext<KEY,VALUE> context) throws IOException,
			InterruptedException {
		// NOTHING
	}
}