package cn.ict.cacuts.mapreduce;

import java.io.IOException;
import cn.ict.cacuts.mapreduce.reduce.*;
public class Reducer<KEY, VALUE>  {

  /**
   * The <code>Context</code> passed on to the {@link Reducer} implementations.
   */
  public abstract class Context  {
  }

  /**
   * Called once at the start of the task.
   */
  protected void setup(ReduceContext context
                       ) throws IOException, InterruptedException {
    // NOTHING
  }

  /**
   * This method is called once for each key. Most applications will define
   * their reduce class by overriding this method. The default implementation
   * is an identity function.
   */
  @SuppressWarnings("unchecked")
  protected void reduce(ReduceContext context) throws IOException, InterruptedException {
	    setup(context);
	    while (context.hasNextLine()) {
	      map(context.getNextLine(), context);
	    }
	    context.flush();
	    cleanup(context);
  }

  /**
   * Called once at the end of the task.
   */
  protected void cleanup(Context context
                         ) throws IOException, InterruptedException {
    // NOTHING
  }

  /**
   * Advanced application writers can use the 
   * {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)} method to
   * control how the reduce task works.
   */
  @SuppressWarnings("unchecked")
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKey()) {
      reduce(context.getCurrentKey(), context.getValues(), context);
      // If a back up store is used, reset it
      ((ReduceContext.ValueIterator)
          (context.getValues().iterator())).resetBackupStore();
    }
    cleanup(context);
  }
}