package cn.ict.cacuts.mapreduce;
import java.io.IOException;

import cn.ict.cacuts.mapreduce.map.MapContext;
/**
 * Mapper : the base workflow of mapper operation.
 * @author jiangbing
 *
 * @param <KEY>
 * @param <VALUE>
 */
public abstract class Mapper<KEY, VALUE> {
	public Mapper() {}
	  /**
	   * Called once at the beginning of the task.
	   */
	  protected void setup(MapContext context
	                       ) throws IOException, InterruptedException {
	    // NOTHING
	  }
	public abstract void map(String line, MapContext<KEY,VALUE> context);
	
	
//	public void map(String line, MapContext<KEY, VALUE> context) {
//		String [] word = line.split(" ");
//		for (String tmp: word) {
//			context.output(tmp, 1);
//		}
//	}
	
	  /**
	   * Expert users can override this method for more complete control over the
	   * execution of the Mapper.
	   * @param context
	   * @throws IOException
	   */
	  public void run(MapContext<KEY,VALUE> context) throws IOException, InterruptedException {
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
	  protected void cleanup(MapContext<KEY,VALUE> context
	                         ) throws IOException, InterruptedException {
	    // NOTHING
	  }
	  
	
}
