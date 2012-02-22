package cn.ict.cacuts.mapreduce.operation;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import cn.ict.cacuts.mapreduce.MRConfig;
import cn.ict.cacuts.mapreduce.Mapper;
import cn.ict.cacuts.mapreduce.Reducer;
import cn.ict.cacuts.mapreduce.map.MapContext;
import cn.ict.cacuts.mapreduce.reduce.ReduceContext;
import cn.ict.cacuts.test.WordCountTest;

import com.transformer.compiler.JobProperties;
import com.transformer.compiler.Operation;

public class ReduceOperation implements Operation{
	private static final Log LOG = LogFactory.getLog(ReduceOperation.class);
	@Override
	public void operate(JobProperties properties, String[] inputPath, String[] outputPath) {
		// TODO Auto-generated method stub
		
//		if (Integer.parseInt(properties.getProperty("reduce.task.num")) != outputPath.length) {
//			LOG.error("The number of reduce task conflicted with the number of output.");
//			return;
//		}
		if (outputPath.length < 1) {
			LOG.error("The number of reduce task conflicted with the number of output.");
			return;
		}
		if (Integer.parseInt(properties.getProperty("map.task.num")) != inputPath.length) {
			LOG.error("The input of Map Task should have one input.");
			return;
		}
		System.out.println(inputPath.length+ "inputpath" + inputPath[0]);
		System.out.println(outputPath.length + "outputPath" + outputPath[0]);
		System.out.println();
		System.out.println();
		ReduceContext context;
		try {
			context = new ReduceContext(inputPath, properties.getProperty("tmpDir"), "/merge_final", outputPath);
			
			Class<? extends Reducer>  reduceClass = (Class<? extends Reducer>) Class.forName(properties.getProperty("reducer.class"));
			//Class<? extends Reducer>  reduceClass = MRConfig.getReduceClass();
			Constructor<Reducer> meth = (Constructor<Reducer>) reduceClass.getConstructor(new Class[0]);
			meth.setAccessible(true);
			meth.newInstance().run(context);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

}
