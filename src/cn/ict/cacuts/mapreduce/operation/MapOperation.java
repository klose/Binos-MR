package cn.ict.cacuts.mapreduce.operation;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import cn.ict.cacuts.mapreduce.MRConfig;
import cn.ict.cacuts.mapreduce.MapContext;
import cn.ict.cacuts.mapreduce.Mapper;
import cn.ict.cacuts.test.WordCountTest;

import com.transformer.compiler.Operation;

public class MapOperation implements Operation{
	private static final Log LOG = LogFactory.getLog(MapOperation.class);
	@Override
	public void operate(String[] inputPath, String[] outputPath) {
		// TODO Auto-generated method stub
		
//		if (MRConfig.getReduceTaskNum() != outputPath.length) {
//			LOG.error("The number of reduce task conflicted with the number of output.");
//		}
//		if (inputPath.length != 1) {
//			LOG.error("The input of Map Task should have one input.");
//		}

		MapContext context;
		try {
			context = new MapContext(inputPath[0], outputPath);
			context.setOutputPath(outputPath);
			//Class<? extends Mapper>  mapClass = MRConfig.getMapClass();
			Class<? extends Mapper>  mapClass = WordCountTest.TokenizerMapper.class;
			
			System.out.println("&&&&&&&&&&&&&&&&&&&" + mapClass.getName());
			Constructor<Mapper> meth = (Constructor<Mapper>) mapClass.getConstructor(new Class[0]);
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
		} 
	}
	
	public static void main(String [] args) {
		
	}
	 
}

