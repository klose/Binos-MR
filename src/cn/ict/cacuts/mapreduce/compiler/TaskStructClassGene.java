package cn.ict.cacuts.mapreduce.compiler;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
/**
 * TaskStructClassGene is used for generating the runnable jar. 
 * Implement the interface of Operation, and realize the function of operate.
 * Version 0.2: public void operate(String[] inputPath, String[] outputPath)
 * The interface will provide more interface in next iteration of development.
 * Developers only care the target of the application, and finish the core operate in
 * the interface Operation.
 * @author jiangbing
 *
 */
public class TaskStructClassGene {
	private String className;
	public TaskStructClassGene(String className) {
		this.className = className;
	}
	public String getClassName() {
		return this.className;
	}
	public void setClassName(String className) {
		this.className = className;
	}
	/**
	 * print the help about args.
	 */
	public static void printUsage() {
		System.out.println(
				"Usage: " + " [className] [-i inputPath ...] [-o outputPath ...] "+"\n");
		System.exit(-1);
	}
	
	/**
	 * check the provided classname valid. 
	 * @param className
	 * @return
	 */
	@SuppressWarnings("finally")
	public static boolean  checkClassName(String className) {
		boolean classExist =  true;
		try {
			Class.forName(className);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			classExist = false;
			e.printStackTrace();
		} finally{
			return classExist;
		}
	}
	public static void main(String[] args) throws MalformedURLException, ClassNotFoundException  {
		
		if(args.length < 5 ) {
			System.out.println("args lack!");
			printUsage();
		}
		TaskStructClassGene tscg = new TaskStructClassGene(args[0]);
		
	
		if(! checkClassName(args[0]) ) {
			System.out.println(args[0]);
			printUsage();
		}
		
		try{
			ClassLoader classLoader = TaskStructClassGene.class.getClassLoader();
			Class cls= classLoader.loadClass(tscg.getClassName());
			ArrayList<String> inputArgs = new ArrayList<String> ();
			ArrayList<String> outputArgs = new ArrayList<String> ();
			boolean isInputArgs = false;
			String pathPattern = "(^\\.|^/|^[a-zA-Z])?:?/.+(/$)?";
			int i  = 1;
			while(i < args.length) {
				if(args[i].equals("-i")) {
					isInputArgs = true;
				}
				else if (args[i].equals("-o")) {
					
					isInputArgs = false;
				}
				else {
					if(!args[i].matches(pathPattern)){
						printUsage();
					}
					if(isInputArgs) {
						inputArgs.add(args[i]);
					}
					else {
						outputArgs.add(args[i]);
					}
				}
				i ++;
			}
			if( (inputArgs.size() + outputArgs.size()) == 0) {
				printUsage();
			}
			
			    Method m = cls.getMethod("operate", String[].class, String[].class);
			m.invoke(cls.newInstance(), inputArgs.toArray(new String[0]), 
					outputArgs.toArray(new String[0]));
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
