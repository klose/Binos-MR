package cn.ict.cacuts.mapreduce.compiler;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * 
 * @author jiangbing
 *
 */
public class JarCreator {
	private String path; //specify the directory that needs to be packaged to jar. 
	private String mainClass; // specify the main-class 
	private String jarPath; //specify the path of the jar generated.
	
	/**
	 * @description construct the CreateJar Object. 
	 * @param path:specify the directory that needs to be packaged to jar
	 * @param mainClass: specify the main-class in the directory
	 * @param jarPath: specify the path of the jar generated. eg. /tmp/1_2_4.jar
	 */
	public JarCreator(String path, String mainClass,  String jarPath) {
		this.path = path;
		this.mainClass = mainClass;
		this.jarPath = jarPath;
	}
	
	public File createJar() throws IOException
	{
		if(!new File(this.path).exists()){
			System.err.println("The path:"+path + " doesn't exist!");
			return null;
		}
		Manifest manifest = new Manifest();
		manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
		manifest.getMainAttributes().putValue("Main-Class",this.mainClass);
		manifest.getMainAttributes().putValue("Created-By", "Transformer");
		//final File jarFile = File.createTempFile(this.jarName, ".jar",new File(this.jarPath));
		
		//JarFile jarFile = new JarFile(this.jarName+".jar");
		File jarFile = new File(this.jarPath);
		JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile),manifest);
		createJarInner(jos,new File(this.path),"");
		jos.flush();
		jos.close();
		return jarFile;
	}
	
	private void createJarInner(JarOutputStream jos,File f,String base) throws IOException {
		if(f.isDirectory()){
			File[] fl = f.listFiles();
			if(base.length()>0){
				base = base + "/";
			}
			for(int i = 0;i<fl.length;i++){
				createJarInner(jos,fl[i],base+fl[i].getName());
			}
		}
		else{
			jos.putNextEntry(new JarEntry(base));
			FileInputStream in = new FileInputStream(f);
			byte[] buffer = new byte[1024];
			int n = in.read(buffer);
			while(n != -1){
				jos.write(buffer, 0, n);
				n = in.read(buffer);
			}
			in.close();
		}
	}
	
	
}
