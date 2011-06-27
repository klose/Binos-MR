package cn.ict.cacuts.mapreduce.compiler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;



/**
 * JarResolver: unjar a jar file.
 * @author jiangbing
 *
 */
public class JarResolver {
	/** Pattern that matches any string */
	private static final Pattern MATCH_ANY = Pattern.compile(".*");
	private String jarPath;
	private String toPath;
	public JarResolver(String jarPath, String toPath) {
		this.jarPath = jarPath;
		this.toPath =  toPath;
	}
	
	/**
	   * Unpack matching files from a jar. Entries inside the jar that do
	   * not match the given pattern will be skipped.
	   *
	   * @param jarFile the .jar file to unpack
	   * @param toDir the destination directory into which to unpack the jar
	   * @param unpackRegex the pattern to match jar entries against
	   */
	  public void unJar()
	    throws IOException {
		  File jarFile = new File(this.jarPath);
		File toDir = new File(this.toPath);
		 JarFile jar = new JarFile(jarFile);
	    try {
	      Enumeration<JarEntry> entries = jar.entries();
	      while (entries.hasMoreElements()) {
	        JarEntry entry = (JarEntry)entries.nextElement();
	        if (!entry.isDirectory() &&
	            this.MATCH_ANY.matcher(entry.getName()).matches()) {
	          InputStream in = jar.getInputStream(entry);
	          try {
	            File file = new File(toDir, entry.getName());
	            ensureDirectory(file.getParentFile());
	            OutputStream out = new FileOutputStream(file);
	            try {
	    	      PrintStream ps = out instanceof PrintStream ? (PrintStream)out : null;
	              byte buf[] = new byte[8192];
	              int bytesRead = in.read(buf);
	              while (bytesRead >= 0) {
	            	
	            	out.write(buf, 0, bytesRead);
	                if ((ps != null) && ps.checkError()) {
	                  throw new IOException("Unable to write to output stream.");
	                }
	                bytesRead = in.read(buf);
	              
	              }
	             }finally {
	              out.close();
	            }
	          } finally {
	            in.close();
	          }
	        }
	      }
	    } finally {
	      jar.close();
	    }
	  }
	  /**
	   * Ensure the existence of a given directory.
	   *
	   * @throws IOException if it cannot be created and does not already exist
	   */
	  private void ensureDirectory(File dir) throws IOException {
	    if (!dir.mkdirs() && !dir.isDirectory()) {
	      throw new IOException("Mkdirs failed to create " +
	                            dir.toString());
	    }
	  }
}
