package cn.ict.cacuts.mapreduce.compiler;

/**
 * This is base operation that can be used.
 * @author jiangbing
 *
 */
public interface Operation {
	public void operate(String[] inputPath, String[] outputPath);
}
