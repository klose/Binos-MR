package cn.ict.cacuts.generateDAG.baseelement;

/**
 * This is base operation that can be used.
 * @author jiangbing
 *
 */
public interface Operation {
	public void operate(String[] inputPath, String[] outputPath);
}
