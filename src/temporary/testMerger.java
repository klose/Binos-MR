package temporary;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import cn.ict.cacuts.mapreduce.Merger;

public class testMerger {
	public static void main(String[] args) {
		Merger merger = new Merger();
		String prefixPath = "/tmp/CactusTest/tmpMapOut_";
		Path [] inputPath  = new Path[5];
		for (int i = 0; i < inputPath.length; i++) {
			inputPath[i] = new Path(prefixPath + (i+1));
		}
		Path[] outputPath = new Path[3];
		for (int i = 0; i < outputPath.length; i++) {
			outputPath[i] = new Path(prefixPath+"output" +i);
		}
		String index[] = {"33,51,16", "33,50,17", "34,49,17", "33,51,16", "33,50,17"};
		try {
			merger.merge(inputPath, index, outputPath, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
