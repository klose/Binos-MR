package temporary;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.hadoop.fs.Path;

import cn.ict.cacuts.mapreduce.mapcontext.KVPair;

import cn.ict.cacuts.mapreduce.Merger;

public class testMerger {
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		Merger merger = new Merger();
		String prefixPath = "testCase/testMerger/tmpMapOut_";
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
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		for (int j = 0; j < 3; j++) {
			FileInputStream fis = new FileInputStream(outputPath[j].toUri().getPath());
			//ByteArrayInputStream bais = new ByteArrayInputStream(fis);
			ObjectInputStream ois = new ObjectInputStream(fis);
			
			Object tmp = null;
			System.out.println("*********************************");
			while (null!=(tmp = ois.readObject())){
				System.out.println((KVPair)tmp);
				if (fis.available() == 0) {
					break;
				}
			}
				
			ois.close();
			fis.close();
		}
	
		
	}
}
