package temporary;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.hadoop.fs.Path;

import com.transformer.compiler.DataState;

import cn.ict.cacuts.mapreduce.map.KVList;
import cn.ict.cacuts.mapreduce.map.KVPair;

import cn.ict.cacuts.mapreduce.Merger;
/**
 * test the merger.
 * @author jiangbing
 *
 */
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
			merger.merge(inputPath, index, outputPath, false,DataState.LOCAL_FILE);
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int total = 0;
		for (int j = 0; j < 3; j++) {
			FileInputStream fis = new FileInputStream(outputPath[j].toUri().getPath());
			//ByteArrayInputStream bais = new ByteArrayInputStream(fis);
			ObjectInputStream ois = new ObjectInputStream(fis);
			
			Object tmp = null;
			System.out.println("*********************************");
		
			while (null!=(tmp = ois.readObject())){
				System.out.println(((KVList)tmp));
				total += ((KVList)tmp).getValue().size();
				if (fis.available() == 0) {
					break;
				}
			}
			System.out.println(total);
			ois.close();
			fis.close();
		}
	
		String prefixPath1 = "testCase/testMerger/tmpMapOut_output";
		Path[] inputPath1 = new Path[3];
		for (int i = 0; i < inputPath1.length; i++) {
			inputPath1[i] = new Path(prefixPath1 + i);
		}
		Path output1 = new Path("testCase/testMerger/tmpMapOut_output_final");
		merger.merge(inputPath1, output1, false, DataState.LOCAL_FILE, Object.class);
		FileInputStream fis = new FileInputStream(output1.toUri().getPath());
		ObjectInputStream ois = new ObjectInputStream(fis);
		Object tmp = null;
		System.out.println("*********************************");
		total = 0;
		while (null!=(tmp = ois.readObject())){
			System.out.println(((KVList)tmp));
			total += ((KVList)tmp).getValue().size();
			System.out.println(total);
			if (fis.available() == 0) {
				break;
			}
		}
		System.out.println(total);
		ois.close();
		fis.close();
	}
}
