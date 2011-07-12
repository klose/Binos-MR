package temporary;

import cn.ict.cacuts.mapreduce.MRConfig;
import cn.ict.cacuts.mapreduce.MapContext;
import cn.ict.cacuts.mapreduce.Mapper;
import cn.ict.cacuts.mapreduce.operation.MapOperation;


class testMapper extends Mapper<String, Integer>{
	
	
	
	//int i = 0 ;
	
	 public testMapper() {
		 super();
	 }
	@Override
	public void map(String line, MapContext context) {
		// TODO Auto-generated method stub
 
		String [] word = line.split(" ");
		for (String tmp: word) {
			//System.out.println(tmp);
			//i++;
			context.output(tmp, 1);
			
		}
		//System.out.println("hello i :  " + i);
	}
	 
}
public class testMapOperation {
	public static void main(String [] args) {
		String inputPath[] = {"0"};
		String outputPath[] = {"/tmp/testmapout0", "/tmp/testmapoutput1"};
//		MRConfig.setMapClass(testMapper.class);
//		MRConfig.setReduceTaskNum(2);
		new MapOperation().operate(inputPath, outputPath);
	}
}
