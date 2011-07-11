package cn.ict.cacuts.test;

import cn.ict.cacuts.mapreduce.MRConfig;
import cn.ict.cacuts.mapreduce.MapContext;
import cn.ict.cacuts.mapreduce.Mapper;
import cn.ict.cacuts.mapreduce.Reducer;
import cn.ict.cacuts.mapreduce.reduce.ReduceContext;
import cn.ict.cacuts.userinterface.MRJob;

public class WordCountTest {

	public static class TokenizerMapper extends Mapper<Object, Object> {
		@Override
		public void map(String line, MapContext<Object, Object> context) {
			// TODO Auto-generated method stub
			System.out.println("hello , i am in the map");
		}
	}

	public static class IntSumReducer extends
			Reducer<Object, Object, Object, Object> {
		@Override
		public void reduce(Object key, Iterable<Object> values,
				ReduceContext context) {
			// TODO Auto-generated method stub
			System.out.println("hello , i am in the reduce");
		}
	}

	public static void main(String[] args) throws Exception {
		MRConfig conf = new MRConfig();
		// String[] otherArgs = new GenericOptionsParser(conf,
		// args).getRemainingArgs();
		// if (otherArgs.length != 2) {
		// System.err.println("Usage: wordcount <in> <out>");
		// System.exit(2);
		// }
		String[] inputFileName = {};
		String[] outputFileName = {};
		MRJob job = new MRJob(conf, "word count");
		job.setInputFileName(inputFileName);
		job.setOutputFileName(outputFileName);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.submit();
	}
}
