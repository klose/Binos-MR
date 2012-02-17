package cn.ict.cacuts.test;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import cn.ict.cacuts.mapreduce.MRConfig;
import cn.ict.cacuts.mapreduce.MapContext;
import cn.ict.cacuts.mapreduce.Mapper;
import cn.ict.cacuts.mapreduce.Reducer;
import cn.ict.cacuts.mapreduce.reduce.ReduceContext;
import cn.ict.cacuts.userinterface.MRJob;

public class WordCountTest {
	public static class TokenizerMapper extends Mapper<String, Integer> {
		String word = new String();
		Integer one = new Integer(1);
		@Override
		public void map(String line, MapContext<String, Integer> context) {
			// TODO Auto-generated method stub
			StringTokenizer itr = new StringTokenizer(line.toString());
		      while (itr.hasMoreTokens()) {
		        word = itr.nextToken();
		        context.output(word, one);
		      }
		}
	}

	public static class IntSumReducer extends
			Reducer<String, Integer, String, Integer> {
		@Override
		public void reduce(String key, Iterable<Integer> values,
				ReduceContext context) {
			// TODO Auto-generated method stub
			int sum = 0;
			Iterator<Integer> iter = values.iterator();
			while (iter.hasNext()) {
				sum += iter.next().intValue();
			}
			context.output(key, new Integer(sum));
			
		}
	}

	public static void main(String[] args) throws Exception {
		MRConfig conf = new MRConfig("wordcount");
		String inputFileName[] = {"input"};
		String outputFileName[] = {"output1", "output2", "output3", "output4"};
		MRJob job = new MRJob(conf, "wordcount");
		job.setInputFileName(inputFileName);
		job.setOutputFileName(outputFileName);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(4);
		job.submit();
	}
}
