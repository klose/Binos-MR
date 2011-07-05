package temporary;

import cn.ict.cacuts.mapreduce.operation.SplitOperation;

public class testSplitOperation {
	public static void main(String [] args) {
		SplitOperation so = new SplitOperation();
		String[] input = {"input"};
		String[] output = {"0","1"};
		so.operate(input, output);
	}
}
