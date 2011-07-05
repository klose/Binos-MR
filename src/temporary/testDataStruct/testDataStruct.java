package temporary.testDataStruct;
import cn.ict.cacuts.mapreduce.mapcontext.*;
import java.util.ArrayList;



public class testDataStruct {
	public static void main(String [] args) {
		ArrayList ai = new ArrayList();
		DataStruct<String, Integer> ds = new DataStruct<String, Integer>("aaaa",1, 1);
		ai.add(ds);
		
		System.out.println(ds.toString());
	}
}
