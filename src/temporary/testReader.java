package temporary;

import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntPar;
import cn.ict.cacuts.mapreduce.ReadFromDataBus;

public class testReader {
	public static void main(String[] args) {
		ReadFromDataBus reader = new ReadFromDataBus("MSG://test-reader");
		long start = System.currentTimeMillis();
		KVPairIntPar tmp = reader.getOneKVPairIntPar();
		System.out.println("used:" + (System.currentTimeMillis() - start) + "ms");
		while ((tmp = reader.getOneKVPairIntPar()) != null) {
			
		}
		reader.close();
		System.out.println("used:" + (System.currentTimeMillis() - start) + "ms");
	}
}
