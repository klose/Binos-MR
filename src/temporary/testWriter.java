package temporary;

import cn.ict.cacuts.mapreduce.WriteIntoDataBus;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntPar;

public class testWriter {
	public static void main(String[] args) {
		WriteIntoDataBus writer = new WriteIntoDataBus("MSG://test-reader");
		System.out.println(writer.dataState.toString());
		KVPairIntPar.Builder builder;
		long start = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++) {
			builder = KVPairIntPar.newBuilder().setKey(String.valueOf(i)).setValue(i).setPartition(i);
			writer.appendKVPairIntPar(builder.build());
		}
		writer.close();
		System.out.println("used:" + (System.currentTimeMillis() - start) + "ms");
	}
}
