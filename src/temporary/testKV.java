package temporary;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;


import cn.ict.cacuts.mapreduce.KeyValue.KVPairInt;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntData;

public class testKV {
	public static void test() {
		KVPairInt pair = KVPairInt.newBuilder().setKey("abcd").setValue(1000).build();
		
		KVPairIntData data;
		
		KVPairIntData.Builder builder = KVPairIntData.newBuilder();
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10000000; i++) {
			 //builder.setKvset(i, pair);
			 builder.addKvset(pair);
		}
		data = builder.build();
		
		try {
			FileOutputStream fos = new FileOutputStream("/tmp/abcd", true);
			data.writeTo(fos);
			fos.close();
			data = null;
			builder = null;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			System.gc();
		}
		System.out.println("write use" + (System.currentTimeMillis() - start) + "ms");
	}
	public static void main(String[] args) {
	
		test();
		
		//System.runFinalization();
		
		//System.gc();
//		builder = builder.clearKvset();
		
		while(true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
//		KVPairData.Builder rebuilder = KVPairData.newBuilder(); 
//		try {
//			rebuilder = rebuilder.mergeFrom(new FileInputStream("/tmp/abcd"));
//			List<KVPair> list = rebuilder.getKvsetList();
////			for (KVPair tmp: list) {
////				//System.out.println(tmp.getKey() + ":" + tmp.getValue());
////			}
//			System.out.println(list.size());
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
		
		
	}
	
}
