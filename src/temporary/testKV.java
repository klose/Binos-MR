package temporary;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import cn.ict.cacuts.mapreduce.KeyValue.KVPairInt;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntData;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairIntList;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairString;
import cn.ict.cacuts.mapreduce.KeyValue.KVPairStringData;
import cn.ict.cacuts.mapreduce.map.SortStructedData;

public class testKV {
	public static void test() throws IOException {
		KVPairInt pair = KVPairInt.newBuilder().setKey("abcd").setValue(1000).build();
		KVPairInt pair1 = KVPairInt.newBuilder().setKey("ab").setValue(1200).build();
//		pair.t
		
		KVPairIntData data;
		KVPairIntList data1 = KVPairIntList.newBuilder().setKey("abcd").
				addVlist(1000).addVlist(2000).build();
		KVPairIntList data2 = KVPairIntList.newBuilder().setKey("abcd").
				addVlist(1000).addVlist(2000).build();
	
		System.out.println(data2.hashCode());
		KVPairIntList.Builder builder = KVPairIntList.newBuilder();
		builder.setKey(data1.getKey()).addAllVlist(data1.getVlistList()).addAllVlist(data2.getVlistList());
		System.out.println(builder.isInitialized());
		
		System.out.println(builder.build().toString());
		if (builder == null)
			System.out.println("ok");
		System.out.println(builder.toString());
		System.out.println(builder.getVlistCount());
		//System.out.println(builder.isInitialized());
		
	}
		/*
		KVPairIntData.Builder builder = KVPairIntData.newBuilder();
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			 //builder.setKvset(i, pair);
			 builder.addKvset(pair);
			 builder.addKvset(pair1);
		}
		KVPairInt[] ss = builder.getKvsetList().toArray(new KVPairInt[0]);
		Arrays.sort(ss, SortStructedData.getComparator());
//		for (int i = 0; i < 2000; i++) {
//			System.out.println(builder.getKvset(i).toString());
//		}
		FileOutputStream fos = new FileOutputStream("/tmp/abcd1", false);
//		for (int i = 0; i < 200000; i++) {
//			//System.out.println(ss[i].toString());
//			ss[i].writeTo(fos);
//		}
		builder.clear();
		System.out.println(builder.getKvsetCount());
		//builder.addAllKvset(ss);
		builder.build().writeTo(fos);
		System.out.println("System used "+ (System.currentTimeMillis() - start) + "ms" );
		//builder.clearKvset();
		
	
		
	}
		
		// once builder.build(), the builder must be reconstructed.
//		try {
//			FileOutputStream fos = new FileOutputStream("/tmp/abcd1", false);
//			
//			builder.clear();
//			
//			//builder.getKvsetCount()
//			
//			System.out.println(builder.getKvsetCount());
//			data = builder.build();
//			data.writeTo(fos);
////			builder = KVPairStringData.newBuilder();
////			for (int i = 0; i < 10000000; i++) {
////				 //builder.setKvset(i, pair);
////				 builder.addKvset(pair);
////			}
//			
//			
//			//data = builder.build();
//			//data.writeTo(fos);
//			fos.close();
//			data = null;
//			builder = null;
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}finally{
//			System.gc();
//		}
//		System.out.println("write use" + (System.currentTimeMillis() - start) + "ms");
//	}*/
	public static void main(String[] args) {
	
		try {
			test();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
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
		
		
		
	}
	
}
