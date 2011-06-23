package cn.ict.cacuts.mapreduce.mapcontext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class Sort {

	public ArrayList sort = new ArrayList();
	//public Object[] sortedResult;
	Sort(ArrayList sort){
		this.sort = sort;
	//	beginSort();
	}
	
	public Object[] beginSort(){
		String key;
		
//		for(int i = 0 ; i < sort.size() ; i ++){
//			key = sort.get(i).toString().split(",")[0];
//		//	System.out.println(key);
//		}
		Object[] sortedResult = sort.toArray();
		Arrays.sort(sortedResult);
//		for(int i = 0 ; i < sortedResult.length ; i ++){
//			System.out.println(sortedResult[i]);
//		}
//		
		
		//Map<KEY , VALUE> tt = new Map();
		
		return sortedResult;
	}
	
//	public Object[]  getSortedResult(){
//		return sortedResult;
//	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ArrayList receiveList= new ArrayList();
		receiveList.add("key5 , 5");
		receiveList.add("key1 , 1");
		receiveList.add("cae , 7");
		receiveList.add("key6 , 6");
		receiveList.add("key2 , 2");
		receiveList.add("key8 , 8");
		receiveList.add("good , 4");
		receiveList.add("key3 , 3");
		receiveList.add("key4 , 6");
		
		receiveList.add("key4 , 4");
		receiveList.add("bda , 5");
		
		
		receiveList.add("hello, 5");
		
		Sort tt = new Sort(receiveList);
		
	}

}
