package cn.ict.cacuts.mapreduce.mapcontext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

public class Sort {

	public ArrayList sort = new ArrayList();
	public Sort(ArrayList sort){
		this.sort = sort;
	}
	
	public Object[] beginSort(){
		ArrayList sorted = new ArrayList();
		String key;
		Object[] sortedResult = sort.toArray();
		System.out.println("sort.length ： " + sort.size());
		System.out.println("sortedResult.length ： " + sortedResult.length);
		Arrays.sort(sortedResult);
		//System.out.println("finished sort  " );
		for(int i = 0 ; i < sortedResult.length ; i ++){
			sorted.add(sortedResult[i]);
		}
		//System.out.println(" finished add " );
		return sortedResult;
	}
	
	public ArrayList beginSort1(){
		ArrayList sorted = new ArrayList();
		String key;
		Object[] sortedResult = sort.toArray();
		Arrays.sort(sortedResult);
		
		for(int i = 0 ; i < sortedResult.length ; i ++){
			sorted.add(sortedResult[i]);
		}
		
		return sorted;
	}
	
	/**
	 * @param args
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
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
