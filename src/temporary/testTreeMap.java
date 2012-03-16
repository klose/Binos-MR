package temporary;



import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class testTreeMap {
	public static void main(String[] args) {
		Map<String, Integer> sortedMap = new TreeMap<String, Integer>();
		sortedMap.put("abcd", 11);
		sortedMap.put("ax", 12);
		sortedMap.put("2x", 13);
		sortedMap.put("a", 14);
		sortedMap.put("abcd", 11);
		sortedMap.put("ax", 12);
		sortedMap.put("2x", 13);
		sortedMap.put("a", 14);
		//Set<String> set = sortedMap.keySet();
		List<String> keyList = new ArrayList();
		keyList.addAll(sortedMap.keySet());
		
		for (String tmp:keyList) 
			System.out.println(tmp + " " + sortedMap.get(tmp));
		for (Integer tmp: sortedMap.values()) {
			System.out.println(tmp);
		}
	}
}
