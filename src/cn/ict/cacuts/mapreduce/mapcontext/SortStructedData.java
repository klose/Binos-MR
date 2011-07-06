package cn.ict.cacuts.mapreduce.mapcontext;

import java.util.ArrayList;
import java.util.Arrays;

public class SortStructedData {
	public static java.util.Comparator getComparator() {
		return new java.util.Comparator() {

			public int compare(Object o1, Object o2) {
				if (o1 instanceof String) {
					return compare((String) o1, (String) o2);
				} else if (o1 instanceof Integer) {
					return compare((Integer) o1, (Integer) o2);
				} else if (o1 instanceof Long) {
					return compare((Long) o1, (Long) o2);
				}

				else if (o1 instanceof KVPair) {
					return compare((KVPair) o1, (KVPair) o2);
				} else {
					System.err
							.println("have not find the coresonsed comparator");
					return 1;
				}
			}

			public int compare(String o1, String o2) {
				String s1 = (String) o1;
				String s2 = (String) o2;
				int len1 = s1.length();
				int len2 = s2.length();
				int n = Math.min(len1, len2);
				char v1[] = s1.toCharArray();
				char v2[] = s2.toCharArray();
				int pos = 0;

				while (n-- != 0) {
					char c1 = v1[pos];
					char c2 = v2[pos];
					if (c1 != c2) {
						return c1 - c2;
					}
					pos++;
				}
				return len1 - len2;
			}

			public int compare(Integer o1, Integer o2) {
				int val1 = o1.intValue();
				int val2 = o2.intValue();
				return (val1 < val2 ? -1 : (val1 == val2 ? 0 : 1));

			}

			public int compare(Long o1, Long o2) {
				Long val1 = o1.longValue();
				Long val2 = o1.longValue();
				return (val1 < val2 ? -1 : (val1 == val2 ? 0 : 1));

			}

			public int compare(Boolean o1, Boolean o2) {

				return (o1.equals(o2) ? 0
						: (o1.booleanValue() == true ? 1 : -1));

			}

			/**
			 * DECLARE : here the key type is not "KEY" but "Object "
			 * */
			public int compare(KVPair o1, KVPair o2) {
				int partionNum1 = o1.getpartitionNum();
				int partionNum2 = o2.getpartitionNum();
				Object key1 = o1.getKey();
				Object key2 = o2.getKey();
				return (compare(partionNum1, partionNum2) == 0 ? (compare(key1,
						key2) == 0 ? 0 : compare(key1, key2)) : compare(
						partionNum1, partionNum2));
			}

		};
	}

	public static void main(String[] args) {
		KVPair[] KVPair = new KVPair[] {
				new KVPair("ouyang", 1, 3), new KVPair("zhuang", 2, 1),
				new KVPair("aaaaaaaaaa", 3, 2),
				new KVPair("cccccccccccc", 4, 1),
				new KVPair("cccccccccccc", 4, 2) };
		ArrayList tt = new ArrayList();
		tt.add(new KVPair("ouyang", 1, 3));
		tt.add(new KVPair("cc", 1, 1));
		tt.add(new KVPair("cc", 1, 3));

//		Arrays.sort(KVPair, SortStructedData.getComparator());
//
//		for (int i = 0; i < KVPair.length; i++) {
//			System.out.println("after sort    PartionNum   ="
//					+ KVPair[i].getPartionNum());
//			System.out.println("after sort   key= " + KVPair[i].getKey());
//		}

		Object[]  ss = tt.toArray();
		Arrays.sort(ss, SortStructedData.getComparator());

//		for (int i = 0; i < tt.size(); i++) {
//			System.out.println("after sort    PartionNum   ="
//					+ ((KVPair) tt.get(i)).getPartionNum());
//			System.out.println("after sort   key= " + ((KVPair) tt.get(i)).getKey());
//		}
		for (int i = 0; i < ss.length; i++) {
		System.out.println("after sort    PartionNum   ="
				+ ((KVPair) ss[i]).getpartitionNum());
		System.out.println("after sort   key= " + ((KVPair) ss[i]).getKey());
	}
	}
}
