package edu.msu.cse.eaccf.server;

import java.util.ArrayList;
import java.util.List;


import edu.msu.cse.dkvf.metadata.Metadata.TgTimeItem;

public class Utils {

	static <T> List<T> createList(int n, T val) {
		List<T> ret = new ArrayList<>();
        for (int j = 0; j < n; j++) {
            ret.add(val);
        }
		return ret;
	}

	static <T> List<List<T>> createMatrix(int n, int m, T val) {
		List<List<T>> ret = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			List<T> l = new ArrayList<>();
			for (int j = 0; j < m; j++) {
				l.add(val);
			}
			ret.add(l);
		}
		return ret;
	}


	static void addEntriesAsMatrix(String str, List<List<Integer>> ret) {
		String[] vals = str.split(";");
		for (String entry : vals) {
			List<Integer> l = new ArrayList<>();
			for (String e : entry.split(",")){
				l.add(Integer.valueOf(e.trim()));
			}
			ret.add(l);
		}
	}

	static void addEntriesAsMatrixAsString(String str, List<List<String>> ret) {
		String[] vals = str.split(";");
		for (String entry : vals) {
			List<String> l = new ArrayList<>();
			for (String e : entry.split(",")){
				l.add(e.trim());
			}
			ret.add(l);
		}
	}

	static List<Long> max (List<Long> first, List<Long> second){
		List<Long> result = new ArrayList<>();
		for (int i=0; i < first.size(); i++){ 
			result.set(i, Math.max(first.get(i), second.get(i)));
		}
		return result;
	}
	
	
	static List<Long> min (List<Long> first, List<Long> second){
		List<Long> result = new ArrayList<>();
		for (int i=0; i < first.size(); i++){ 
			result.set(i, Math.min(first.get(i), second.get(i)));
		}
		return result;
	}
	
	static long getPhysicalTime (){
		return System.currentTimeMillis();
	}
	
	
	static long max (List<TgTimeItem> ds){
		long result = ds.get(0).getTime(); 
		for (int i = 1; i < ds.size(); i++)
			if (result < ds.get(i).getTime()) 
				result = ds.get(i).getTime();
		return result;
	}
	
	//HLC operations
	public static long getL(long time) {
		return time & 0xFFFFFFFFFFFF0000L;
	}

	public static long getC(long time) {
		return time & 0x000000000000FFFFL;
	}

	public static long incrementL(long time) {
		return shiftToHighBits(1) + time;
	}


	public static long shiftToHighBits(long time) {
		return time << 16;
	}
	
	public static long maxDsTime(List<TgTimeItem> tgItemList) {
		if (tgItemList == null || tgItemList.isEmpty())
			return 0;
		long result = tgItemList.get(0).getTime();
		for (int i = 1; i < tgItemList.size(); i++)
			if (result < tgItemList.get(i).getTime())
				result = tgItemList.get(i).getTime();
		return result;
	}
}
