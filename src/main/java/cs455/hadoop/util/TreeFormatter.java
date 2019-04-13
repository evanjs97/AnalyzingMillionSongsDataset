package cs455.hadoop.util;

import java.util.*;

public class TreeFormatter {
	private TreeMap<MapKey, String> maxes = new TreeMap<>();
	private HashMap<String, Double> valueAgg = new HashMap<>();

	public void addKey(String key, double value, String name) {
		Double val = valueAgg.get(key);
		MapKey replace;
		if(val != null)  {
			replace = new MapKey(key, val, true);
			maxes.remove(replace);
			val += value;
			replace.setValue(val);
		}else {
			replace = new MapKey(key, value, true);
			val = value;
		}
		maxes.put(replace, name);
		valueAgg.put(key, val);
	}

	public void addKey(String key, double value) {
		addKey(key, value, key);
	}

	public boolean isEmpty() {
		return maxes.isEmpty();
	}

	public void addAll(Collection<String> collection, String regex) {
		for(String vals : collection) {
			String[] arr = vals.split(regex);
			if(arr.length != 2) continue;
			addKey(arr[0],Double.parseDouble(arr[1]));
		}
	}

	public String getMaxKeyValue() {
		return maxes.pollLastEntry().getValue();
	}

	public String getMaxNKeyValues(int count) {
		StringBuilder builder = new StringBuilder();
		for(int i = 0; i < count; i++) {
			builder.append(getMaxKeyValue());
			if(i < count-1) builder.append(", ");
		}
		return builder.toString();
	}

}
