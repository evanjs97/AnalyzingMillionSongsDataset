package cs455.hadoop.util;

import cs455.hadoop.reducer.SixTaskReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.util.hash.Hash;

import java.util.*;

public class Util {

	/**
	 * readCsv method converts csv file to a list of strings, this method correctly handles files that contains commas other than splitters
	 * taken from https://stackoverflow.com/questions/1757065/java-splitting-a-comma-separated-string-but-ignoring-commas-in-quotes
	 * @param csv the csv file to process
	 * @return a List<String> representation of the input file
	 */
	public static List<String> readCSV(String csv) {

		List<String> result = new ArrayList<>();
		int start = 0;
		boolean inQuotes = false;
		for (int current = 0; current < csv.length(); current++) {
			if (csv.charAt(current) == '\"') inQuotes = !inQuotes; // toggle state
			boolean atLastChar = (current == csv.length() - 1);
			if(atLastChar) result.add(csv.substring(start));
			else if (csv.charAt(current) == ',' && !inQuotes) {
				result.add(csv.substring(start, current));
				start = current + 1;
			}
		}
		return result;
	}

	public static AvgDouble[] lineTo1DAvgDoubleArr(String line) {
		return Arrays.stream(line.split(" ")).map(x -> new AvgDouble(Double.parseDouble(x))).toArray(AvgDouble[]	::new);
	}

	public static AvgDouble[][] lineTo2DAvgDoubleArr(String line, int numCols) {
		AvgDouble[] oneD = Arrays.stream(line.split(" ")).map(x -> new AvgDouble(Double.parseDouble(x))).toArray(AvgDouble[]::new);
		AvgDouble[][] twoD = new AvgDouble[oneD.length/numCols][numCols];
		int index = 0;
		for(int i = 0; i < twoD.length; i++) {
			twoD[i] = Arrays.copyOfRange(oneD, index, index+numCols);
			index += numCols;
		}
		return twoD;
	}

	public static String formatList(HashMap<String, Integer> values) {
		StringBuilder builder = new StringBuilder();
		Iterator<Map.Entry<String, Integer>> iter = values.entrySet().iterator();
		while(iter.hasNext()) {
			Map.Entry<String, Integer> pair = iter.next();
			if(pair.getKey().isEmpty()) continue;
			builder.append(pair.getKey());
			builder.append(" ");
			builder.append(pair.getValue());
			if(iter.hasNext()) builder.append(",");
		}
		return builder.toString();
	}

	public static double DoubleOrZero(String val) {
		try{
			return Double.parseDouble(val);
		}catch(NumberFormatException nfe) {
			return 0;
		}
	}

	public static String mostCommonStringInList(List<String> splitKeys) {
		return mostCommonStringInList(splitKeys, 1);
	}

	public static String mostCommonStringInList(List<String> splitKeys, int count) {
		HashMap<String, Integer> keyMap = new HashMap<>();
		TreeMap<MapKey, Integer> treeMap = new TreeMap<>();
		for(String k : splitKeys) {
			if(k.isEmpty()) continue;
			Integer old = keyMap.get(k);
			if(old != null) {
				keyMap.replace(k, old, old+1);
				MapKey mapKey = new MapKey(k, old, true);
				treeMap.remove(mapKey);
				mapKey.setValue(old + 1);
				treeMap.put(mapKey, old+1);
			}else {
				treeMap.put(new MapKey(k, 1, true), 1);
				keyMap.put(k,1);
			}

		}
		StringBuilder output = new StringBuilder();
		for(int i = 0; i < count; i++) {
			output.append(treeMap.pollLastEntry().getKey().getKey());
			if(i < count-1) output.append(", ");
		}
		return output.toString();
	}

	public static void addStringPairToHashMap(String pair, HashMap<String, Integer> map) {
		if(!pair.isEmpty()) {
			String[] keyVal = pair.split(" ");
			if(!keyVal[0].isEmpty()) {
				Integer result = map.getOrDefault(keyVal[0], 0);
				map.put(keyVal[0], result + 1);
			}
		}
	}

}
