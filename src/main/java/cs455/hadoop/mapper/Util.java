package cs455.hadoop.mapper;

import java.util.ArrayList;
import java.util.List;

public class Util {
	public static List<String> readCSV(String csv) {

//		Reader input = new StringReader(csv);
//		for(CSVRecord record )

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
}
