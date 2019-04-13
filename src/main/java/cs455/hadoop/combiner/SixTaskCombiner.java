package cs455.hadoop.combiner;

import cs455.hadoop.util.AvgDouble;
import cs455.hadoop.util.Util;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class SixTaskCombiner extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		switch (key.charAt(0)) {
			case 'A':
				String artist = "";
				int sum = 0;
				for (Text val : values) {
					String entry = val.toString();
					String[] arr = entry.split("\t");
					artist = arr[0];
					sum += Integer.parseInt(arr[1]);
				}
				context.write(new Text(key), new Text("N" + artist + "\t" + sum));
				break;
			case 'B':
				for (Text val : values) {  //placeholder
					context.write(new Text(key),val);
				}
				break;
			case 'C':
				for (Text val : values) {  //placeholder
					context.write(new Text(key), new Text(val));
				}
				break;
			case 'D':
				for (Text val : values) {  //placeholder
					context.write(new Text(key), new Text(val));
				}
				break;
			case 'E':
				for (Text val : values) {  //placeholder
					context.write(new Text(key), new Text(val));
				}
				break;
			case 'F':
				for (Text val : values) {  //placeholder
					context.write(new Text(key), new Text(val));
				}
				break;
			case 'G':
				String idArtist = "";
				int sumID = 0;
				for (Text val : values) {
					String entry = val.toString();
					String[] arr = entry.split("\t");
					if (arr[0].charAt(0) == 'N') idArtist = arr[0];
					else sumID += Integer.parseInt(arr[1]);
				}
				if (idArtist.equals("")) idArtist = "E";
				context.write(new Text(key), new Text(idArtist + "\t" + sumID));
				break;
			case 'H':
				hotnessTask(values, context, key);
				break;
		}
	}


	/**
	 *
	 * Input Order in values = duration, fade_in, key, loudness, mode, fade_out, tempo, time_signature
	 * @param values
	 * @param context
	 * @param key
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void hotnessTask(Iterable<Text> values, Context context, Text key) throws IOException, InterruptedException {
		AvgDouble duration = new AvgDouble();
		AvgDouble loudness = new AvgDouble();
		AvgDouble tempo = new AvgDouble();
		AvgDouble fadeIn = new AvgDouble();
		AvgDouble fadeOut = new AvgDouble();

		HashMap<String, Integer> keys = new HashMap<>();
		HashMap<String, Integer> modes = new HashMap<>();
		HashMap<String, Integer> timeSigs = new HashMap<>();
		HashMap<String, Integer> keywords = new HashMap<>();
//		LinkedList<String> keys = new LinkedList<>();
//		LinkedList<String> modes = new LinkedList<>();
//		LinkedList<String> timeSigs = new LinkedList<>();

		for(Text val : values) {
			String entry = val.toString();
			if(entry.charAt(0) == 'N') {
				for(String s : entry.substring(1).split(",")) {
					String[] keyVal = s.split(" ");
					if(keyVal[0].isEmpty()) continue;
					Integer result = keywords.get(keyVal[0]);
					if(result == null) result = 0;
					keywords.put(keyVal[0], 1 + result);
				}
			}
			else {
				String[] arr = entry.substring(1).split("\t", -1);
				double temp = Util.DoubleOrZero(arr[0]);
				if(temp != 0) {
					duration.add(temp);
				}
				temp = Util.DoubleOrZero(arr[1]);
				if(temp != 0) {
					fadeIn.add(temp);
				}
				temp = Util.DoubleOrZero(arr[3]);
				if(temp != 0) {
					loudness.add(temp);
				}
				temp = Util.DoubleOrZero(arr[5]);
				if(temp != 0) {
					fadeOut.add(temp);
				}
				temp = Util.DoubleOrZero(arr[6]);
				if(temp != 0) {
					tempo.add(temp);
				}

				if(!arr[2].isEmpty()) {
					String[] keyVal = arr[2].split(" ");
					if(!keyVal[0].isEmpty()) {
						Integer result = keys.get(keyVal[0]);
						if (result == null) result = 0;
						keys.put(keyVal[0], result + 1);
					}
				}
				if(!arr[4].isEmpty()) {
					String[] keyVal = arr[4].split(" ");
					if(!keyVal[0].isEmpty()) {
						Integer result = modes.get(keyVal[0]);
						if (result == null) result = 0;
						modes.put(keyVal[0], result + 1);
					}
				}
				if(!arr[7].isEmpty()) {
					String[] keyVal = arr[7].split(" ");
					if(!keyVal[0].isEmpty()) {
						Integer result = timeSigs.get(keyVal[0]);
						if (result == null) result = 0;
						timeSigs.put(keyVal[0], result + 1);
					}
				}


			}
		}
		String keywordOut = Util.formatList(keywords);
		if(!keywordOut.isEmpty()) context.write(new Text(key), new Text("N"+keywordOut));

		context.write(new Text(key), new Text("C"+duration.preserveCountString() + "\t"
				+ fadeIn.preserveCountString() + "\t" + Util.formatList(keys)+ "\t" + loudness.preserveCountString()
				+ "\t" + Util.formatList(modes) + "\t" + fadeOut.preserveCountString() + "\t" + tempo + "\t" + Util.formatList(timeSigs)));

	}

//	private String (HashMap<String, Integer> keywords) {
//		StringBuilder keyWordBuilder = new StringBuilder();
//		Iterator<Map.Entry<String, Integer>> keywordIter = keywords.entrySet().iterator();
//		while(keywordIter.hasNext()) {
//			Map.Entry<String, Integer> pair = keywordIter.next();
//			keyWordBuilder.append(pair.getKey());
//			keyWordBuilder.append(" ");
//			keyWordBuilder.append(pair.getValue());
//			if(keywordIter.hasNext()) keyWordBuilder.append(",");
//		}
//	}
}
