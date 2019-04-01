package cs455.hadoop.reducer;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class SixTaskReducer extends Reducer<Text, Text, Text, NullWritable> {

	private TreeMap<MapKey, String> maxes = new TreeMap<>();

	private String outputHeader = "";
	private char outputType = ' ';
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String keyString = key.toString().substring(1);
		switch (key.charAt(0)) {
			case 'A':
				outputHeader = "Top 10 artists with the most songs:\n";
				outputType = 'A';
				completeTaskOne(keyString,values);
				break;
			case 'B':
				outputHeader = "Top 10 artists with the loudest songs on average:\n";
				outputType = 'B';
				completeTaskTwo(keyString,values);
				break;
			case 'C':
				outputHeader = "Top 10 songs with the highest hotness score:\n";
				outputType = 'C';
				completeBasicTask(keyString,values, true);
				break;
			case 'D':
				outputHeader = "Top 10 artists with the longest total fade time in their songs:\n";
				outputType = 'D';
				completeBasicTask(keyString,values, true);
				break;
			case 'E':
				outputHeader = "Shortest Songs, Longest Songs and Median Length Songs\n";
				outputType = 'E';
				completeBasicTask(keyString,values, true);
				break;
			case 'F':
				outputHeader = "Top 10 most energetic and Danceable Songs:\n";
				outputType = 'F';
				completeTaskSix(keyString,values);
				break;
		}
	}

	class MapKey implements Comparable<MapKey>{
		protected double value;
		protected String key;
		protected boolean increasing;


		public MapKey(String key, double value, boolean increasing) {
			this.value = value;
			this.key = key;
			this.increasing = increasing;
		}

		@Override
		public int compareTo(MapKey o) {
			if(this.value == o.value) return 0;
			else if(increasing) {
				if(this.value < o.value) return -1;
				else return 1;
			}else {
				if(this.value > o.value) return -1;
				else return 1;
			}
		}
	}

	private String getOutputValue() {

		switch (outputType) {
			case 'A':
				Map.Entry<MapKey, String> countEntry = maxes.pollLastEntry();
				return String.format("%s wrote %.0f songs",countEntry.getValue(), countEntry.getKey().value);
			case 'B':
				Map.Entry<MapKey,String> loudEntry = maxes.pollLastEntry();
				return String.format("%s wrote songs with an average loudness of %.2f decibals",loudEntry.getValue(), loudEntry.getKey().value);
			case 'C':
				Map.Entry<MapKey, String> hotEntry = maxes.pollLastEntry();
				return String.format("%s has a hotness score of %.2f.",hotEntry.getValue(), hotEntry.getKey().value);
			case 'D':
				Map.Entry<MapKey, String> fadeEntry = maxes.pollLastEntry();
				return String.format("%s wrote songs with a total fade time of %.2f minutes",fadeEntry.getValue(), fadeEntry.getKey().value / 60);

			default:
				Map.Entry<MapKey, String> danceEntry = maxes.pollLastEntry();
				return String.format("%s has a combined danceability and energy score of %.2f", danceEntry.getValue(), danceEntry.getKey().value);
		}
	}

	private void outputLengths(Context context) throws IOException, InterruptedException{
		int splits = Math.max(Math.min(maxes.size() / 3,5),1);
		if(splits % 2 == 0) splits --;

		context.write(new Text("The top " + splits + " longest songs are:"), NullWritable.get());
		for(int i = 0; i < splits; i++) {
			Map.Entry<MapKey,String> entry;
			if(maxes.size() < 3) entry = maxes.lastEntry();
			else entry = maxes.pollLastEntry();
			context.write(new Text(String.format("%s has a runtime of %.2f minutes.",entry.getValue(), entry.getKey().value / 60)), NullWritable.get());
		}

		context.write(new Text("\nThe top " + splits + " shortest songs are:"), NullWritable.get());
		for(int i = 0; i < splits; i++) {
			Map.Entry<MapKey, String> entry;
			if (maxes.size() < 2) entry = maxes.firstEntry();
			else entry = maxes.pollFirstEntry();
			context.write(new Text(String.format("%s has a runtime of %.2f minutes.",entry.getValue(), entry.getKey().value / 60)), NullWritable.get());
		}

		context.write(new Text("\nThe top " + splits + " median length songs are:"), NullWritable.get());
		Set<Map.Entry<MapKey, String>> entries = maxes.entrySet();
		Iterator<Map.Entry<MapKey,String>> iter = entries.iterator();
		int currIndex = 0;
		int midPoint = entries.size() / 2;
		if(entries.size() % 2 == 0) midPoint -=1;
		int start = midPoint - (splits / 2);
		int end = midPoint + (splits / 2);

		while(iter.hasNext()) {
			Map.Entry<MapKey, String> entry = iter.next();
			if(currIndex >= start && currIndex <= end) {
				context.write(new Text(String.format("%s has a runtime of %.2f minutes.",entry.getValue(), entry.getKey().value / 60)), NullWritable.get());
			}
			currIndex++;
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text(outputHeader), NullWritable.get());
		if(outputType == 'E' && !maxes.isEmpty()) outputLengths(context);
		else {
			for (int i = 0; i < Math.min(10, maxes.size()); i++) {
				context.write(new Text(getOutputValue()), NullWritable.get());
			}
		}
	}


	private void completeTaskOne(String key, Iterable<Text> values) {
		int sum = 0;
		String artist = "";
		for(Text value : values) {
			String entry = value.toString();
			String[] arr = entry.split("\t");
			artist = arr[0];
			sum += Integer.parseInt(arr[1]);
		}
		if(!artist.equals("")) maxes.put(new MapKey(key, sum, true),artist);
	}



	private void completeTaskTwo(String key, Iterable<Text> values) {
		String artist = "";
		double loudness = 0;
		int count = 0;
		for(Text value : values) {
			String entry = value.toString();
			if(entry.charAt(0) == 'N') {
				artist = entry.substring(1);
				count++;
			}else {
				loudness = Double.parseDouble(entry.substring(1));
			}
		}
		if(!artist.equals("") && loudness != 0) maxes.put(new MapKey(key,loudness / count, false), artist);
	}

	private void completeTaskSix(String key, Iterable<Text> values) {
		String name = "";
		double dance = 0;
		double energy = 0;
		for(Text val : values) {
			String entry = val.toString();
			if (entry.charAt(0) == 'N') {
				name = entry.substring(1);
			} else {
				String[] arr = entry.split("\t");
				dance = Double.parseDouble(arr[0]);
				energy = Double.parseDouble(arr[1]);
			}
		}
		if(!name.equals("") && (dance != 0 || energy != 0)) maxes.put(new MapKey(key, dance + energy, true), name);
	}

	private void completeBasicTask(String key, Iterable<Text> values, boolean increasing) {
		String name = "";
		double value = 0;
		for(Text val : values) {
			String entry = val.toString();
			if (entry.charAt(0) == 'N') {
				name = entry.substring(1);
			} else value = Double.parseDouble(entry.substring(1));
		}
		if(!name.equals("") && value != 0) maxes.put(new MapKey(key, value, increasing), name);
	}
}
