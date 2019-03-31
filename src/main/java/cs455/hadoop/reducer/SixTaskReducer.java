package cs455.hadoop.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class SixTaskReducer extends Reducer<Text, Text, Text, NullWritable> {
	private TreeMap<Integer, String> maxCount = new TreeMap<>();
	private TreeMap<Double, String> maxAvgLoudness = new TreeMap<>();
	private TreeMap<Double, String> maxHotness = new TreeMap<>();
	private TreeMap<Double, String> maxFade = new TreeMap<>();
	private TreeMap<Double, String> lengths = new TreeMap<>();
	private TreeMap<Double, String> maxDance = new TreeMap<>();

	private String outputHeader = "";
	private char outputType = ' ';
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//		for(Text text : values) {
//			context.write(new Text(key.toString() + "\t" + text.toString()), NullWritable.get());
//		}
		switch (key.charAt(0)) {
			case 'A':
				outputHeader = "Top 10 artists with the most songs:\n";
				outputType = 'A';
				completeTaskOne(values);
				break;
			case 'B':
				outputHeader = "Top 10 artists with the loudest songs on average:\n";
				outputType = 'B';
				completeTaskTwo(values);
				break;
			case 'C':
				outputHeader = "Top 10 songs with the highest hotness score:\n";
				outputType = 'C';
				completeTaskThree(values);
				break;
			case 'D':
				outputHeader = "Top 10 artists with the longest total fade time in their songs:\n";
				outputType = 'D';
				completeTaskFour(values);
				break;
			case 'E':
				outputHeader = "Shortest Songs, Longest Songs and Median Length Songs\n";
				outputType = 'E';
				completeTaskFive(values);
				break;
			case 'F':
				outputHeader = "Top 10 most energetic and Danceable Songs:\n";
				outputType = 'F';
				completeTaskSix(values);
				break;
		}
	}

	private String formatLength(String song, Double length) {
		return song + " has a runtime of " + length + " minutes.";
	}

	private String getOutputValue() {

		switch (outputType) {
			case 'A':
				Map.Entry<Integer, String> countEntry = maxCount.pollLastEntry();
				return countEntry.getValue() +" wrote " + countEntry.getKey() + "songs";
			case 'B':
				Map.Entry<Double,String> loudEntry = maxAvgLoudness.pollFirstEntry();
				return loudEntry.getValue() + " wrote songs with an average loudness of " + loudEntry.getKey();
			case 'C':
				Map.Entry<Double, String> hotEntry = maxHotness.pollLastEntry();
				return hotEntry.getValue() + " has a hotness score of " + hotEntry.getKey();
			case 'D':
				Map.Entry<Double, String> fadeEntry = maxFade.pollLastEntry();
				return fadeEntry.getValue() + " wrote songs with a total fade time of " + fadeEntry.getKey();

			default:
				Map.Entry<Double, String> danceEntry = maxDance.pollLastEntry();
				return danceEntry.getValue() + " has a combined danceability and energy score of " + danceEntry.getKey();
		}
	}

	private int getMapLength() {
		switch (outputType) {
			case 'A':
				return maxCount.size();
			case 'B':
				return maxAvgLoudness.size();
			case 'C':
				return maxHotness.size();
			case 'D':
				return maxFade.size();
			case 'E':
				return lengths.size();
			default:
				return maxDance.size();
		}


	}


	private void outputLengths(Context context) throws IOException, InterruptedException{
		int splits = Math.max(Math.min(lengths.size() / 3,5),1);
		if(splits % 2 == 0) splits --;

		context.write(new Text("The top " + splits + " longest songs are:"), NullWritable.get());
		for(int i = 0; i < splits; i++) {
			Map.Entry<Double,String> entry;
			if(lengths.size() < 3) entry = lengths.lastEntry();
			else entry = lengths.pollLastEntry();
			context.write(new Text(entry.getValue() + " has a runtime of " + entry.getKey() + " minutes."), NullWritable.get());
		}

		context.write(new Text("\nThe top " + splits + " shortest songs are:"), NullWritable.get());
		for(int i = 0; i < splits; i++) {
			Map.Entry<Double, String> entry;
			if (lengths.size() < 2) entry = lengths.firstEntry();
			else entry = lengths.pollFirstEntry();
			context.write(new Text(entry.getValue() + " has a runtime of " + entry.getKey() + " minutes."), NullWritable.get());
		}

		context.write(new Text("\nThe top " + splits + " median length songs are:"), NullWritable.get());
		Set<Map.Entry<Double, String>> entries = lengths.entrySet();
		Iterator<Map.Entry<Double,String>> iter = entries.iterator();
		int currIndex = 0;
		int midPoint = entries.size() / 2;
		int start = midPoint - (splits / 2);
		int end = midPoint + (splits / 2);
		if(entries.size() % 2 == 0) midPoint -=1;

		while(iter.hasNext()) {
			Map.Entry<Double, String> entry = iter.next();
			if(currIndex == midPoint && entries.size() % 2 == 0) {
				context.write(new Text(entry.getValue() + " has a runtime of " + entry.getKey() + " minutes."), NullWritable.get());
			}else if(currIndex >= start && currIndex <= end) {
				context.write(new Text(entry.getValue() + " has a runtime of " + entry.getKey() + " minutes."), NullWritable.get());
			}
			currIndex++;
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text(outputHeader), NullWritable.get());
		if(outputType == 'E' && !lengths.isEmpty()) outputLengths(context);
		else {
			for (int i = 0; i < Math.min(10, getMapLength()); i++) {
				context.write(new Text(getOutputValue()), NullWritable.get());
			}
		}
	}


	private void completeTaskOne(Iterable<Text> values) {
		int sum = 0;
		String artist = "";
		for(Text value : values) {
			String entry = value.toString();
			String[] arr = entry.split("\t");
			artist = arr[0];
			sum += Integer.parseInt(arr[1]);
		}
		if(!artist.equals("")) maxCount.put(sum, artist);
	}

	private void completeTaskTwo(Iterable<Text> values) {
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
		if(!artist.equals("") && loudness != 0) maxAvgLoudness.put(loudness / count, artist);
	}

	private void completeTaskThree(Iterable<Text> values) {
		String song = "";
		double hotness = 0;
		for(Text value : values) {
			String entry = value.toString();
			if(entry.charAt(0) == 'N') {
				song = entry.substring(1);
			}else hotness = Double.parseDouble(entry.substring(1));
		}
		if(hotness != 0 && !song.equals("")) maxHotness.put(hotness, song);

	}

	private void completeTaskFour(Iterable<Text> values) {
		String artist = "";
		double fadeDuration = 0;
		for (Text value : values) {
			String entry = value.toString();
			if (entry.charAt(0) == 'N') {
				artist = entry.substring(1);
			} else {
				fadeDuration = Double.parseDouble(entry.substring(1));
			}
		}
		if (!artist.equals("") && fadeDuration != 0) {
			maxFade.put(fadeDuration / 60, artist);
		}
	}

	private void completeTaskFive(Iterable<Text> values) {
		String song = "";
		double length = 0;
		for(Text value : values) {
			String entry = value.toString();
			if (entry.charAt(0) == 'N') {
				song = entry.substring(1);
			} else {
				length = Double.parseDouble(entry.substring(1));
			}
		}
		if(!song.equals("") && length != 0) {
			lengths.put(length / 60, song);
		}

	}

	private void completeTaskSix(Iterable<Text> values) {
		String song = "";
		double danceability = 0;
		double energy = 0;

		for(Text value : values) {
			String entry = value.toString();
			if (entry.charAt(0) == 'N') {
				song = entry.substring(1);
			}else {
				String[] arr = entry.split("\t");
				danceability = Double.parseDouble(arr[0]);
				energy = Double.parseDouble(arr[1]);
			}
		}
		maxDance.put(danceability * energy, song);
	}



}
