package cs455.hadoop.reducer;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class SixTaskReducer extends Reducer<Text, Text, Text, NullWritable> {

	private TreeMap<MapKey, String> maxes = new TreeMap<>();
	private HashMap<String, Double> valueAgg = new HashMap<>();

	private String outputHeader = "";
	private char outputType = ' ';
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String keyString = key.toString().substring(1);
		switch (key.charAt(0)) {
			case 'A':
				outputHeader = "Top 10 artists with the most songs:\n";
				outputType = 'A';
				completeSumTasks(keyString,values);
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
				completeTaskFour(keyString,values);
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
			case 'G':
				outputHeader = "Most Generic and Most Unique Artists:\n";
				outputType = 'G';
				completeSumTasks(keyString,values);
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
			if(this.key.equals(o.key) || this.value == o.value) return 0;
			else if(increasing) {
				if(this.value < o.value) return -1;
				else return 1;
			}else {
				if(this.value > o.value) return -1;
				else return 1;
			}
		}

		@Override
		public boolean equals(Object o) {
			if(o.equals(this)) return true;
			else if(!(o instanceof MapKey)) return false;
			else {
				MapKey other = (MapKey) o;
				return other.key.equals(this.key);
			}
		}

		@Override
		public int hashCode() {
			return key.hashCode();
		}
	}

	private String getOutputValue() {

		switch (outputType) {
			case 'A':
				Map.Entry<MapKey, String> countEntry = maxes.pollLastEntry();
				return String.format("%s wrote %.0f songs",countEntry.getValue(), countEntry.getKey().value);
			case 'B':
				Map.Entry<MapKey,String> loudEntry = maxes.pollLastEntry();
				String[] arr = loudEntry.getValue().split("\t");
				double loudness = Double.parseDouble(arr[0]);
				return String.format("%s: wrote songs with an average loudness of %.2f decibals", arr[2], loudness);
			case 'C':
				Map.Entry<MapKey, String> hotEntry = maxes.pollLastEntry();
				return String.format("%s has a hotness score of %.2f.",hotEntry.getValue(), hotEntry.getKey().value);
			case 'D':
				Map.Entry<MapKey, String> fadeEntry = maxes.pollLastEntry();
				String[] arr2 = fadeEntry.getValue().split("\t");
				return String.format("%s wrote songs with a total fade time of %.2f minutes",arr2[1], fadeEntry.getKey().value / 60);
			case 'G':
				Map.Entry<MapKey, String> generic = maxes.pollLastEntry();
				Map.Entry<MapKey, String> unique = maxes.pollFirstEntry();
				String uniqueS = String.format("The most unique artist is %s who is only similar with %.0f other artists", unique.getValue(), unique.getKey().value);
				String genericS = String.format("The most generic artist is %s who is similar with %.0f other artists", generic.getValue(), generic.getKey().value);
				return uniqueS + "\n" + genericS;
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
		else if(outputType == 'G' && !maxes.isEmpty()) context.write(new Text(getOutputValue()), NullWritable.get());
		else {
			for (int i = 0; i < Math.min(10, maxes.size()); i++) {
				context.write(new Text(getOutputValue()), NullWritable.get());
			}
		}
	}


	private void completeSumTasks(String key, Iterable<Text> values) {
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


///fix this fuinction
	private void completeTaskTwo(String key, Iterable<Text> values) {
		String artist = "";
		String artistID = "";
		double loudness = 0;
		for(Text value : values) {
			String entry = value.toString();
			if(entry.charAt(0) == 'N') {
				String[] arr = entry.substring(1).split("\t");
				artistID = arr[0];
				artist = arr[1];
			}else {
				loudness = Double.parseDouble(entry.substring(1));
			}
		}

		if(!artist.equals("")) {
			//MapKey replace = new MapKey(artistID, 0,true);
			double oldVal = 0;
			if(valueAgg.containsKey(artistID)) oldVal = valueAgg.get(artistID);
			MapKey replace = new MapKey(artistID, oldVal, true);
			//if(value.equals("")) value = "0.0\t0\t"+artist;
			//String value = maxes.getOrDefault(replace, "0.0\t0\t"+artist);
			int oldCount = 0;
			if(maxes.containsKey(replace)) {
				String[] arr = maxes.remove(replace).split("\t");
				oldCount = Integer.parseInt(arr[1]);
			}
			//String[] arr = value.split("\t");
			//int oldCount = Integer.parseInt(arr[1]);
			double newVal = (oldVal * oldCount) + loudness;
			int newCount = oldCount +1;
			replace.value = newVal / newCount;
			valueAgg.put(artistID, replace.value);
			maxes.put(replace, replace.value + "\t" + newCount + "\t" + artist);
		}
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

	private void completeTaskFour(String key, Iterable<Text> values) {
		String artist = "";
		String artistID = "";
		double fade = 0;
		for(Text value : values) {
			String entry = value.toString();
			if (entry.charAt(0) == 'N') {
				String[] arr = entry.substring(1).split("\t");
				artistID = arr[0];
				artist = arr[1];
			} else {
				fade = Double.parseDouble(entry.substring(1));
			}
		}
		if(!artist.equals("") && fade > 0) {
			double oldFade = 0;
                        if(valueAgg.containsKey(artistID)) oldFade = valueAgg.get(artistID);
                        MapKey replace = new MapKey(artistID, oldFade, true);

			//MapKey replace = new MapKey(artistID, 0, true);
			//String value = "";
			if(maxes.containsKey(replace)) maxes.remove(replace);
			//if(value.equals("")) value = "0.0\t"+artist;
			//String[] arr = value.split("\t");
			//double oldFade = Double.parseDouble(arr[0]);
			replace.value = oldFade + fade;
			maxes.put(replace, replace.value + "\t" + artist);
		}

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
