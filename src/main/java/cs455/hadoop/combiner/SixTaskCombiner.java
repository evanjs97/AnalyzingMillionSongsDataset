package cs455.hadoop.combiner;

import cs455.hadoop.util.AvgDouble;
import cs455.hadoop.util.Util;
import cs455.hadoop.util.pair.TwoPairStringInt;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
public class SixTaskCombiner extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		switch (key.charAt(0)) {
			case 'A':
				TwoPairStringInt artistSum = null;
//				String artist = "";
//				int sum = 0;
				for (Text val : values) {
//					String entry = val.toString();
//					String[] arr = entry.split("\t");
//					artist = arr[0];
//					sum += Integer.parseInt(arr[1]);
					if(artistSum == null) artistSum = new TwoPairStringInt(val);
					else artistSum.add(Integer.parseInt(val.toString().split("\t")[1]));
				}
				if(artistSum != null) context.write(new Text(key), new Text("N" + artistSum.getStr() + "\t" + artistSum.getVal()));
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
	 * Method to handle input and final output for task 9/hotness task
	 * Hotness Task:
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

		for(Text val : values) {
			String entry = val.toString();
			if(entry.charAt(0) == 'N') {
				context.write(new Text(key), new Text(entry));
			}
			else {
				String[] arr = entry.substring(1).split("\t", -1);

				duration.addStringIfNotZero(arr[0]);
				fadeIn.addStringIfNotZero(arr[1]);
				loudness.addStringIfNotZero(arr[3]);
				fadeOut.addStringIfNotZero(arr[5]);
				tempo.addStringIfNotZero(arr[6]);

				Util.addStringPairToHashMap(arr[2], keys);
				Util.addStringPairToHashMap(arr[4], modes);
				Util.addStringPairToHashMap(arr[7], timeSigs);
			}
		}
		String keywordOut = Util.formatList(keywords);
		if(!keywordOut.isEmpty()) context.write(new Text(key), new Text("N"+keywordOut));

		context.write(new Text(key), new Text("C"+duration.preserveCountString() + "\t"
				+ fadeIn.preserveCountString() + "\t" + Util.formatList(keys)+ "\t" + loudness.preserveCountString()
				+ "\t" + Util.formatList(modes) + "\t" + fadeOut.preserveCountString() + "\t" + tempo + "\t" + Util.formatList(timeSigs)));

	}
}
