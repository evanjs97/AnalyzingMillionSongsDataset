package cs455.hadoop.combiner;

import cs455.hadoop.util.AvgDouble;
import cs455.hadoop.util.Util;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;

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



	private void hotnessTask(Iterable<Text> values, Context context, Text key) throws IOException, InterruptedException {
//		double duration = 0;
//		int durCount = 0;
		AvgDouble duration = new AvgDouble();
		AvgDouble loudness = new AvgDouble();
		AvgDouble tempo = new AvgDouble();
		AvgDouble fadeIn = new AvgDouble();
		AvgDouble fadeOut = new AvgDouble();
//		double loudness = 0;
//		int loudCount = 0;
//		double tempo = 0;
//		int tempoCount = 0;
//		double fadeIn = 0;
//		int inCount = 0;
//		double fadeOut = 0;
//		int outCount = 0;
		LinkedList<String> keys = new LinkedList<>();
		LinkedList<String> modes = new LinkedList<>();
		LinkedList<String> timeSigs = new LinkedList<>();

		for(Text val : values) {
			String entry = val.toString();
			if(entry.charAt(0) == 'N') context.write(new Text(key), new Text(entry));
			else {
				String[] arr = entry.substring(1).split("\t");
				double temp = Util.DoubleOrZero(arr[0]);
				if(temp != 0) {
					duration.add(temp);
//					duration += temp;
//					durCount++;
				}
				temp = Util.DoubleOrZero(arr[1]);
				if(temp != 0) {
					fadeIn.add(temp);
//					fadeIn += temp;
//					inCount++;
				}
				temp = Util.DoubleOrZero(arr[3]);
				if(temp != 0) {
					loudness.add(temp);
//					loudness += temp;
//					loudCount++;
				}
				temp = Util.DoubleOrZero(arr[5]);
				if(temp != 0) {
					fadeOut.add(temp);
//					fadeOut += temp;
//					outCount++;
				}
				temp = Util.DoubleOrZero(arr[6]);
				if(temp != 0) {
					tempo.add(temp);
				}

				if(!arr[2].isEmpty()) keys.add(arr[2]);
				if(!arr[4].isEmpty()) modes.add(arr[4]);
				if(!arr[7].isEmpty()) timeSigs.add(arr[7]);

				context.write(new Text(key), new Text("C"+duration.preserveCountString() + "\t"
						+ fadeIn.preserveCountString() + "\t" + Util.formatList(keys)+ "\t" + loudness.preserveCountString()
						+ "\t" + Util.formatList(modes) + "\t" + fadeOut.preserveCountString() + "\t" + tempo + "\t" + Util.formatList(timeSigs)));
			}
		}

	}
}
