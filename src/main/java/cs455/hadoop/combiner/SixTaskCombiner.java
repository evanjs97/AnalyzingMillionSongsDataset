package cs455.hadoop.combiner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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
			default:
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
		}
	}
}
