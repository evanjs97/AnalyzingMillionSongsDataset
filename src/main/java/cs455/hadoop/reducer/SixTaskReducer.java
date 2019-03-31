package cs455.hadoop.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SixTaskReducer extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for(Text value : values) {
			String entry = value.toString();
			switch (key.charAt(0)) {
				case 'A':

					break;
				case 'B':

					break;
				case 'C':

					break;
				case 'D':

					break;
				case 'E':

					break;
				case 'F':

					break;
			}
		}
	}

}
