package cs455.hadoop.combiner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SixTaskCombiner  extends Reducer<Text, Text, Text, NullWritable> {
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String keyString = key.toString();
//		LinkedList<String>
		switch (keyString.charAt(0)) {
			case 'G':

		}
	}

}
