package cs455.hadoop.combiner;

import cs455.hadoop.util.SongSegment;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TaskSevenCombiner extends Reducer<NullWritable, SongSegment, NullWritable, SongSegment> {
	protected void reduce (NullWritable key, Iterable < SongSegment > values, Context context) throws
			IOException, InterruptedException {
			//		String keyString = key.toString();
			SongSegment seg = SongSegment.createInputSplit(values);
			if (seg != null) context.write(NullWritable.get(), seg);
		}
}
