package cs455.hadoop.mapper;

import cs455.hadoop.util.SongSegment;
import cs455.hadoop.util.Util;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class TaskSevenMapper extends Mapper<LongWritable, Text, NullWritable, SongSegment> {
	protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
		List<String> line = Util.readCSV(value.toString());
		if(!line.get(1).equals("song_id")) {
			SongSegment segment = new SongSegment(Util.lineTo1DAvgDoubleArr(line.get(18).trim()),Util.lineTo1DAvgDoubleArr(line.get(22).trim()),
						Util.lineTo1DAvgDoubleArr(line.get(23).trim()), Util.lineTo1DAvgDoubleArr(line.get(24).trim()),Util.lineTo2DAvgDoubleArr(line.get(20).trim(),12),
					Util.lineTo2DAvgDoubleArr(line.get(21).trim(),12));
			if(!segment.isInvalid() && segment.getSize() > 0) context.write(NullWritable.get(), segment);
		}
	}
}
