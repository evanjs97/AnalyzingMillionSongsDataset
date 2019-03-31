package cs455.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class SixTaskMetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
		public SixTaskMetadataMapper(){}
		/**
		 * map input data to ouput pair containing song id and loudness
		 * @param key the key for data
		 * @param value the data
		 * @param context the context to write to
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			List<String> line = Util.readCSV(value.toString());
			//song_id = line.get(8), artist_id = line.get(3), artist_name = line.get(7), song_title = line.get(9);

			//output key=Aartist_id value=artist_name TAB 1 (for answering question 1
			if(!line.get(3).equals("")) context.write(new Text("A" + line.get(3)), new Text(line.get(7) + "\t" + 1));

			//output key=Bsong_id value=artist_name (for answering question 2 & 4)
			if(!line.get(8).equals("")) {
				context.write(new Text("B" + line.get(8)), new Text(line.get(7)));
				context.write(new Text("D" + line.get(8)), new Text(line.get(7)));
			}

			//for answering question 3, 5 & 6
			if(!line.get(9).equals("")) {
				context.write(new Text("C"), new Text(line.get(9)));
				context.write(new Text("E"), new Text(line.get(9)));
				context.write(new Text("F"), new Text(line.get(9)));
			}

		}

}
