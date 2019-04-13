package cs455.hadoop.mapper;

import cs455.hadoop.util.Util;
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
			//similar_artists = line.get(10)
			if(!line.get(8).equals("song_id")) {
				//output key=Aartist_id value=artist_name TAB 1 (for answering question 1
				if (!line.get(3).equals("") && !line.get(7).equals("")) {
					context.write(new Text("A" + line.get(3)), new Text("N" + line.get(7).substring(2, line.get(7).length() - 1) + "\t" + 1));

				}

				//output key=Bsong_id value=artist_name (for answering question 2 & 4)
				if (!line.get(8).equals("") && !line.get(7).equals("")) {
					String artist = line.get(7).substring(2,line.get(7).length()-1);
					context.write(new Text("B" + line.get(8)), new Text("N" + line.get(3) + "\t" + artist));
					context.write(new Text("D" + line.get(8)), new Text("N" + line.get(3) + "\t" + artist));

					//for completing job 8, get list of similar artists and output all ids for combining later
					if(!line.get(10).equals("")) {
						context.write(new Text("G" + line.get(3)), new Text("N"+artist+"\t"+"0"));
						String[] others = line.get(10).split(" ");
						for(String id : others) {
							if(!id.equals(" ")) {
								context.write(new Text("G"+ id), new Text("E\t1"));
							}
						}
					}
				}

				//for answering question 3, 5 & 6
				if (!line.get(8).equals("")) {
					String song = line.get(9).substring(2,line.get(9).length()-1);
					context.write(new Text("C" + line.get(8)), new Text("N"+song));
					context.write(new Text("E" + line.get(8)), new Text("N"+song));
					context.write(new Text("F" + line.get(8)), new Text("N"+song));
					String[] keywords = line.get(11).trim().split(" ");
					StringBuilder builder = new StringBuilder();
					for(int i = 0; i < keywords.length; i++) {
						builder.append(keywords[i]);
						builder.append(" 1");
						if(i < keywords.length -1)builder.append(",");
					}
					if(keywords.length > 0)context.write(new Text("H"), new Text("N"+builder.toString()));
				}

			}
		}

}
