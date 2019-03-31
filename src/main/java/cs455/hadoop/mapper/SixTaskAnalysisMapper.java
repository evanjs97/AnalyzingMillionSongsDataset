package cs455.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class SixTaskAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			List<String> line = Util.readCSV(value.toString());
			//song_id = line.get(1), fade_end_in = line.get(6), fade_start_out = line.get(13), song_length = line.get(5)
			//loudness = line.get(10), hotness = line.get(2), danceability = line.get(4), energy = line.get(7)
			if(!line.get(1).equals("song_id")) {
				if (!line.get(1).equals("")) {
					Double fadeDuration = calculateFade(line.get(6), line.get(13), line.get(5));
					double loudness = -1;
					try {
						loudness = Double.parseDouble(line.get(10));

					} catch (NumberFormatException nfe) {
					}
					//for answering question 4
					if (!fadeDuration.equals(-1.0))
						context.write(new Text("D" + line.get(1)), new Text("F" + fadeDuration));

					//for answering question 2
					if (loudness != -1) context.write(new Text("B" + line.get(1)), new Text("L" + loudness));

					//for answering quetions 3
					if (!line.get(2).equals(""))
						context.write(new Text("C" + line.get(1)), new Text("H" + line.get(2)));

					//for answering question 5
					if (!line.get(5).equals("")) context.write(new Text("E" + line.get(1)), new Text("L"+line.get(5)));

					//for answering question 6
					if (!line.get(4).equals("") && !line.get(7).equals(""))
						context.write(new Text("F" + line.get(1)), new Text(line.get(4) + "\t" + line.get(7)));
				}
			}
		}

		private Double calculateFade(String endFadeIn, String startFadeOut, String duration) {
			try {
				Double fadeIn = Double.parseDouble(endFadeIn);
				Double fadeOut = Double.parseDouble(startFadeOut);
				Double time = Double.parseDouble(duration);
				return fadeIn + (time - fadeOut);
			}catch(NumberFormatException e) {
				return -1.0;
			}

		}

}
