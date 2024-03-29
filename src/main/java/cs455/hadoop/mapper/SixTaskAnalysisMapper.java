package cs455.hadoop.mapper;


import cs455.hadoop.util.Util;
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
		//segments_start = line[18], segments_pitches = line[20], segments_timbre = line[21], segments_loudness_max = line[22]
		//segments_max_loudness_time = line[23], segments_start_loudness = line[24]
		if(!line.get(1).equals("song_id")) {
			if (!line.get(1).equals("")) {
				Double fadeDuration = calculateFade(line.get(6), line.get(13), line.get(5));
//				Double fadeDuration = Double.parseDouble(line.get(6));
				double loudness = 0;
				try {
					loudness = Double.parseDouble(line.get(10));

				} catch (NumberFormatException nfe) {
				}
				//for answering question 4
				if (!fadeDuration.equals(-1.0))
					context.write(new Text("D" + line.get(1)), new Text("F" + fadeDuration));

				//for answering question 2
				if (loudness != 0) context.write(new Text("B" + line.get(1)), new Text("L" + loudness));

				//for answering quetions 3
				if (!line.get(2).equals(""))
					context.write(new Text("C" + line.get(1)), new Text("H" + line.get(2)));

				//for answering question 5
				if (!line.get(5).equals("")
						&& !line.get(5).equals("0")) context.write(new Text("E" + line.get(1)), new Text("L"+line.get(5)));

				//for answering question 6
				if (!line.get(4).equals("") && !line.get(7).equals(""))
					context.write(new Text("F" + line.get(1)), new Text(line.get(4) + "\t" + line.get(7)));

				//for popularity:
				// song traits: duration(5), key(8), loudness(10), mode(11), tempo(14), time_signature(15), danceability(4), energy(7)
				// fade_in(6), fade_out(13)
				// artist traits: terms(11) space separated

				//Question 9 approach:
				//from all songs with popularity score of 1.0
				//average duration, most common key, average loudness, most common mode, average tempo, most common time_signature
				//ignore dance and energy, average fade_in time, average fade_out time
				//output order duration, fade_in, key, loudness, mode, fade_out, tempo, time_signature
				if(!line.get(2).isEmpty() && Double.parseDouble(line.get(2)) > .95)
					context.write(new Text("H"+line.get(1)), new Text("M"+line.get(5)+"\t"+line.get(6)+"\t"
						+line.get(8)+" 1\t"+line.get(10)+"\t"+line.get(11)+" 1\t"
							+(Double.parseDouble(line.get(5))-Double.parseDouble(line.get(13)))+"\t"+line.get(14)+"\t"+line.get(15) + " 1"));

			}
		}
	}



	//idea for job 7 completion: sum the values from each set of segment data starting from both front and end and move towards center, this will preserve how the average
	//song starts and ends which is likely important, keep track of how many songs there are for averaging
	//also keep track of average number of segments in songs and use that many segments for creating segment for average song
	//once again iterating from front and back at same time for averages

//	private SongSegment createSegmentData(DoubleWritable[] starts, DoubleWritable[][] pitches, DoubleWritable[][] timbres,
//										  DoubleWritable[] maxLouds, DoubleWritable[] maxLoudTimes, DoubleWritable[] loudStarts) {
//		//return String.join("\t", starts) + ""
//		return new SongSegment(starts, maxLouds, maxLoudTimes, loudStarts, pitches, timbres);
//	}

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

