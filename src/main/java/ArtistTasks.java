import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class ArtistTasks {

	private final String regex = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";

	public static void createJob(Job job, String analysis, String metadata, String output) throws IOException, ClassNotFoundException, InterruptedException{
		job.setJarByClass(ArtistTasks.class);
		job.setCombinerClass(Task2Reducer.class);
		job.setReducerClass(Task2Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputDirRecursive(job, true);

		MultipleInputs.addInputPath(job, new Path(analysis), TextInputFormat.class, Task2AnalysisMapper.class);
		MultipleInputs.addInputPath(job, new Path(metadata), TextInputFormat.class, Task2MetadataMapper.class);

		Path temp = new Path("/cs455/temp/task2/job1");
		FileOutputFormat.setOutputPath(job, temp);

		Path temp2 = new Path("/cs455/temp/task2/job2");
		reduceArtistJob(temp, temp2);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static void reduceArtistJob(Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job2");
		job.setJarByClass(ArtistTasks.class);
		job.setMapperClass(TaskArtistMapper.class);
		job.setCombinerClass(TaskArtistReducer.class);
		job.setReducerClass(TaskArtistReducer.class);

		FileInputFormat.setInputDirRecursive(job, true);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
	}

	public static void reduceArtistTopNJob(Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job3");
		job.setJarByClass(ArtistTasks.class);
		job.setMapperClass(TaskArtistTopNMapper.class);
		job.setReducerClass(TaskArtistTopNReducer.class);

		FileInputFormat.setInputDirRecursive(job, true);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
	}

	class Task2AnalysisMapper  extends Mapper<LongWritable, Text, Text, Text> {
		/**
		 * Get unique song id for each song
		 * @param key
		 * @param value
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split(regex);
			//outputing song_id      loudness      fade_duration
			String fadeDuration = calculateFade(line[5],line[12],line[4]).toString();
			context.write(new Text(line[0]), new Text("C"+line[9] + "\t" + fadeDuration));
		}

		protected Double calculateFade(String startFadeIn, String startFadeOut, String duration) {
			Double fadeIn = Double.parseDouble(startFadeIn);
			Double fadeOut = Double.parseDouble(startFadeOut);
			Double time = Double.parseDouble(duration);
			return fadeIn + (time - fadeOut);
		}
	}

	class Task2MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
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
			String[] line = value.toString().split(regex);
			//outputing song_id    artist_id
			context.write(new Text(line[7]), new Text("B" + line[2] + "\t" + line[6]));
			//outputting artist_id
			//context.write(new Text(line[2]), new Text("A"));

		}
	}

	class Task2Reducer  extends Reducer<Text, IntWritable, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//int counter = 0;
			String artistID = "";
			String loudnessFade = "";
			String artistName = "";
			for (Text value : values) {
				if(value.charAt(0) == 'B') {
					String[] arr = value.toString().substring(1).split("\t");
					artistID = arr[0];
					artistName = arr[1];
				}else if(value.charAt(0) == 'C') {
					loudnessFade = value.toString();
				}
			}
			//writing key=artists_id     value=loudness TAB fade TAB artistName
			context.write(new Text(artistID), new Text(loudnessFade + "\t" + artistName));
		}
	}

	class TaskArtistMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			//outputting key=artistID     value=1 TAB loudness TAB fade TAB artistName
			//1 is for counting each song for each artist (Primarily to allow use of combiner)
			context.write(key, new Text(1 + "\t" + value.toString()));
		}
	}

	class TaskArtistReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int counter = 0;
			double loudness = 0;
			double fadeDuration = 0;
			String artistName = "";
			for(Text text : values) {
				String[] arr = text.toString().split("\t");
				counter += Integer.parseInt(arr[0]);
				loudness += Double.parseDouble(arr[1]);
				fadeDuration += Double.parseDouble(arr[2]);
				artistName = arr[3];
			}
			//outputting key=artistID     value=numberOfSongs TAB totalLoudnessSummed TAB totalFadeSummed TAB artistName
			context.write(key, new Text(counter +"\t" + loudness + "\t" + fadeDuration + "\t" + artistName));
		}
	}

	class TaskArtistTopNMapper extends Mapper<Text, Text, Text, Text> {
		private TreeMap<Integer, String> counts = new TreeMap<>();
		private TreeMap<Double, String> loudness = new TreeMap<>();
		private TreeMap<Double, String> fades = new TreeMap<>();
		private Text artistName;
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String[] splits = value.toString().split("\t");
			Integer count = Integer.parseInt(splits[0]);
			Double avgLoudness = Double.parseDouble(splits[1]) / count;
			Double fade = Double.parseDouble(splits[2]);
			artistName = new Text(splits[3]);
			counts.put(count, splits[3]);
			loudness.put(avgLoudness, splits[3]);
			fades.put(fade, splits[3]);
			if(counts.size() > 10) counts.remove(counts.lastKey());
			if(loudness.size() > 10) loudness.remove(loudness.lastKey());
			if(fades.size() > 10) fades.remove(fades.lastKey());
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			String output = "";
			Iterator<Integer> countIter = counts.keySet().iterator();
			Iterator<Double> loudnessIter = loudness.keySet().iterator();
			Iterator<Double> fadeIter = fades.keySet().iterator();
			while(countIter.hasNext() && loudnessIter.hasNext() && fadeIter.hasNext()) {
				output += countIter.next() + "\t" + loudnessIter.next() + "\t" + fadeIter.next();
			}
			//outputting key=artistID     value=numberOfSongs TAB avgLoudness TAB totalFadeSummed TAB artistName
			context.write(artistName, new Text(output));
		}

	}

	class TaskArtistTopNReducer extends Reducer<Text, Text, Text, Text> {
		private TreeMap<Integer, String> counts = new TreeMap<>();
		private TreeMap<Double, String> loudness = new TreeMap<>();
		private TreeMap<Double, String> fades = new TreeMap<>();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text text : values) {
				String[] arr = text.toString().split("\t");
				Integer count = Integer.parseInt(arr[0]);
				Double avgLoudness = Double.parseDouble(arr[1]);
				Double fade = Double.parseDouble(arr[2]);
				Text artistName = new Text(arr[3]);
				counts.put(count, arr[3]);
				loudness.put(avgLoudness, arr[3]);
				fades.put(fade, arr[3]);
				if(counts.size() > 10) counts.remove(counts.lastKey());
				if(loudness.size() > 10) loudness.remove(loudness.lastKey());
				if(fades.size() > 10) fades.remove(fades.lastKey());

				context.write(new Text("Top 10 artists with the most songs:"), new Text("\n"));
				for(Integer num : counts.keySet()) {
					context.write(artistName, new Text("wrote " + num.toString() + "songs"));
				}

				context.write(new Text("\nTop 10 artists with the loudest songs on average:"), new Text("\n"));

				for(Double num : loudness.keySet()) {
					context.write(artistName, new Text("has an average loudness of " + num.toString() + " in their songs"));
				}

				context.write(new Text("\nTop 10 artists with the longest total fade time in their songs:"), new Text("\n"));

				for(Double num : fades.keySet()) {
					context.write(artistName, new Text("has a total fade time of " + num.toString() + " in their songs"));
				}


			}
		}

		protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {

		}
	}



}
