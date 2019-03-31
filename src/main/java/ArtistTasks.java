import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import java.net.URI;
import java.util.*;

public class ArtistTasks {

	private static final String regex = ",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))";


	public static void createJob(Job job, String analysis, String metadata, String output) throws IOException, ClassNotFoundException, InterruptedException{
		job.setJarByClass(ArtistTasks.class);
//		job.setCombinerClass(Task2Reducer.class);
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
		job.waitForCompletion(true);

		Path temp2 = new Path("/cs455/temp/task2/job2");
		reduceArtistJob(temp, temp2);

		reduceArtistTopNJob(temp2, new Path(output));
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

		List<Path> paths = new ArrayList<>();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] statuses = fs.globStatus(new Path(input + "/part*"));
		for (FileStatus status : statuses) {
			System.out.println(status.getPath().toString());
			paths.add(status.getPath());
		}
		FileInputFormat.setInputPaths(job, (Path[]) paths.toArray(new Path[paths.size()]));

		//FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
	}

	public static void reduceArtistTopNJob(Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "job3");
		job.setJarByClass(ArtistTasks.class);
		job.setMapperClass(TaskArtistTopNMapper.class);
		job.setReducerClass(TaskArtistTopNReducer.class);
		job.setNumReduceTasks(1);
		FileInputFormat.setInputDirRecursive(job, true);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		List<Path> paths = new ArrayList<>();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] statuses = fs.globStatus(new Path(input + "/part*"));
		for (FileStatus status : statuses) {
			System.out.println(status.getPath().toString());
			paths.add(status.getPath());
		}
		FileInputFormat.setInputPaths(job, (Path[]) paths.toArray(new Path[paths.size()]));

//		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
//
//	public static class Task2AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
//		public Task2AnalysisMapper() {}
//		/**
//		 * Get unique song id for each song
//		 * @param key
//		 * @param value
//		 * @param context
//		 * @throws IOException
//		 * @throws InterruptedException
//		 */
//		@Override
//		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//			String[] line = value.toString().split(regex);
//			//outputing song_id      Cloudness TAB fade_duration
//			if(!line[1].equals("song_id")) {
//				Double fadeDuration = calculateFade(line[6], line[13], line[5]);
//				if (fadeDuration != -1) {
//					try{
//						Double.parseDouble(line[10]);
//						context.write(new Text(line[1]), new Text("C" + line[10] + "\t" + fadeDuration));
//					}catch(NumberFormatException e) {
//
//					}
//
//				}
//			}
//
//
//		}
//
//		protected Double calculateFade(String startFadeIn, String startFadeOut, String duration) {
//			try {
//				Double fadeIn = Double.parseDouble(startFadeIn);
//				Double fadeOut = Double.parseDouble(startFadeOut);
//				Double time = Double.parseDouble(duration);
//				return fadeIn + (time - fadeOut);
//			}catch(NumberFormatException e) {
//				return -1.0;
//			}
//
//		}
//	}


//
//	public static class SixTaskMetadataMapper extends Mapper<LongWritable, Text, Text, Text> {
//		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//			List<String> line = readCSV(value.toString());
//			//song_id = line.get(8), artist_id = line.get(3), artist_name = line.get(7), song_title = line.get(9);
//
//			//output key=Aartist_id value=artist_name TAB 1 (for answering question 1
//			if(!line.get(3).equals("")) context.write(new Text("A" + line.get(3)), new Text(line.get(7) + "\t" + 1));
//
//			//output key=Bsong_id value=artist_name (for answering question 2 & 4)
//			if(!line.get(8).equals("")) {
//				context.write(new Text("B" + line.get(8)), new Text(line.get(7)));
//				context.write(new Text("D" + line.get(8)), new Text(line.get(7)));
//			}
//
//			//for answering question 3, 5 & 6
//			if(!line.get(9).equals("")) {
//				context.write(new Text("C"), new Text(line.get(9)));
//				context.write(new Text("E"), new Text(line.get(9)));
//				context.write(new Text("F"), new Text(line.get(9)));
//			}
//
//		}
//	}








	public static class Task2Reducer  extends Reducer<Text, Text, Text, Text> {
		public Task2Reducer(){}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int counter = 0;
			String artistID = "";
			String loudnessFade = "";
			String artistName = "";
//			StringBuilder builder = new StringBuilder();
			for (Text value : values) {
//				builder.append("\t");
//				builder.append(value.toString());
				String str = value.toString();
				if(str.charAt(0) == 'B') {
					String[] arr = str.split("\t");
					artistID = arr[0];
					artistName = arr[1];
					//context.write(new Text(artistID),new Text(artistName + "\t**B**"));
				}else if(str.charAt(0) == 'C') {
					loudnessFade = str.substring(1);
					//context.write(key, new Text(loudnessFade + "\t**C**"));
				}
//				else {
//					context.write(new Text(key), new Text(value));
//				}
				counter++;
//				String result ="\t" + counter;
//				builder.append(result);

			}
//			String outString = builder.toString();
//			context.write(new Text(key), new Text(outString));
			//writing key=artists_id     value=loudness TAB fade TAB artistName
			if(counter > 1) context.write(new Text(artistID), new Text(loudnessFade + "\t" + artistName));
		}
	}

	public static class TaskArtistMapper extends Mapper<LongWritable, Text, Text, Text> {
		public TaskArtistMapper(){}
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//outputting key=artistID     value=1 TAB loudness TAB fade TAB artistName
			//1 is for counting each song for each artist (Primarily to allow use of combiner)
			String valString = value.toString();
			int index = valString.indexOf("\t");
			context.write(new Text(valString.substring(0,index)), new Text(1 + "\t" + valString.substring(index+1)));
		}
	}

	public static class TaskArtistReducer extends Reducer<Text, Text, Text, Text> {
		public TaskArtistReducer(){}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int counter = 0;
			double loudness = 0;
			double fadeDuration = 0;
			String artistName = "";
			for(Text text : values) {
				String[] arr = text.toString().split("\t");
				if(arr.length == 4) {
					counter += Integer.parseInt(arr[0]);
					loudness += Double.parseDouble(arr[1]);
					fadeDuration += Double.parseDouble(arr[2]);
					artistName = arr[3];
				}
			}
			//outputting key=artistID     value=numberOfSongs TAB totalLoudnessSummed TAB totalFadeSummed TAB artistName
			if(!artistName.equals(""))context.write(key, new Text(counter +"\t" + loudness + "\t" + fadeDuration + "\t" + artistName));
		}
	}

	public static class TaskArtistTopNMapper extends Mapper<LongWritable, Text, Text, Text> {
		public TaskArtistTopNMapper(){}
		private TreeMap<Integer, String> counts = new TreeMap<>();
		private TreeMap<Double, String> loudness = new TreeMap<>();
		private TreeMap<Double, String> fades = new TreeMap<>();
		private Text artistName;
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splits = value.toString().split("\t");
			Integer count = Integer.parseInt(splits[0]);
			Double avgLoudness = Double.parseDouble(splits[1]) / count;
			Double fade = Double.parseDouble(splits[2]);
			artistName = new Text(splits[3]);
			counts.put(count, splits[3]);
			loudness.put(avgLoudness, splits[3]);
			fades.put(fade, splits[3]);
			if(counts.size() > 10) counts.remove(counts.firstKey());
			if(loudness.size() > 10) loudness.remove(loudness.lastKey());
			if(fades.size() > 10) fades.remove(fades.firstKey());
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if(artistName != null) {
				String output = "";
				Iterator<Integer> countIter = counts.keySet().iterator();
				Iterator<Double> loudnessIter = loudness.keySet().iterator();
				Iterator<Double> fadeIter = fades.keySet().iterator();
				while (countIter.hasNext() && loudnessIter.hasNext() && fadeIter.hasNext()) {
					output += countIter.next() + "\t" + loudnessIter.next() + "\t" + fadeIter.next();
				}
				//outputting key=artistID     value=numberOfSongs TAB avgLoudness TAB totalFadeSummed TAB artistName
				context.write(artistName, new Text(output));
			}
		}

	}

	public static class TaskArtistTopNReducer extends Reducer<Text, Text, Text, Text> {
		private TreeMap<Integer, String> counts = new TreeMap<>();
		private TreeMap<Double, String> loudness = new TreeMap<>();
		private TreeMap<Double, String> fades = new TreeMap<>();

		public TaskArtistTopNReducer(){}
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
				if(counts.size() > 10) counts.remove(counts.firstKey());
				if(loudness.size() > 10) loudness.remove(loudness.lastKey());
				if(fades.size() > 10) fades.remove(fades.firstKey());

				context.write(new Text("Top 10 artists with the most songs:"), new Text("\n"));
				ArrayList<Integer> countList = new ArrayList<>(counts.keySet());
				Collections.sort(countList, Collections.reverseOrder());
				for(Integer num : countList) {
					context.write(artistName, new Text("wrote " + num.toString() + "songs"));
				}

				context.write(new Text("\nTop 10 artists with the loudest songs on average:"), new Text("\n"));

				for(Double num : loudness.keySet()) {
					context.write(artistName, new Text("has an average loudness of " + num.toString() + " in their songs"));
				}

				context.write(new Text("\nTop 10 artists with the longest total fade time in their songs:"), new Text("\n"));
				ArrayList<Double> fadeList = new ArrayList<>(fades.keySet());
				Collections.sort(fadeList, Collections.reverseOrder());
				for(Double num : fadeList) {
					context.write(artistName, new Text("has a total fade time of " + num.toString() + " in their songs"));
				}


			}
		}

		protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
			FileSystem hdfs = FileSystem.get(URI.create("hdfs://<juneau>:<41928>"), conf);
			hdfs.delete(new Path("/cs455/temp/task2"), true);
		}
	}



}
