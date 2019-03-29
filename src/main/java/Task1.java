import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Task1 {
	public static void createJob(Job job, String input, String output) throws IOException, ClassNotFoundException, InterruptedException{
		job.setJarByClass(Task1.class);
		job.setMapperClass(Task1Mapper.class);
		job.setCombinerClass(Task1Reducer.class);
		job.setReducerClass(Task1Reducer.class);


		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	class Task1Mapper  extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// tokenize into words.
			String[] line = value.toString().split(",");
			context.write(new Text(line[2]), new IntWritable(1));
		}
	}

	class Task1Reducer  extends Reducer<Text, IntWritable, Text, IntWritable> {
		int count = 0;

		// calculate the total count
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			for (IntWritable value : values) {
				count += value.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
}
