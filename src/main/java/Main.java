import cs455.hadoop.mapper.SixTaskAnalysisMapper;
import cs455.hadoop.mapper.SixTaskMetadataMapper;
import cs455.hadoop.partitioner.SixTaskParitioner;
import cs455.hadoop.reducer.SixTaskReducer;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length < 3) {
			System.out.println("Must specify at least 3 arguments");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task");

		switch(args[0]) {
			case "-1":
				createJobOne(job, args[1], args[2], args[3]);
				break;
		}
	}

	public static void createJobOne(Job job, String analysis, String metadata, String output) throws IOException, ClassNotFoundException, InterruptedException{
		job.setJarByClass(Main.class);
//		job.setCombinerClass(Task2Reducer.class);
		job.setPartitionerClass(SixTaskParitioner.class);
		job.setReducerClass(SixTaskReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputDirRecursive(job, true);

		MultipleInputs.addInputPath(job, new Path(analysis), TextInputFormat.class, SixTaskAnalysisMapper.class);
		MultipleInputs.addInputPath(job, new Path(metadata), TextInputFormat.class, SixTaskMetadataMapper.class);


		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setNumReduceTasks(6);
		job.waitForCompletion(true);

	}
}
