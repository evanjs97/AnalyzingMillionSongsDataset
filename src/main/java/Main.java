import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class Main {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length < 1) {
			System.out.println("Must specify at least 1 argument");
			System.exit(1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Task");

		switch(args[0]) {
			case "-1":
				Task1.createJob(job, args[1], args[2]);
				break;
		}
	}
}
