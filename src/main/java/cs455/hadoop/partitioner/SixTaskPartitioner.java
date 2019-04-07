package cs455.hadoop.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SixTaskPartitioner extends Partitioner<Text, Text> {

	/**
	 * Partitioner to send each task to a different reducer, then each reducer is able to create top N lists for its task
	 * @param key the key of the pair
	 * @param value the value of the pair
	 * @param numReduceTasks the number of reduce tasks
	 * @return reducer to send data to
	 */
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		if(numReduceTasks != 7) return key.hashCode() % numReduceTasks;
		else {
			String str = key.toString();
			switch (str.charAt(0)) {
				case 'A':
					return 0;
				case 'B':
					return 1;
				case 'C':
					return 2;
				case 'D':
					return 3;
				case 'E':
					return 4;
				case 'F':
					return 5;
				default:
					return 6;
			}
		}
	}
}
