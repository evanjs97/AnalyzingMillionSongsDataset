package cs455.hadoop.reducer;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader;
import cs455.hadoop.util.AvgDouble;
import cs455.hadoop.util.SongSegment;
import cs455.hadoop.util.Util;
import org.apache.commons.collections.ArrayStack;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

public class TaskSevenReducer extends Reducer<NullWritable, SongSegment, NullWritable, Text> {
	protected void reduce (NullWritable key, Iterable <SongSegment> values, Context context) throws IOException, InterruptedException {
		//		String keyString = key.toString();
		SongSegment output = SongSegment.createInputSplit(values);
		if (output != null) context.write(NullWritable.get(), new Text(output.toString()));
	}
}









//	private void addTwoLists(ArrayList<Double> lists) {
//		int front = 0;
//		int back = starts.length -1;
//		int start = 0;
//		int end = startList.size()-1;
//	}

//	private void

//new idea, find average segment length;
	//create new array of average segment size
	//add to array from each array in list using sample window
	//num sample windows =

//	private Text combineSegments(Iterable<Text> values) {
//		ArrayList<Double> startList = new ArrayList<>();
//		ArrayList<ArrayList<Double>> pitchList = new ArrayList<>();
//		ArrayList<ArrayList<Double>> timbreList = new ArrayList<>();
//		ArrayList<Double> maxLoudList = new ArrayList<>();
//		ArrayList<Double> maxLoudTimeList = new ArrayList<>();
//		ArrayList<Double> maxLoudStartList = new ArrayList<>();
//		for(Text vals : values) {
//			String[] splits = vals.toString().split("\t");
//			Double[] starts = Util.lineTo1DDoubleArr(splits[0]);
//			Double[][] pitches = Util.lineTo2DDoubleArr(splits[1],12);
//			Double[][] timbres = Util.lineTo2DDoubleArr(splits[2],12);
//			Double[] maxLouds = Util.lineTo1DDoubleArr(splits[3]);
//			Double[] maxLoudTimes = Util.lineTo1DDoubleArr(splits[4]);
//			Double[] maxLoudStartTimes = Util.lineTo1DDoubleArr(splits[5]);
//			int front = 0;
//			int back = starts.length -1;
//			int start = 0;
//			int end = startList.size()-1;
//			while(front < (starts.length-1) / 2 && back >= (starts.length-1)/2) {
//				int endIndex = Math.min(end, startList.size());
//				if(start >= startList.size()) {
//					startList.add(start, starts[front]);
//					pitchList.add(start, new ArrayList<>(Arrays.asList(pitches[front])));
//					timbreList.add(start, new ArrayList<>(Arrays.asList(timbres[front])));
//					maxLoudList.add(start, maxLouds[front]);
//					maxLoudTimeList.add(start, maxLoudTimes[front]);
//					maxLoudStartList.add(start, maxLoudStartTimes[front]);
//
//
//					startList.add(endIndex, starts[back]);
//					pitchList.add(endIndex, new ArrayList<>(Arrays.asList(pitches[back])));
//					timbreList.add(endIndex, new ArrayList<>(Arrays.asList(timbres[back])));
//					maxLoudList.add(endIndex, maxLouds[back]);
//					maxLoudTimeList.add(endIndex, maxLoudTimes[back]);
//					maxLoudStartList.add(endIndex, maxLoudStartTimes[back]);
//
//				}
//				else {
//					startList.set(start, startList.get(start) + starts[front]);
//					startList.set(endIndex, startList.get(endIndex) + starts[back]);
//					pitchList.set(start, pitchList.get(start)new ArrayList<>(Arrays.asList(pitches[front]));
//					pitchList.set(endIndex, new ArrayList<>(Arrays.asList(pitches[back])));
//					timbreList.set(start, new ArrayList<>(Arrays.asList(timbres[front])));
//					timbreList.set(endIndex, new ArrayList<>(Arrays.asList(timbres[back])));
//					maxLoudList.set(start, maxLoudList.get(start) + maxLouds[front]);
//					maxLoudList.set(endIndex, maxLoudList.get(endIndex) + maxLouds[back]);
//					maxLoudTimeList.set(start, maxLoudTimeList.get(start) + maxLoudTimes[front]);
//					maxLoudTimeList.set(endIndex, maxLoudTimeList.get(endIndex) + maxLoudTimes[back]);
//					maxLoudStartList.set(start, maxLoudStartList.get(start) + maxLoudStartTimes[front]);
//					maxLoudStartList.set(endIndex, maxLoudStartList.get(endIndex) + maxLoudStartTimes[back]);
//
//				}
//				start++;
//				end--;
//				front++;
//				back--;
//
//			}
//		}
//	}

