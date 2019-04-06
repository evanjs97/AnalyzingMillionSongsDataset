package cs455.hadoop.util;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedList;

public class SongSegment implements Writable{
	private AvgDoubleArrayWritable segStartTimes;
	private AvgDoubleArrayWritable segMaxLoudness;
	private AvgDoubleArrayWritable segMaxLoudnessDurations;
	private AvgDoubleArrayWritable segMaxLoudnessStartTimes;

	private AvgDouble2DArrayWritable segPitches;
	private AvgDouble2DArrayWritable segTimbres;

	private boolean invalid = false;
	private int size;

	class AvgDoubleArrayWritable extends ArrayWritable {
		public AvgDoubleArrayWritable() {
			super(AvgDouble.class);
		}

		public AvgDoubleArrayWritable(AvgDouble[] vals) {
			super(AvgDouble.class, vals);
		}
	}

	class AvgDouble2DArrayWritable extends TwoDArrayWritable {
		public AvgDouble2DArrayWritable() {
			super(AvgDouble.class);
		}

		public AvgDouble2DArrayWritable(AvgDouble[][] vals) {
			super(AvgDouble.class, vals);
		}
	}

	public SongSegment(){
		segStartTimes = new AvgDoubleArrayWritable();
		segMaxLoudness = new AvgDoubleArrayWritable();
		segMaxLoudnessDurations = new AvgDoubleArrayWritable();
		segMaxLoudnessStartTimes = new AvgDoubleArrayWritable();

		segPitches = new AvgDouble2DArrayWritable();
		segTimbres = new AvgDouble2DArrayWritable();
	}


	public SongSegment(AvgDouble[] starts, AvgDouble[] maxLouds, AvgDouble[] maxLoudDurations,
					   AvgDouble[] maxLoudStarts, AvgDouble[][] pitches, AvgDouble[][] timbres ) {
		this.segStartTimes = new AvgDoubleArrayWritable(starts);
		this.segMaxLoudness = new AvgDoubleArrayWritable(maxLouds);
		this.segMaxLoudnessDurations = new AvgDoubleArrayWritable(maxLoudDurations);
		this.segMaxLoudnessStartTimes = new AvgDoubleArrayWritable(maxLoudStarts);
		this.segPitches = new AvgDouble2DArrayWritable(pitches);
		this.segTimbres = new AvgDouble2DArrayWritable(timbres);
		if(starts.length != maxLouds.length || maxLouds.length != maxLoudDurations.length || maxLoudDurations.length != maxLoudStarts.length
				|| maxLoudStarts.length != pitches.length || pitches.length != timbres.length) invalid = true;
		this.size = starts.length;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		segStartTimes.write(dataOutput);
		segMaxLoudness.write(dataOutput);
		segMaxLoudnessDurations.write(dataOutput);
		segMaxLoudnessStartTimes.write(dataOutput);
		segPitches.write(dataOutput);
		segTimbres.write(dataOutput);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		segStartTimes.readFields(dataInput);
		segMaxLoudness.readFields(dataInput);
		segMaxLoudnessDurations.readFields(dataInput);
		segMaxLoudnessStartTimes.readFields(dataInput);
		segPitches.readFields(dataInput);
		segTimbres.readFields(dataInput);
	}

	public static SongSegment createInputSplit(Iterable<SongSegment> values) {
		LinkedList<SongSegment> splits = new LinkedList<>();
		int avgLength = 0;
		for(SongSegment segment : values) {
			//String[] split = vals.toString().split("\t");
			avgLength += segment.getSize();
			splits.add(segment);
		}
		if(splits.isEmpty()) return null;
		avgLength = avgLength / splits.size();
		return combineSegments(splits, avgLength);
	}

	private static SongSegment combineSegments(LinkedList<SongSegment> splits, int length) {
		AvgDouble[] sumStarts = new AvgDouble[length];
		AvgDouble[][] sumPitches = new AvgDouble[length][12];
		AvgDouble[][] sumTimbres = new AvgDouble[length][12];
		AvgDouble[] sumMaxLouds = new AvgDouble[length];
		AvgDouble[] sumMaxLoudTimes = new AvgDouble[length];
		AvgDouble[] sumMaxLoudStarts = new AvgDouble[length];
		for(SongSegment seg : splits) {
//			AvgDouble[] starts = Util.lineTo1DAvgDoubleArr(split[0]);
//			AvgDouble[][] pitches = Util.lineTo2DAvgDoubleArr(split[1],12);
//			AvgDouble[][] timbres = Util.lineTo2DAvgDoubleArr(split[2],12);
//			AvgDouble[] maxLouds = Util.lineTo1DAvgDoubleArr(split[3]);
//			AvgDouble[] maxLoudTimes = Util.lineTo1DAvgDoubleArr(split[4]);
//			AvgDouble[] maxLoudStartTimes = Util.lineTo1DAvgDoubleArr(split[5]);
			int windowSize = seg.getSize() / length;
			double remainder = seg.getSize() % length;
			double addIndex = 0;
			if(remainder > 0) addIndex = length / remainder;
			AvgDouble.sum1DArrs(sumStarts, seg.getSegStartTimes(), windowSize, addIndex);
			AvgDouble.sum2DArrs(sumPitches, seg.getSegPitches(), windowSize, addIndex);
			AvgDouble.sum2DArrs(sumTimbres, seg.getSegTimbres(), windowSize, addIndex);
			AvgDouble.sum1DArrs(sumMaxLouds, seg.getSegMaxLoudness(), windowSize, addIndex);
			AvgDouble.sum1DArrs(sumMaxLoudTimes, seg.getSegMaxLoudnessDurations(), windowSize, addIndex);
			AvgDouble.sum1DArrs(sumMaxLoudStarts, seg.getSegMaxLoudnessStartTimes(), windowSize, addIndex);
		}
		return new SongSegment(sumStarts, sumMaxLouds, sumMaxLoudTimes, sumMaxLoudStarts, sumPitches, sumTimbres);
	}

	public String toString() {
		String output = "\n-----Average Segment Start Times-----\n";
		for(Writable w : this.segStartTimes.get()) output += ((AvgDouble)w) + " ";
		output += "\n\n-----Average Segment Pitches-----\n";
		for(Writable[] arr : this.segPitches.get()) {
			for(Writable w : arr) output += ((AvgDouble)w) + " ";
		}
		output += "\n\n-----Average Segment Timbres-----\n";
		for(Writable[] arr : this.segTimbres.get()) {
			for(Writable w : arr) output += ((AvgDouble)w) + " ";
		}
		output += "\n\n-----Average Segment Max Loudness-----\n";
		for(Writable w : this.segMaxLoudness.get()) output += ((AvgDouble)w) + " ";
		output += "\n\n-----Average Segment Max Loudness Durations\n";
		for(Writable w : this.segMaxLoudnessDurations.get()) output += ((AvgDouble)w) + " ";
		output += "\n\n-----Average Segment Max Loudness Start Times\n";
		for(Writable w : this.segMaxLoudnessStartTimes.get()) output += ((AvgDouble)w) + " ";
		output += "\n";
		return output;
	}

	public Writable[] getSegStartTimes() {
		return segStartTimes.get();
	}

	public Writable[] getSegMaxLoudness() {
		return segMaxLoudness.get();
	}

	public Writable[] getSegMaxLoudnessDurations() {
		return segMaxLoudnessDurations.get();
	}

	public Writable[] getSegMaxLoudnessStartTimes() {
		return segMaxLoudnessStartTimes.get();
	}

	public Writable[][] getSegPitches() {
		return segPitches.get();
	}

	public Writable[][] getSegTimbres() {
		return segTimbres.get();
	}

	public int getSize() { return this.segMaxLoudness.get().length; }

	public boolean isInvalid() { return invalid; }
}
