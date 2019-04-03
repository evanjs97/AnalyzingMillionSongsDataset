package cs455.hadoop.util;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SongSegment implements Writable{
	private ArrayWritable segStartTimes;
	private ArrayWritable segMaxLoudness;
	private ArrayWritable segMaxLoudnessDurations;
	private ArrayWritable segMaxLoudnessStartTimes;

	private TwoDArrayWritable segPitches;
	private TwoDArrayWritable segTimbres;

	public SongSegment(DoubleWritable[] starts, DoubleWritable[] maxLouds, DoubleWritable[] maxLoudDurations,
					   DoubleWritable[] maxLoudStarts, DoubleWritable[][] pitches, DoubleWritable[][] timbres ) {
		this.segStartTimes = new ArrayWritable(DoubleWritable.class, starts);
		this.segMaxLoudness = new ArrayWritable(DoubleWritable.class, maxLouds);
		this.segMaxLoudnessDurations = new ArrayWritable(DoubleWritable.class, maxLoudDurations);
		this.segMaxLoudnessStartTimes = new ArrayWritable(DoubleWritable.class, maxLoudStarts);
		this.segPitches = new TwoDArrayWritable(DoubleWritable.class, pitches);
		this.segTimbres = new TwoDArrayWritable(DoubleWritable.class, timbres);
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

	public ArrayWritable getSegStartTimes() {
		return segStartTimes;
	}

	public ArrayWritable getSegMaxLoudness() {
		return segMaxLoudness;
	}

	public ArrayWritable getSegMaxLoudnessDurations() {
		return segMaxLoudnessDurations;
	}

	public ArrayWritable getSegMaxLoudnessStartTimes() {
		return segMaxLoudnessStartTimes;
	}

	public TwoDArrayWritable getSegPitches() {
		return segPitches;
	}

	public TwoDArrayWritable getSegTimbres() {
		return segTimbres;
	}
}
