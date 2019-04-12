package cs455.hadoop.util;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgDouble implements Writable{
	private DoubleWritable sum;
	private int count;

	public AvgDouble() { this(0,0); }

	public AvgDouble(double sum) { this(sum, 1); }

	public AvgDouble(double sum, int count) {
		this.sum = new DoubleWritable(sum);
		this.count = count;
	}

	public double getSum() { return this.sum.get(); }

	public void combine(AvgDouble other) {
		sum.set(other.getSum() +getSum());
		count += other.count;
	}

	public void add(double other) {
		sum.set(other + getSum());
		count++;
	}

	public void add(double other, int otherCount) {
		sum.set(other + getSum());
		count += otherCount;
	}

	public String toAvgString() {
		if(count == 0) count = 1;
		return ""+(getSum() / count);
	}

	public String preserveCountString() { return getSum() + " " + count; }

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		if(count == 0) count = 1;
		sum.set(getSum() / count);
		sum.write(dataOutput);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		sum.readFields(dataInput);
		count = 1;
	}

	public static String arr2DToString(AvgDouble[][] arr) {
		String output = "";
		for(int i = 0; i < arr.length; i++) {
			output += arr1DToString(arr[i]);
			if(i < arr.length-1) output += " ";
		}
		return output;
	}

	public static String arr1DToString(AvgDouble[] arr) {
		String output = "";
		for(int i = 0; i < arr.length; i++) {
			output += arr[i].toAvgString();
			if(i < arr.length-1) output += " ";
		}
		return output;
	}

	public static void sum1DArrs(AvgDouble[] to, Writable[] from, int window, double addIndex) {
		for(int i = 0; i < to.length; i++) {
			if(to[i] == null) to[i] = getWindowSum(from, i , window, addIndex);
			else to[i].combine(getWindowSum(from, i, window, addIndex));
		}
	}


	static class IntPair {
		int start;
		int end;
		IntPair(int start, int end) {
			this.start = start;
			this.end = end;
		}
	}

	private static IntPair getWindow(int winNum, int window, double addIndex) {
		int startIndex = winNum * window;
		int endIndex = startIndex + window;
		if(addIndex > 0) {
			startIndex += winNum / addIndex     ;
			endIndex += winNum / addIndex;
			if (winNum % addIndex < 1) { ///this causes most of issues
				endIndex++;
			}
		}
		return new IntPair(startIndex, endIndex);
	}

	public String toString() {
		if(count == 0) count++;
		return String.format("%.2f",sum.get()/count);
	}

	private static AvgDouble getWindowSum(Writable[] arr, int winNum, int windowSize, double addIndex) {
		IntPair indeces = getWindow(winNum, windowSize, addIndex);
		double sum = 0;
		int count = indeces.end - indeces.start;
		for(int i = indeces.start; i < indeces.end; i++) {
			sum+= ((AvgDouble) arr[i]).getSum();
		}
		return new AvgDouble(sum, count);
	}

	public static void sum2DArrs(AvgDouble[][] to, Writable[][] from, int window, double addIndex) {
		for(int row = 0; row < to.length; row++) {
			IntPair indeces = getWindow(row, window, addIndex);
			if(to[row] == null) to[row] = new AvgDouble[12];
			for(int j = 0; j < to[row].length; j++) if(to[row][j] == null) to[row][j] = new AvgDouble();
			for(int i = indeces.start; i < indeces.end; i++) {
//				if(to[row] == null) to[row] = new AvgDouble[12];
				sum1DArrs(to[row], from[i], 1, 0);
			}
		}
	}

}
