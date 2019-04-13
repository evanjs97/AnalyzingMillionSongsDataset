package cs455.hadoop.util.pair;

import org.apache.hadoop.io.Text;

public class TwoPairStringInt {
	private String str;
	private int val;

	public TwoPairStringInt(String str, int val) {
		this.str = str;
		this.val = val;
	}

	public TwoPairStringInt(Text text) {
		String entry = text.toString();
		String[] arr = entry.split("\t");
		str = arr[0];
		val = Integer.parseInt(arr[1]);
	}

	public void add(int val) {
		this.val += val;
	}

	public int getVal() {
		return val;
	}

	public String getStr() {
		return str;
	}
}
