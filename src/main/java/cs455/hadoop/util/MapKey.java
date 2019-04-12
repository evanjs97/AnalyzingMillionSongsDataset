package cs455.hadoop.util;

public class MapKey implements Comparable<MapKey>{
	private double value;
	private String key;
	private boolean increasing;

	/**
	 *
	 * @param key = key for this keypair
	 * @param value for this keypair
	 * @param increasing if true: sort from min->max else: sort from max->min
	 */
	public MapKey(String key, double value, boolean increasing) {
		this.value = value;
		this.key = key;
		this.increasing = increasing;
	}

	/**
	 * return correct ordering for MapKey object
	 * @param o the object to compare to
	 * @return the correct ordering
	 */
	@Override
	public int compareTo(MapKey o) {
		if(this.key.equals(o.key) || this.value == o.value) return 0;
		else if(increasing) {
			if(this.value < o.value) return -1;
			else return 1;
		}else {
			if(this.value > o.value) return -1;
			else return 1;
		}
	}

	/**
	 * returns true if MapKey's key.equals(o's key)
	 * @param o the MapKey to compare with this one
	 * @return true if equal, else false
	 */
	@Override
	public boolean equals(Object o) {
		if(o.equals(this)) return true;
		else if(!(o instanceof MapKey)) return false;
		else {
			MapKey other = (MapKey) o;
			return other.key.equals(this.key);
		}
	}

	public void add(double val) {
		this.value += val;
	}

	public void setValue(double newValue) {
		this.value = newValue;
	}

	public double getValue() { return this.value; }

	public String getKey() { return this.key; }

	@Override
	public int hashCode() {
		return key.hashCode();
	}
}
