package cs6240;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/*
 * DoublePairRecord : custom data structure for storing temperature data
 */
public class DoublePairRecord implements WritableComparable<DoublePairRecord> {

	private double tmin;
	private double tmax;
	private long countTMIN;
	private long countTMAX;

	public DoublePairRecord() {
		this.tmin = 0;
		this.tmax = 0;
		this.countTMIN = 0;
		this.countTMAX = 0;
	}
	
	/*
	 * incrementTMIN : used for combining similar temperature records
	 */
	public void incrementTMIN(double tmin) {
		this.tmin += tmin;
		this.countTMIN++;
	}

	/*
	 * incrementTMAX : used for combining similar temperature records
	 */
	public void incrementTMAX(double tmax) {
		this.tmax += tmax;
		this.countTMAX++;
	}

	public double getTMIN() {
		return tmin;
	}

	public double getTMAX() {
		return tmax;
	}

	public long getCountTMIN() {
		return countTMIN;
	}

	public long getCountTMAX() {
		return countTMAX;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		tmin = in.readDouble();
		tmax = in.readDouble();
		countTMIN = in.readLong();
		countTMAX = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeDouble(tmin);
		out.writeDouble(tmax);
		out.writeLong(countTMIN);
		out.writeLong(countTMAX);
	}

	/*
	 * compareTo : used for internal sorting and comparison of two DoublePairRecords 
	 */
	@Override
	public int compareTo(DoublePairRecord other) {

		if (tmin < other.tmin) {
			return -1;
		} else if (tmin > other.tmin) {
			return 1;
		} else if (tmax < other.tmax) {
			return -1;
		} else if (tmax > other.tmax) {
			return 1;
		} else {
			return 0;
		}
	}
}
