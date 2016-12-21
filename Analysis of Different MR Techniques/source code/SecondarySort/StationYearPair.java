package cs6240;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StationYearPair implements WritableComparable<StationYearPair> {

	private String stationID;
	private int year;
	
	public StationYearPair() {
		this.stationID = "";
		this.year = 0;		
	}

	public StationYearPair(String stationID, int year) {
		this.stationID = stationID;
		this.year = year;		
	}
	
	public void setStation(String stationID) {
		this.stationID = stationID;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public String getStation() {
		return this.stationID;
	}

	public int getYear() {
		return this.year;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

		stationID = in.readUTF();
		year = in.readInt();		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub

		out.writeUTF(stationID);
		out.writeInt(year);		
	}

	@Override
	public int compareTo(StationYearPair other) {
		// TODO Auto-generated method stub

		if (stationID.compareTo(other.stationID) < 0) {
			return -1;
		} else if (stationID.compareTo(other.stationID) > 0) {
			return 1;
		} else if (year < other.year) {
			return -1;
		} else if (year > other.year) {
			return 1;
		} else {
			return 0;
		}
	}
	
	public String toString() {
        return ("" + this.stationID + "\t" + this.year);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + year;
        result = prime * result + ((stationID == null) ? 0 : stationID.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final StationYearPair other = (StationYearPair) obj;
        if (year != other.year)
            return false;
        if (stationID == null) {
            if (other.stationID != null)
                return false;
        } else if (!stationID.equals(other.stationID))
            return false;
        return true;
    }
    
}

