/*
 * RecordEntry computes and stores average TMAX
 */



public class RecordEntry {

	long count;
	double average;

	public RecordEntry() {

		count = 0;
		average = 0;
	}

	/*
	 * computeAverage : calculates and stores average TMAX
	 * 
	 * @param tmax : temperature value
	 */
	public void computeAverage(double tmax) {

		double totalSum = average * count + tmax;
		count++;
		average = (double) totalSum / count;
	}

	/*
	 * clearRecord : reset average and count to 0
	 */
	public void clearRecord() {

		count = 0;
		average = 0;
	}
	
	public long getCount()
	{
		return count;
	}
	
	public double getAverage()
	{
		return average;
	}
	
	public void setCount(long count)
	{
		this.count = count;
	}
	
	public void setAverage(double average)
	{
		this.average = average;
	}
}