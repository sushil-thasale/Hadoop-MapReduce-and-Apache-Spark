/*
 * DataPrinters contains all functions used for printing/writing
 * ArrayList and HashMap 
 */



import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

public class DataPrinters {

	// printArrayList : prints array lists using iterator
	public void printArrayList(CentralData c) {

		Iterator<String> itr = c.csvArrayList.iterator();

		while (itr.hasNext()) {
			System.out.println(itr.next());
		}
	}

	// printAverageTMAX : prints HashMap using iterator
	public void printAverageTMAX(CentralData c) {

		Iterator hmItr = c.stationMap.entrySet().iterator();

		while (hmItr.hasNext()) {
			Map.Entry pair = (Map.Entry) hmItr.next();
			String key = pair.getKey().toString();
			RecordEntry r = (RecordEntry) pair.getValue();

			System.out.println(key + " : " + r.average);
		}
	}

	/*
	 * writeAverageTMAX : writes HashMap "stationMap" to give file
	 * 
	 * @param c: instance of CentralData
	 * 
	 * @param opFile: name of output file
	 */
	public void writeAverageTMAX(CentralData c, String opFile) {

		Iterator hmItr = c.stationMap.entrySet().iterator();
		try {
			PrintWriter pWriter = new PrintWriter(opFile);

			while (hmItr.hasNext()) {
				Map.Entry pair = (Map.Entry) hmItr.next();
				String key = pair.getKey().toString();
				RecordEntry r = (RecordEntry) pair.getValue();
				String op = key + " : " + r.average;

				// System.out.println(key + " : " + r.average);
				pWriter.println(op);
			}

			pWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * dumpTimeTracker : prints minimum, maximum, average time of execution over
	 * 10 iterations
	 * 
	 * @param timeTracker : array storing execution time for each iteration
	 */
	public void dumpTimeTracker(long[] timeTracker) {

		long min = 100000000, max = 0;
		long sum = 0;

		for (int i = 0; i < timeTracker.length; i++) {
			
			sum += timeTracker[i];
			if (timeTracker[i] < min)
				min = timeTracker[i];

			if (timeTracker[i] > max)
				max = timeTracker[i];

		}
		double averageTime = (double) sum / timeTracker.length;

		System.out.println("Minimum time : " + min);
		System.out.println("Maximum time : " + max);
		System.out.println("Average Time : " + averageTime);
	}

}