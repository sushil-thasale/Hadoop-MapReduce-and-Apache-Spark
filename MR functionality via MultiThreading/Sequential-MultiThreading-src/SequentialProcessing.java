/*
 * SequentialProcessing : 
 * 		- loads input file into ArrayList "csvArrayList"
 * 		- computes average TMAX for each station and store it in "stationMap" (HashMap) 
 */



import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class SequentialProcessing{

	public void SequentialExecution(CentralData c) throws Exception {

		final String opFileName 	= "output/SequentialThreadOP.txt";

		long timeTracker[] 	= new long[10]; // array to keep track of time
		long startTime 	= 0, endTime 	= 0;
		int iterations = 10;
		
		
				
		for (int i = 0; i < iterations; i++) {

			c.clearStationMap();
			
			startTime = System.currentTimeMillis();

			computeAverageTMAX(c.csvArrayList, c.stationMap);

			endTime = System.currentTimeMillis();

			timeTracker[i] = endTime - startTime;												
		}
		
		DataPrinters dPrinter = new DataPrinters();
		dPrinter.writeAverageTMAX(c, opFileName);
		dPrinter.dumpTimeTracker(timeTracker);
		
		c.clearStationMap();
	}

	/*
	 *  computeAverageTMAX : computes average TMAX for each station and stores in stationMap
	 *  @param csvArrayList : in-memory copy of input file
	 *  @param stationMap : HashMap used to store average TMAX of all stations 
	 */
	public void computeAverageTMAX(ArrayList<String> csvArrayList,
			HashMap<String, RecordEntry> stationMap) {
		
		String record = "";
		Iterator<String> itr = csvArrayList.iterator();

		while (itr.hasNext()) {
			record = itr.next();
			if (record.contains("TMAX")) {
				String columnValues[] = record.split(",");
				String stationID = columnValues[0];
				double tmax = Double.parseDouble(columnValues[3]);

				stationMap.get(stationID).computeAverage(tmax);
			}
		}
	}
}