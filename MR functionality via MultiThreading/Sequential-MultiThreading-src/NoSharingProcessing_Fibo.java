/*
 * NoSharingProcessing_Fibo : 
 * 		- loads input file into ArrayList "csvArrayList"
 * 		- spawns 8 threads
 * 		- divide input into 8 equal size partitions
 * 		- assign equal work to each thread
 * 		- threads save data to their dedicated data structure i.e no sharing
 * 		- refer ProcessorThread.java for thread synchronization 
 * 		- each thread returns a hashmap which is merged into "stationMap"
 * 		- adds a delay while updating dedicated data structure
 */




import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class NoSharingProcessing_Fibo {

	public void NoSharingExecution_Fibo(CentralData c) throws Exception {

		final int totalCores 		= 8;	// threads to be spawned
		final String opFileName 	= "output/NoSharing_Fibo.txt";
		
		long timeTracker[] 	= new long[10]; // array to keep track of time
		long startPos 	= 0, endPos 	= 0;
		long startTime 	= 0, endTime 	= 0;

		long inputSize = c.csvArrayList.size();

		// creating partitions for each thread
		long partitionSize = inputSize / totalCores;

		for (int iteration = 0; iteration < 10; iteration++) {

			c.clearStationMap();
			
			startPos = 0;
			endPos = 0;
			startTime = System.currentTimeMillis();

			ArrayList<NoSharing_ProcessorThread_Fibo> pThreads = new ArrayList<NoSharing_ProcessorThread_Fibo>();

			for (int i = 0; i < totalCores; i++) {

				startPos = endPos;

				if (i == 7)
					endPos = inputSize;
				else
					endPos = startPos + partitionSize;

				pThreads.add(new NoSharing_ProcessorThread_Fibo(startPos, endPos, c));
			}

			for (int i = 0; i < totalCores; i++) {

				pThreads.get(i).join();
			}

			ArrayList<HashMap<String, RecordEntry>> threadMapList = new ArrayList<HashMap<String, RecordEntry>>();

			for (int i = 0; i < totalCores; i++) {
				threadMapList.add(pThreads.get(i).getThreadMap());
			}

			mergeThreadMaps(threadMapList, c);

			endTime = System.currentTimeMillis();

			timeTracker[iteration] = endTime - startTime;
						
		}

		DataPrinters dPrinter = new DataPrinters();
		dPrinter.dumpTimeTracker(timeTracker);
		dPrinter.writeAverageTMAX(c, opFileName);
		
		c.clearStationMap();
	}

	/*
	 * mergeThreadMaps : merges each thread's dedicated hashmap 
	 * 					 into centralized data structure 
	 */
	public static void mergeThreadMaps(
			ArrayList<HashMap<String, RecordEntry>> threadMapList, CentralData c) {
		Iterator itr = c.stationMap.entrySet().iterator();

		while (itr.hasNext()) {
			Map.Entry cPair = (Map.Entry) itr.next();
			String cStationID = cPair.getKey().toString();
			RecordEntry cRecordEntry = (RecordEntry) cPair.getValue();

			double sum = 0;
			long count = 0;

			for (int i = 0; i < threadMapList.size(); i++) {
				HashMap<String, RecordEntry> threadMap = threadMapList.get(i);

				if (threadMap.get(cStationID) != null) {
					double localAvg = threadMap.get(cStationID).getAverage();
					long localCount = threadMap.get(cStationID).getCount();

					sum += localAvg * localCount;
					count += localCount;
				}

			}

			cRecordEntry.setAverage((double) sum / count);
			cRecordEntry.setCount(count);
		}
	}
}
