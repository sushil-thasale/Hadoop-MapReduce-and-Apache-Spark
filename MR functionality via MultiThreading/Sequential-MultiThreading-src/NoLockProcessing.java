/*
 * NoLockProcessing : 
 * 		- loads input file into ArrayList "csvArrayList"
 * 		- spawns 8 threads
 * 		- divide input into 8 equal size partitions
 * 		- assign equal work to each thread
 * 		- threads are not synchronized
 * 		- refer ProcessorThread.java for thread synchronization 
 */



import java.util.ArrayList;

public class NoLockProcessing {

	public void NoLockExecution(CentralData c) throws Exception {

		final int totalCores 		= 8;	// threads to be spawned
		final String opFileName 	= "output/NoLockThreadOP.txt";
		
		long timeTracker[] 	= new long[10]; // array to keep track of time
		long startPos 	= 0, endPos 	= 0;
		long startTime 	= 0, endTime 	= 0;

		long inputSize = c.csvArrayList.size();

		// creating partitions for each thread
		long partitionSize = inputSize / totalCores;

		// executing 10 times to compute average time
		for (int iteration = 0; iteration < 10; iteration++) {

			c.clearStationMap();
			
			startPos = 0;
			endPos = 0;
			startTime = System.currentTimeMillis();
			
			ArrayList<NoLock_ProcessorThread> pThreads = new ArrayList<NoLock_ProcessorThread>();

			// assigning equal work to each threads
			for (int i = 0; i < totalCores; i++) {

				startPos = endPos;

				if (i == 7)
					endPos = inputSize;
				else
					endPos = startPos + partitionSize;

				pThreads.add(new NoLock_ProcessorThread(startPos, endPos, c));
			}

			// waiting for thread completion
			for (int i = 0; i < totalCores; i++) {

				pThreads.get(i).join();
			}

			endTime = System.currentTimeMillis();

			timeTracker[iteration] = endTime - startTime;
		}
		
		// printing time logs and writing result to output file
		DataPrinters dPrinter = new DataPrinters();
		dPrinter.dumpTimeTracker(timeTracker);
		dPrinter.writeAverageTMAX(c, opFileName);
		
		c.clearStationMap();
	}
}
