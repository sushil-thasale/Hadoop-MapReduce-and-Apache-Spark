/*
 * CoarseLock_ProcessorThread_Fibo extends thread class and computes average TMAX 
 * for the assigned load and stores it in centralized data structure "stationMap".
 * Also, add a delay while updating data structure!
 */



public class CoarseLock_ProcessorThread_Fibo extends Thread{
	
	long start, end;
	CentralData c;
	
	/*
	 * @param start : starting position of the assigned subset of input file
	 * @param end : end position of the assigned subset of input file
	 * @param c : instance of CentralData to access stationMap 	
	 */
	public CoarseLock_ProcessorThread_Fibo(long start, long end, CentralData c)
	{
		this.start = start;
		this.end = end;
		this.c = c;
		start();
	}
	
	public void run()
	{
		// creating mutex over centralized data structure "stationMap"
		synchronized (c.stationMap) {
			computeAverageTMAX();
		}
		
	}	
		
	/*
	 * computeAverageTMAX : computes average TMAX of each station
	 * 						from assigned load 
	 */
	public void computeAverageTMAX() {
		
		String record = "";		

		for(long i=start; i<end;i++) {
			
			record = c.csvArrayList.get((int)i);
			if (record.contains("TMAX")) {
				
				String columnValues[] = record.split(",");
				String stationID = columnValues[0];
				double tmax = Integer.parseInt(columnValues[3]);
				
				c.stationMap.get(stationID).computeAverage(tmax);
				fibo();		//adding delay
			}
		}
	}
	
	/*
	 * fibo : computes fibo(17) and adds delay
	 */
	void fibo() {
		int num = 7;
		int x = 0, y = 1, z = 0;

		for (int i = 2; i <= num; i++) {
			z = x + y;
			x = y;
			y = z;
		}
	}


}