/*
 * This Class is responsible for :- 
 * 	- loading input file and storing it into memory 
 * 	- maintaining a centralized data structure "stationMap" for all threads  
 */



import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import java.util.Iterator;

public class CentralData {

	// stores input file "1912.csv"
	public ArrayList<String> csvArrayList = new ArrayList<String>();

	// stores average TMAX for all stations
	public HashMap<String, RecordEntry> stationMap = new HashMap<String, RecordEntry>();
	
	/*
	 * loadInMemory : loads input file into memory
	 * 
	 * @param csvFile : (String) name of input file
	 */
	public void loadInMemory(String csvFile) {

		String csvline = "";
		BufferedReader bReader = null;

		try {

			bReader = new BufferedReader(new FileReader(csvFile));

			while ((csvline = bReader.readLine()) != null) {

				csvArrayList.add(csvline);

				// adding keys to stationMap
				if (csvline.contains("TMAX")) {
					String columnValues[] = csvline.split(",");
					String stationID = columnValues[0];

					if (!stationMap.containsKey(stationID)) {
						stationMap.put(stationID, new RecordEntry());
					}
				}
			}
		} catch (FileNotFoundException e) {

			e.printStackTrace();

		} catch (Exception e) {

			e.printStackTrace();

		} finally {

			if (bReader != null) {
				try {
					bReader.close();

				} catch (IOException e) {

					e.printStackTrace();
				}
			}

		}

	}
	
	/*
	 * clearStationMap : clears all values of stationMap
	 */
	public void clearStationMap()
	{
		Iterator itr = stationMap.entrySet().iterator();

		while (itr.hasNext()) {
			Map.Entry pair = (Map.Entry) itr.next();
			RecordEntry r = (RecordEntry) pair.getValue();
			r.clearRecord();
		}		
	}
}