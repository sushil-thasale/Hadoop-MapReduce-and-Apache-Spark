package Prediction;

import java.util.ArrayList;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/*
 * Performs simple data parsing given input record
 */
public class DataHandler {

	public static final int TRAIN = 1;
	public static final int TEST = 2;

	public SamplingEventDetails getSamplingDetails(
			ArrayList<String> refinedValues) {

		SamplingEventDetails sampleingEvent = new SamplingEventDetails();

		// birdPresent
		boolean birdPresent = Boolean.parseBoolean(refinedValues.get(0));
		sampleingEvent.birdPresent = (new BooleanWritable(birdPresent));

		// Sampling ID
		String SAMPLING_EVENT_ID = refinedValues.get(1);
		sampleingEvent.SAMPLING_EVENT_ID = (new Text(SAMPLING_EVENT_ID));

		// Location ID
		String LOC_ID = refinedValues.get(2);
		sampleingEvent.LOC_ID = (new Text(LOC_ID));

		// Time
		double TIME = Double.parseDouble(refinedValues.get(3));
		sampleingEvent.TIME = (new DoubleWritable(TIME));

		// State
		// String STATE = refinedValues.get(3);
		// sampleingEvent.STATE = (new Text(STATE));
		//
		// // County
		// String COUNTY = refinedValues.get(4);
		// sampleingEvent.COUNTY = (new Text(COUNTY));

		// effort hours
		double EFFORT_HRS = Double.parseDouble(refinedValues.get(4));
		sampleingEvent.EFFORT_HRS = (new DoubleWritable(EFFORT_HRS));

		// population per square mile - 955
		double POPULATION_PER_MILE = Double.parseDouble(refinedValues.get(5));
		sampleingEvent.POPULATION_PER_MILE = (new DoubleWritable(
				POPULATION_PER_MILE));

		// housing density - 956
		double HOUSING_DENSITY = Double.parseDouble(refinedValues.get(6));
		sampleingEvent.HOUSING_DENSITY = (new DoubleWritable(HOUSING_DENSITY));

		// housing vacant - 957
		double HOUSING_VACANT = Double.parseDouble(refinedValues.get(7));
		sampleingEvent.HOUSING_VACANT = (new DoubleWritable(HOUSING_VACANT));

		// omernik_l3_ecoregion - 962
		int OMERNIK_L3_ECOREGION = Integer.parseInt(refinedValues.get(8));
		sampleingEvent.OMERNIK_L3_ECOREGION = (new IntWritable(
				OMERNIK_L3_ECOREGION));

		// temp_average - 963
		double TEMP_AVG = Double.parseDouble(refinedValues.get(9));
		sampleingEvent.TEMP_AVG = (new DoubleWritable(TEMP_AVG));

		// flowing_fresh_water - 1091
		int FLOWING_FRESH_IN = Integer.parseInt(refinedValues.get(10));
		sampleingEvent.FLOWING_FRESH_IN = (new IntWritable(FLOWING_FRESH_IN));

		// wetveg_fresh_from - 1094
		int WETVEG_FRESH_FROM = Integer.parseInt(refinedValues.get(11));
		sampleingEvent.WETVEG_FRESH_FROM = (new IntWritable(WETVEG_FRESH_FROM));

		// WETVEG_FRESH_IN - 1095
		int WETVEG_FRESH_IN = Integer.parseInt(refinedValues.get(12));
		sampleingEvent.WETVEG_FRESH_IN = (new IntWritable(WETVEG_FRESH_IN));

		// FLOWING_BRACKISH_FROM - 1096
		int FLOWING_BRACKISH_FROM = Integer.parseInt(refinedValues.get(13));
		sampleingEvent.FLOWING_BRACKISH_FROM = (new IntWritable(
				FLOWING_BRACKISH_FROM));

		// FLOWING_BRACKISH_IN - 1097
		int FLOWING_BRACKISH_IN = Integer.parseInt(refinedValues.get(14));
		sampleingEvent.FLOWING_BRACKISH_IN = (new IntWritable(
				FLOWING_BRACKISH_IN));

		// STANDING_BRACKISH_FROM - 1098
		int STANDING_BRACKISH_FROM = Integer.parseInt(refinedValues.get(15));
		sampleingEvent.STANDING_BRACKISH_FROM = (new IntWritable(
				STANDING_BRACKISH_FROM));

		// STANDING_BRACKISH_IN - 1099
		int STANDING_BRACKISH_IN = Integer.parseInt(refinedValues.get(16));
		sampleingEvent.STANDING_BRACKISH_IN = (new IntWritable(
				STANDING_BRACKISH_IN));

		// WETVEG_BRACKISH_FROM - 1100
		int WETVEG_BRACKISH_FROM = Integer.parseInt(refinedValues.get(17));
		sampleingEvent.WETVEG_BRACKISH_FROM = (new IntWritable(
				WETVEG_BRACKISH_FROM));

		// WETVEG_BRACKISH_IN - 1101
		int WETVEG_BRACKISH_IN = Integer.parseInt(refinedValues.get(18));
		sampleingEvent.WETVEG_BRACKISH_IN = (new IntWritable(WETVEG_BRACKISH_IN));

		// month - 5
		int MONTH = Integer.parseInt(refinedValues.get(19));
		sampleingEvent.MONTH = (new IntWritable(MONTH));

		// index - 
		long index = Long.parseLong(refinedValues.get(20));
		sampleingEvent.index = (new LongWritable(index));

		return sampleingEvent;
	}

	/*
	 * Performs parsing of input data
	 * Doesn't eliminate records with missing fields
	 * Instead set them as an outlier "-999"
	 */
	public ArrayList<String> parse(String record, int type) {

		String[] row = record.split(",");
		ArrayList<String> refinedValues = new ArrayList<>();
		int column = 0;

		try {

			// System.out.println("\n inside parser \n");

			// birdPresent
			column = 26;
			String birdPresent = "false";
			if (type == TRAIN) {
				if (row[column].equals("") || row[column].contains("?")) {
					birdPresent = "false";
				} else if (row[column].contains("X")) {
					birdPresent = "true";
				} else if (Integer.parseInt(row[column]) > 0) {
					birdPresent = "true";
				}
			}
			refinedValues.add(birdPresent); // gives better accuracy

			// sampling id
			column = 0;
			refinedValues.add(row[column]);

			// loc id
			column = 1;
			refinedValues.add(row[column]);

			// time
			column = 7;
			double time = Double.parseDouble(row[column]);
			refinedValues.add("" + time);

			// State
			// refinedValues.add("" + row[9]);
			//
			// // County
			// refinedValues.add("" + row[10]);

			// effort hours
			column = 12;
			double effort_hrs;
			if (row[column].equals("") || row[column].contains("?")) {
				effort_hrs = -999;
			} else
				effort_hrs = Double.parseDouble(row[column]);
			refinedValues.add("" + effort_hrs);

			// population per mile
			column = 955;
			double population_per_mile;
			if (row[column].equals("") || row[column].contains("?")) {
				population_per_mile = -999;
			} else
				population_per_mile = Double.parseDouble(row[column]);
			refinedValues.add("" + population_per_mile);

			// housing density
			column = 956;
			double housing_density;
			if (row[column].equals("") || row[column].contains("?")) {
				housing_density = -999;
			} else
				housing_density = Double.parseDouble(row[column]);
			refinedValues.add("" + housing_density);

			// housing vacant
			column = 957;
			double houses_vacant;
			if (row[column].equals("") || row[column].contains("?")) {
				houses_vacant = -999;
			} else
				houses_vacant = Double.parseDouble(row[column]);
			refinedValues.add("" + houses_vacant);

			// OMERNIK_L3_ECOREGION
			column = 962;
			int omernik;
			if (row[column].equals("") || row[column].contains("?")) {
				omernik = -999;
			} else
				omernik = Integer.parseInt(row[column]);
			refinedValues.add("" + omernik);

			// Average Temp
			column = 963;
			double avg_temp;
			if (row[column].equals("") || row[column].contains("?")) {
				avg_temp = -999;
			} else
				avg_temp = Double.parseDouble(row[column]);
			refinedValues.add("" + avg_temp);

			// Flowing fresh in
			column = 1091;
			int flowing_fresh;
			if (row[column].equals("") || row[column].contains("?")) {
				flowing_fresh = -999;
			} else
				flowing_fresh = Integer.parseInt(row[column]);
			refinedValues.add("" + flowing_fresh);

			// WetVeg fresh from
			column = 1094;
			int wetveg_fresh_from;
			if (row[column].equals("") || row[column].contains("?")) {
				wetveg_fresh_from = -999;
			} else
				wetveg_fresh_from = Integer.parseInt(row[column]);
			refinedValues.add("" + wetveg_fresh_from);

			// WetVeg fresh in
			column = 1095;
			int wetveg_fresh_in;
			if (row[column].equals("") || row[column].contains("?")) {
				wetveg_fresh_in = -999;
			} else
				wetveg_fresh_in = Integer.parseInt(row[column]);
			refinedValues.add("" + wetveg_fresh_in);

			// flowing brackish from
			column = 1096;
			int flowing_brakish_from;
			if (row[column].equals("") || row[column].contains("?")) {
				flowing_brakish_from = -999;
			} else
				flowing_brakish_from = Integer.parseInt(row[column]);
			refinedValues.add("" + flowing_brakish_from);

			// flowing brackish in
			column = 1097;
			int flowing_brakish_in;
			if (row[column].equals("") || row[column].contains("?")) {
				flowing_brakish_in = -999;
			} else
				flowing_brakish_in = Integer.parseInt(row[column]);
			refinedValues.add("" + flowing_brakish_in);

			// standing brackish from
			column = 1098;
			int standing_brackish_from;
			if (row[column].equals("") || row[column].contains("?")) {
				standing_brackish_from = -999;
			} else
				standing_brackish_from = Integer.parseInt(row[column]);
			refinedValues.add("" + standing_brackish_from);

			// standing brackish in
			column = 1099;
			int standing_brackish_in;
			if (row[column].equals("") || row[column].contains("?")) {
				standing_brackish_in = -999;
			} else
				standing_brackish_in = Integer.parseInt(row[column]);
			refinedValues.add("" + standing_brackish_in);

			// wetveg brackish from
			column = 1100;
			int wetveg_brackish_from;
			if (row[column].equals("") || row[column].contains("?")) {
				wetveg_brackish_from = -999;
			} else
				wetveg_brackish_from = Integer.parseInt(row[column]);
			refinedValues.add("" + wetveg_brackish_from);

			// wetveg brackish in
			column = 1101;
			int wetveg_brackish_in;
			if (row[column].equals("") || row[column].contains("?")) {
				wetveg_brackish_in = -999;
			} else
				wetveg_brackish_in = Integer.parseInt(row[column]);
			refinedValues.add("" + wetveg_brackish_in);

			// month
			column = 5;
			int month = Integer.parseInt(row[column]);
			refinedValues.add("" + month);
			
			// index
			column = 1657;
			long index = 0;
			if (type == TEST) {
				index = Long.parseLong(row[column]);
			}
			refinedValues.add(""+index);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return refinedValues;

	}
}
