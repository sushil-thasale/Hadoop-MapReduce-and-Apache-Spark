package Prediction;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*
 * Stores all the features required to train model
 */
public class SamplingEventDetails implements Writable {

	BooleanWritable birdPresent; // 17
	Text SAMPLING_EVENT_ID;
	Text LOC_ID;
	DoubleWritable TIME;
	// Text STATE;
	// Text COUNTY;
	DoubleWritable EFFORT_HRS;
	DoubleWritable POPULATION_PER_MILE;
	DoubleWritable HOUSING_DENSITY;
	DoubleWritable HOUSING_VACANT;
	IntWritable OMERNIK_L3_ECOREGION; // check if between 1-120
	DoubleWritable TEMP_AVG;
	IntWritable FLOWING_FRESH_IN;
	IntWritable WETVEG_FRESH_FROM;
	IntWritable WETVEG_FRESH_IN;
	IntWritable FLOWING_BRACKISH_FROM;
	IntWritable FLOWING_BRACKISH_IN;
	IntWritable STANDING_BRACKISH_FROM;
	IntWritable STANDING_BRACKISH_IN;
	IntWritable WETVEG_BRACKISH_FROM;
	IntWritable WETVEG_BRACKISH_IN;
	IntWritable MONTH;
	LongWritable index;
	


	public SamplingEventDetails() {

		birdPresent = new BooleanWritable();
		SAMPLING_EVENT_ID = new Text();
		LOC_ID = new Text();
		TIME = new DoubleWritable();
		// STATE = new Text();
		// COUNTY = new Text();
		EFFORT_HRS = new DoubleWritable();
		POPULATION_PER_MILE = new DoubleWritable();
		HOUSING_DENSITY = new DoubleWritable();
		HOUSING_VACANT = new DoubleWritable();
		OMERNIK_L3_ECOREGION = new IntWritable(); // check if between 1-120
		TEMP_AVG = new DoubleWritable();
		FLOWING_FRESH_IN = new IntWritable();
		WETVEG_FRESH_FROM = new IntWritable();
		WETVEG_FRESH_IN = new IntWritable();
		FLOWING_BRACKISH_FROM = new IntWritable();
		FLOWING_BRACKISH_IN = new IntWritable();
		STANDING_BRACKISH_FROM = new IntWritable();
		STANDING_BRACKISH_IN = new IntWritable();
		WETVEG_BRACKISH_FROM = new IntWritable();
		WETVEG_BRACKISH_IN = new IntWritable();
		MONTH = new IntWritable();	
		index = new LongWritable();
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		birdPresent.readFields(in);
		SAMPLING_EVENT_ID.readFields(in);
		LOC_ID.readFields(in);
		TIME.readFields(in);
		// STATE.readFields(in);
		// COUNTY.readFields(in);
		EFFORT_HRS.readFields(in);
		POPULATION_PER_MILE.readFields(in);
		HOUSING_DENSITY.readFields(in);
		HOUSING_VACANT.readFields(in);
		OMERNIK_L3_ECOREGION.readFields(in); // check if between 1-120
		TEMP_AVG.readFields(in);
		FLOWING_FRESH_IN.readFields(in);
		WETVEG_FRESH_FROM.readFields(in);
		WETVEG_FRESH_IN.readFields(in);
		FLOWING_BRACKISH_FROM.readFields(in);
		FLOWING_BRACKISH_IN.readFields(in);
		STANDING_BRACKISH_FROM.readFields(in);
		STANDING_BRACKISH_IN.readFields(in);
		WETVEG_BRACKISH_FROM.readFields(in);
		WETVEG_BRACKISH_IN.readFields(in);
		MONTH.readFields(in);	
		index.readFields(in);	
	}

	@Override
	public void write(DataOutput out) throws IOException {

		birdPresent.write(out);
		SAMPLING_EVENT_ID.write(out);
		LOC_ID.write(out);
		TIME.write(out);
		// STATE.write(out);
		// COUNTY.write(out);
		EFFORT_HRS.write(out);
		POPULATION_PER_MILE.write(out);
		HOUSING_DENSITY.write(out);
		HOUSING_VACANT.write(out);
		OMERNIK_L3_ECOREGION.write(out); // check if between 1-120
		TEMP_AVG.write(out);
		FLOWING_FRESH_IN.write(out);
		WETVEG_FRESH_FROM.write(out);
		WETVEG_FRESH_IN.write(out);
		FLOWING_BRACKISH_FROM.write(out);
		FLOWING_BRACKISH_IN.write(out);
		STANDING_BRACKISH_FROM.write(out);
		STANDING_BRACKISH_IN.write(out);
		WETVEG_BRACKISH_FROM.write(out);
		WETVEG_BRACKISH_IN.write(out);
		MONTH.write(out);	
		index.write(out);

	}

	@Override
	public String toString() {
		return "" + SAMPLING_EVENT_ID.toString() + "," + LOC_ID + "," + TIME
				+ "," + EFFORT_HRS + "," + POPULATION_PER_MILE + ","
				+ HOUSING_DENSITY + "," + HOUSING_VACANT + ","
				+ OMERNIK_L3_ECOREGION + "," + TEMP_AVG + ","
				+ FLOWING_FRESH_IN + "," + WETVEG_FRESH_FROM + ","
				+ WETVEG_FRESH_IN + "," + FLOWING_BRACKISH_FROM + ","
				+ FLOWING_BRACKISH_IN + "," + STANDING_BRACKISH_FROM + ","
				+ STANDING_BRACKISH_IN + "," + WETVEG_BRACKISH_FROM + ","
				+ WETVEG_BRACKISH_IN + "," + MONTH + "," + birdPresent
				+","+index;
	}
}
