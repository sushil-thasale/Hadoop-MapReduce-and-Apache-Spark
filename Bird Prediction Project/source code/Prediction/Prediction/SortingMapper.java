package Prediction;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * This class is used to sort the records as the index of records in "unlabelled.csv.bz2"
 */
public class SortingMapper extends Mapper<Object, Text, LongWritable, Text> {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String values[] = value.toString().split(",");
		long index = Long.parseLong(values[2].trim());
		String samplingPredictions = values[0] + "," + values[1];
		String header = "SAMPLING_EVENT_ID, SAW_AGELAIUS_PHOENICEUS";
		
		// print the header as first record
		if(index == 0)
			context.write(new LongWritable(index-1), new Text(header));
		
		context.write(new LongWritable(index), new Text(samplingPredictions));
	}
}