package Prediction;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortingReducer extends Reducer<LongWritable, Text, Text, Text> {

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// writes the records in ascending order of index #
		for (Text val : values) {
			context.write(val, new Text());
		}
	}
}
