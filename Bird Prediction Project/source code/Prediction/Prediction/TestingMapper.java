package Prediction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * Reads test/unlabeled data 
 * Assigns a random # to each record
 * sets the random # as key and emits
 */
public class TestingMapper extends
		Mapper<Object, Text, IntWritable, SamplingEventDetails> {

	private static int totalModels;
	private static DataHandler handler;
	private static Random random;

	public void setup(Context context) {
		totalModels = context.getConfiguration().getInt("models", -10);
		handler = new DataHandler();
		random = new Random();
	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		if (!value.toString().equals("")) {
			handler = new DataHandler();
			ArrayList<String> parsedData = handler.parse(value.toString(),
					handler.TEST);

			if (true && parsedData.size() == 21) {
				SamplingEventDetails samplingDetails = handler
						.getSamplingDetails(parsedData);

				// System.out.println(samplingDetails.toString());

				int rand = random.nextInt(totalModels);

				context.write(new IntWritable(rand), samplingDetails);
			}
		}
	}
}