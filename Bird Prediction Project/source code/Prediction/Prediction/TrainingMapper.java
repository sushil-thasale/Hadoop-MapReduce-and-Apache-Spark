package Prediction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * assign as random # to each record and emit
 * creates random trees for building random forest
 */
public class TrainingMapper extends
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
					handler.TRAIN);

			System.out.println("correct value found");

			if (true && parsedData.size() == 21) {
				SamplingEventDetails samplingDetails = handler
						.getSamplingDetails(parsedData);

				// generate a random #
				int rand = random.nextInt(totalModels);

				// use that random # as key
				context.write(new IntWritable(rand), samplingDetails);
			}

		}
	}

	// used for printing the output of parse()
	public void printArray(ArrayList<String> array) {
		for (String a : array) {
			System.out.print(a + ",");
		}
	}
}