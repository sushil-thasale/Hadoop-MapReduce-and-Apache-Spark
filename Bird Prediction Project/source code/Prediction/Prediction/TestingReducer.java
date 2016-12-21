package Prediction;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import weka.classifiers.Classifier;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;

/*
 * This class predicts the class of incoming record
 * 
 */
public class TestingReducer extends
		Reducer<IntWritable, SamplingEventDetails, Text, Text> {

	private static ArrayList<Classifier> classifiers;
	private static Instances set;
	private static int totalModels;
	private static int totalTypes;

	public void setup(Context context) {

		totalModels = context.getConfiguration().getInt("models", -10);
		totalTypes = context.getConfiguration().getInt("types", -10);
		set = InstanceHandler.initInstances();
		classifiers = new ArrayList<Classifier>();

		// load all the classifiers from disk
		try {
			// for each type of classification algorithm used
			for (int type = 0; type < totalTypes; type++) {
				// for each random tree in random forest
				for (int model = 0; model < totalModels; model++) {
					// System.out.println("inputing model "+i);
					classifiers.add(getClassifier(model, type));
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("in set up");
		}
	}

	private Classifier getClassifier(int model, int type) throws Exception {
		try {
			// System.out.println("reading models");
			Path path = new Path("models-" + type + "/" + model);
			FileSystem fileSystem = path.getFileSystem(new Configuration());
			FSDataInputStream inputStream = fileSystem.open(path);
			return (Classifier) SerializationHelper.read(inputStream);
		} catch (Exception e) {
			System.out.println("\n file not found \n");
			e.printStackTrace();
			return null;
		}

	}

	@Override
	public void reduce(IntWritable key, Iterable<SamplingEventDetails> values,
			Context context) throws IOException, InterruptedException {

		// probability that the bird will be seen
		double presentProbability;
		// probability that the bird will not be seen
		double absentProbability;

		for (SamplingEventDetails samplingEvent : values) {

			presentProbability = 0;
			absentProbability = 0;

			Instance instance = InstanceHandler.getInstance(samplingEvent, set);
			instance.setDataset(set);

			String output = samplingEvent.SAMPLING_EVENT_ID.toString() + ",";

			try {
				// accumulate probabilities generated over different classification models
				for (Classifier c : classifiers) {
					double[] probabilities = c
							.distributionForInstance(instance);

					presentProbability += probabilities[0];
					absentProbability += probabilities[1];
				}

				// System.out.println(ones + "\t" + zeros);
				
				// determine class based on larger probability
				String prediction = presentProbability > absentProbability ? "1"
						: "0";
				String index = "," + samplingEvent.index.toString();
				context.write(new Text(output + prediction + index), new Text(
						""));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
