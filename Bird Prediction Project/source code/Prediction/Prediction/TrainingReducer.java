package Prediction;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.trees.RandomForest;
import weka.classifiers.trees.RandomTree;
import weka.core.Instances;

/*
 * creates model using 3 different classification algorithms
 * writes those models to local disk
 */
public class TrainingReducer extends
		Reducer<IntWritable, SamplingEventDetails, Text, BytesWritable> {

	private static int totalTypes;

	public void setup(Context context) {
		totalTypes = context.getConfiguration().getInt("types", -10);
	}

	// builds classification models
	private Classifier classify(Instances trainingSet, int model)
			throws Exception {

		// 76 combined
		switch (model) {
		case 0:
			return classifyNaiveBayes(trainingSet); // 71 - alone
		case 1:
			return classifyRandomTree(trainingSet); // 75 - alone
		case 2:
			return classifyRandomForest(trainingSet); // 76 - alone
		default:
			return null;
		}
	}

	// given a training set builds naive bayes classifier
	private Classifier classifyNaiveBayes(Instances trainingSet)
			throws Exception {
		NaiveBayes naiveBayes = new NaiveBayes();
		naiveBayes.setUseKernelEstimator(true);
		naiveBayes.buildClassifier(trainingSet);
		return naiveBayes;
	}

	// given a training set builds a random forest classifier
	private Classifier classifyRandomForest(Instances trainingSet)
			throws Exception {
		RandomForest randomForest = new RandomForest();
		randomForest.setMaxDepth(15);
		randomForest.setNumTrees(15);
		randomForest.setNumFeatures(17);
		randomForest.buildClassifier(trainingSet);
		return randomForest;
	}

	// given a training set builds a random tree classifier
	private Classifier classifyRandomTree(Instances trainingSet)
			throws Exception {
		RandomTree randomTree = new RandomTree();
		randomTree.setMaxDepth(0);
		randomTree.setKValue(8);
		randomTree.setMinNum(1);
		randomTree.buildClassifier(trainingSet);
		return randomTree;
	}

	@Override
	public void reduce(IntWritable key, Iterable<SamplingEventDetails> values,
			Context context) throws IOException, InterruptedException {

		Instances trainingSet = InstanceHandler.initInstances();

		// System.out.println("in model reducer");

		for (SamplingEventDetails eventDetails : values) {

			// System.out.println(eventDetails.toString());

			trainingSet.add(InstanceHandler.getInstance(eventDetails,
					trainingSet));
		}

		try {
			// for a training set create 3 different classifiers
			// write each classifier to local disk
			for (int type = 0; type < totalTypes; type++) {
				Classifier model = classify(trainingSet, type);
				FileSystem fileSystem = FileSystem.get(URI.create("models/"),
						new Configuration());
				FSDataOutputStream fsDataOutputStream = fileSystem
						.create(new Path("models-" + type + "/"
								+ key.toString()));
				weka.core.SerializationHelper.write(fsDataOutputStream, model);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
