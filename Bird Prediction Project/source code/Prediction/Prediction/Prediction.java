package Prediction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Prediction extends Configured implements Tool {

	// # of groups used for modeling data
	static final int randomModel = 20;
	
	// # of classification algorithms used for modeling data
	static final int classficationTypes = 3;
	static int noOfReducers;

	/*
	 * Jobs that creates models using training data	 
	 */
	private boolean trainData(String[] args) throws Exception {

		System.out.println("\n\n\n start of job-1 \n\n\n");

		Configuration conf = new Configuration();
		conf.setInt("models", randomModel);
		conf.setInt("types", classficationTypes);

		Job job = Job.getInstance(conf, "model training");
		job.setJarByClass(Prediction.class);
		job.setMapperClass(TrainingMapper.class);
		job.setReducerClass(TrainingReducer.class);
		job.setPartitionerClass(RandomKeyPartitioner.class);
		job.setNumReduceTasks(noOfReducers);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(SamplingEventDetails.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		// provide training data as input
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// provide path to write intermediate models
		FileOutputFormat
				.setOutputPath(job, new Path(args[2] + "/intermediate"));

		return job.waitForCompletion(true);
	}

	/*
	 * job that predicts the class of unlabeled data
	 */
	private boolean classifyData(String[] args) throws Exception {

		System.out.println("\n\n\n start of job-2 \n\n\n");

		Configuration conf = new Configuration();
		conf.setInt("models", randomModel);
		conf.setInt("types", classficationTypes);

		Job job = Job.getInstance(conf, "model testing");
		job.setJarByClass(Prediction.class);
		job.setMapperClass(TestingMapper.class);
		job.setReducerClass(TestingReducer.class);
		job.setPartitionerClass(RandomKeyPartitioner.class);
		job.setNumReduceTasks(noOfReducers);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(SamplingEventDetails.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// provide path testing data
		FileInputFormat.addInputPath(job, new Path(args[1]));

		// provide path for final output
		FileOutputFormat.setOutputPath(job, new Path(args[2]
				+ "/unOrderedPredictions"));
		return job.waitForCompletion(true);
	}

	/*
	 * Sorts the data as per index
	 * As per the requirements of professor
	 */
	private boolean sortSamplingEvents(String[] args) throws Exception {

		System.out.println("\n\n\n start of job-3 \n\n\n");

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "sorting on index");
		job.setJarByClass(Prediction.class);
		job.setMapperClass(SortingMapper.class);
		job.setReducerClass(SortingReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// provide path testing data
		FileInputFormat.addInputPath(job, new Path(args[2]
				+ "/unOrderedPredictions"));

		// provide path for final output
		FileOutputFormat.setOutputPath(job, new Path(args[2]
				+ "/orderedPredictions"));
		return job.waitForCompletion(true);
	}

	@Override
	public int run(String[] paths) throws Exception {
		return trainData(paths) && classifyData(paths)
				&& sortSamplingEvents(paths) ? 0 : 1;
	}

	public static void main(String[] args) {
		try {

			if (args.length != 4) {
				throw new Error("input format: \n" + "training-data-path "
						+ "test-data-path" + "output-path "
						+ "#-reducers");
			}

			noOfReducers = Integer.parseInt(args[3]);

			System.exit(ToolRunner.run(new Prediction(), args));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
