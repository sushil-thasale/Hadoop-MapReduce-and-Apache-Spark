package cs6240;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SecondarySort {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err
					.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count");
		job.setJarByClass(SecondarySort.class);
		job.setMapperClass(SecondarySortMapper.class);
		job.setMapOutputKeyClass(StationYearPair.class);
		job.setMapOutputValueClass(DoublePairRecord.class);
//		job.setPartitionerClass(SecondarySortPartitioner.class);
		job.setGroupingComparatorClass(StationYearGroupingComparator.class);
		job.setNumReduceTasks(5);
		job.setReducerClass(SecondarySortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

//		FileSystem fs = FileSystem.get(conf);
//
//		fs.delete(new Path(otherArgs[otherArgs.length - 1]), true);

		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class SecondarySortMapper extends
		Mapper<Object, Text, StationYearPair, DoublePairRecord> {

	HashMap<StationYearPair, DoublePairRecord> stationMap;

	public void setup(Context context) {
		stationMap = new HashMap<StationYearPair, DoublePairRecord>();
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// System.out.println(value.toString());
		String columnValues[] = value.toString().split(",");
		String stationID = columnValues[0];
		int year = Integer.parseInt(columnValues[1].substring(0, 4));
		double tValue = Double.parseDouble(columnValues[3]);
		StationYearPair sy = new StationYearPair(stationID, year);

		if (columnValues[2].equals("TMIN")) {

			if (stationMap.containsKey(sy)) {
				stationMap.get(sy).incrementTMIN(tValue);
			} else {
				DoublePairRecord dp = new DoublePairRecord();
				dp.incrementTMIN(tValue);
				stationMap.put(sy, dp);
			}
		} else if (columnValues[2].equals("TMAX")) {

			if (stationMap.containsKey(sy)) {
				stationMap.get(sy).incrementTMAX(tValue);
			} else {
				DoublePairRecord dp = new DoublePairRecord();
				dp.incrementTMAX(tValue);
				stationMap.put(sy, dp);
			}
		}
	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Entry<StationYearPair, DoublePairRecord> entry : stationMap
				.entrySet()) {

			context.write(entry.getKey(), entry.getValue());
		}
	}
}

class SecondarySortPartitioner extends
		Partitioner<StationYearPair, DoublePairRecord> {
	@Override
	public int getPartition(StationYearPair sy, DoublePairRecord dp, int nrt) {

		return (sy.getStation().hashCode() % nrt);
	}
}

class StationYearGroupingComparator extends WritableComparator {

	public StationYearGroupingComparator() {
		super(StationYearPair.class, true);
	}

	@Override
	public int compare(WritableComparable tp1, WritableComparable tp2) {
		StationYearPair sy1 = (StationYearPair) tp1;
		StationYearPair sy2 = (StationYearPair) tp2;
		return sy1.getStation().compareTo(sy2.getStation());
	}
}

class SecondarySortReducer extends
		Reducer<StationYearPair, DoublePairRecord, Text, Text> {

	private Text key_text = new Text();
	private Text result_text = new Text();
	private static StringBuilder summary;

	private static double sumTMIN;
	private static double sumTMAX;
	private static long countTMIN;
	private static long countTMAX;
	private static double avgTMIN;
	private static double avgTMAX;
	private static int currentYear;

	public void reduce(StationYearPair key, Iterable<DoublePairRecord> values,
			Context context) throws IOException, InterruptedException {

		summary = new StringBuilder("[");
		currentYear = 0;
		clearVariables();

		for (DoublePairRecord val : values) {

			if (key.getYear() != currentYear) {

				if (currentYear != 0) {

					computeAvg();
				}

				currentYear = key.getYear();
				clearVariables();
				aggregateValues(val);

			} else {
				aggregateValues(val);
			}
		}

		computeAvg();

		summary.replace(summary.length()-1, summary.length(), "]");
		
		key_text.set(key.getStation());
		result_text.set(summary.toString());
		context.write(key_text, result_text);
	}

	public void aggregateValues(DoublePairRecord val) {

		sumTMIN += val.getTMIN();
		sumTMAX += val.getTMAX();
		countTMIN += val.getCountTMIN();
		countTMAX += val.getCountTMAX();
	}

	public void clearVariables() {
		sumTMIN = 0;
		sumTMAX = 0;
		countTMIN = 0;
		countTMAX = 0;
		avgTMIN = 0;
		avgTMAX = 0;
	}

	public void handleNaN() {
		if (Double.isNaN(avgTMAX))
			avgTMAX = 0.0;

		if (Double.isNaN(avgTMIN))
			avgTMIN = 0.0;
	}

	public void computeAvg() {
		avgTMAX = (double) sumTMAX / countTMAX;
		avgTMIN = (double) sumTMIN / countTMIN;

		handleNaN();

		summary.append("(" + currentYear + ", " + avgTMIN + ", " + avgTMAX
				+ "),");
	}
}
