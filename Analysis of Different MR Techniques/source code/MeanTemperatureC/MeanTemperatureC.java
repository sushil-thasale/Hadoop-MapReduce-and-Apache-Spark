/*
 * MeanTemperatureC : Computes average TMIN and TMAX for each station 
 * 					  without using In-Mapper Combiner
 * 
 */
package cs6240;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MeanTemperatureC {
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
		job.setJarByClass(MeanTemperatureC.class);
		job.setMapperClass(MeanTempMapperC.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoublePairRecord.class);
		job.setReducerClass(MeanTempReducerC.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

/*
 * MeanTempMapperC : Combines DoublePairRecords for specific stationID
 * 					 Stores combined DoublePairRecords into HashMap with corresponding stationID
 * 					 Emits stationID and corresponding DoublePairRecord
 */
class MeanTempMapperC extends Mapper<Object, Text, Text, DoublePairRecord> {

	HashMap<String, DoublePairRecord> stationMap;
	Text key = new Text();

	public void setup(Context context) {
		stationMap = new HashMap<String, DoublePairRecord>();
	}

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String columnValues[] = value.toString().split(",");
		String stationID = columnValues[0];
		double tValue = Double.parseDouble(columnValues[3]);

		if (columnValues[2].equals("TMIN")) {

			if (stationMap.containsKey(stationID)) {
				stationMap.get(stationID).incrementTMIN(tValue);
			} else {
				DoublePairRecord dp = new DoublePairRecord();
				dp.incrementTMIN(tValue);
				stationMap.put(stationID, dp);
			}
		} else if (columnValues[2].equals("TMAX")) {

			if (stationMap.containsKey(stationID)) {
				stationMap.get(stationID).incrementTMAX(tValue);
			} else {
				DoublePairRecord dp = new DoublePairRecord();
				dp.incrementTMAX(tValue);
				stationMap.put(stationID, dp);
			}
		}
	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Entry<String, DoublePairRecord> entry : stationMap.entrySet()) {

			key.set(entry.getKey()); 
			context.write(key, entry.getValue());
		}
	}
}

/*
 * MeanTempReducerC : Combines all the DoublePairRecords of a particular station
 * 					  Computes average tmin and average tmax 
 * 					  Emits stationID and corresponding average tmin and tmax	
 */
class MeanTempReducerC extends Reducer<Text, DoublePairRecord, Text, Text> {

	private Text result = new Text();

	public void reduce(Text key, Iterable<DoublePairRecord> values,
			Context context) throws IOException, InterruptedException {

		double sumTMIN = 0;
		double sumTMAX = 0;
		long countTMIN = 0;
		long countTMAX = 0;
		double avgTMIN = 0;
		double avgTMAX = 0;

		for (DoublePairRecord val : values) {

			sumTMIN += val.getTMIN();
			sumTMAX += val.getTMAX();
			countTMIN += val.getCountTMIN();
			countTMAX += val.getCountTMAX();
		}

		avgTMAX = (double) sumTMAX / countTMAX;
		avgTMIN = (double) sumTMIN / countTMIN;
		
		result.set(String.valueOf(avgTMIN) + "\t" + String.valueOf(avgTMAX));
		context.write(key, result);
	}
}
