package cs6240;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	public static void main(String[] args) throws Exception {

		// job-1
		Configuration conf = new Configuration();
		Job job1 = new Job(conf, "Word Count");
		job1.setJarByClass(WordCount.class);
		job1.setMapperClass(TokenizerMapper1.class);
		job1.setReducerClass(IntSumReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job1, new Path("alice.txt"));
		FileOutputFormat.setOutputPath(job1, new Path("output-job1"));
		job1.waitForCompletion(true);

		// job-2
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "Sort Count");
		job2.setJarByClass(WordCount.class);
		job2.setMapperClass(TokenizerMapper2.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);		
		job2.setPartitionerClass(SortPartitioner.class);
		job2.setNumReduceTasks(2);
		job2.setReducerClass(IntSumReducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job2, new Path(
				"output-job1/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path("output-job2"));
		job2.waitForCompletion(true);
		
		FileSystem fs = FileSystem.get(conf2);
		fs.delete(new Path("output-job1"), true);
		System.exit(0);
	}
}

class TokenizerMapper1 extends Mapper<Object, Text, Text, IntWritable> {
	private final static Pattern nw1 = Pattern.compile("[^'a-zA-Z]");
	private final static Pattern nw2 = Pattern.compile("(^'+|'+$)");
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());

		while (itr.hasMoreTokens()) {
			Matcher mm1 = nw1.matcher(itr.nextToken());
			Matcher mm2 = nw2.matcher(mm1.replaceAll(""));
			String ww = mm2.replaceAll("").toLowerCase();

			if (!ww.equals("")) {
				word.set(ww);
				context.write(word, one);
			}
		}
	}
}

class IntSumReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}

class TokenizerMapper2 extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	private LongWritable count = new LongWritable();
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String words[] = value.toString().split("	"); 
		
		word.set(words[0].toString());		
		count.set(Integer.parseInt(words[1]));
		
		context.write(count, word);
	}
}

class SortPartitioner extends Partitioner<LongWritable, Text> {
	@Override
	public int getPartition(LongWritable key, Text value, int nrt) {

		if (key.get() <= 10)
			return (2%nrt);
		else
			return (1%nrt);
	}
}

class IntSumReducer2 extends Reducer<LongWritable, Text, Text, LongWritable> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		for (Text val : values) {			
			context.write(val, key);
		}
		
	}
}

