package PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
	static final int convergence = 10;
	static long totalNoPages = 0;
	static float alpha = 0.15f;
	static long initialSinkSum;
	static Path input, output;
	static String outputString;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("convergence", convergence);
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length == 2) {
			input = new Path(otherArgs[0]);
			output = new Path(otherArgs[1]);
			outputString = otherArgs[1];
		} else {
			throw new Error("enter input and output files");
		}

		// first job performs text preprocessing
		// also computes initial page ranks
		totalNoPages = readAndIterate(conf);

		// computes initial page ranks (1/totalPages) and assigns it to page
		// also, computes initial sum of page ranks of sink nodes
		initialPageRank(conf);

		// compute page ranks over 10 iterations
		int ii = 0;
		long sinkPRSum = initialSinkSum;
		
		while (ii < convergence) {
//			System.out.println("\n\n\n "+initialSinkSum+"\n\n");
			sinkPRSum = iterate(conf, ii++, sinkPRSum);
		}

		// write top 100 pages with highest page ranks
		writeOutput(conf, ii);
	}

	// 1st job performs text preprocessing
	// finds total no. pages and returns it to driver
	// writes initial page rank (1/N) for each page along with its outlinks
	public static long readAndIterate(Configuration conf) throws Exception {
		conf.setInt("itr", -1);

		Job job = Job.getInstance(conf, "pre-process");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(InputMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageData.class);
		job.setReducerClass(InputReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PageData.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, new Path(outputString + "/data-1"));

		boolean ok = job.waitForCompletion(true);
		if (!ok) {
			throw new Exception("Job failed");
		}

		long total = job.getCounters().findCounter("totalPages", "").getValue();
		if (total == -10) {
			throw new Error("Didn't propagate totalPages");
		}

		return total;
	}

	// computes initial page rank => (1/total-pages)
	// assign it to each page and write to sequence file along with its outlinks
	// also computes sum of page rank of sink nodes to handle sink nodes
	public static void initialPageRank(Configuration conf) throws Exception {
		conf.setInt("itr", -1);
		conf.setLong("totalPages", totalNoPages);

		Job job = Job.getInstance(conf, "initial-pagerank");
		job.setJarByClass(PageRank.class);
		// mapper simply emits key, value => (page, PageData)
		job.setMapperClass(SecondMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageData.class);
		// reducer computes initial page rank and assign it to all pages
		job.setReducerClass(SecondReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PageData.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(outputString + "/data-1"));
		FileOutputFormat.setOutputPath(job, new Path(outputString + "/data0"));

		boolean ok = job.waitForCompletion(true);
		if (!ok) {
			throw new Exception("Job failed");
		}

		// return a long value
		// must be divided by 1000000000 to get accurate value
		initialSinkSum = job.getCounters().findCounter("initialSinkSum", "")
				.getValue();
		if (initialSinkSum == -10) {
			throw new Error("Didn't propagate initialSinkSum");
		}
	}

	// refines page ranks of all pages
	public static long iterate(Configuration conf, int ii, long sinkPRSum)
			throws Exception {

		conf.setInt("itr", ii);
		conf.setLong("totalPages", totalNoPages);
		conf.setFloat("alpha", alpha);
		conf.setLong("oldSinkPRSum", sinkPRSum);

		Job job = Job.getInstance(conf, "page-rank iteration");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(PageRankMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageData.class);
		job.setReducerClass(PageRankReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PageData.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat
				.addInputPath(job, new Path(outputString + "/data" + ii));
		FileOutputFormat.setOutputPath(job, new Path(outputString + "/data"
				+ (ii + 1)));

		boolean ok = job.waitForCompletion(true);
		if (!ok) {
			throw new Exception("Job failed");
		}

		// returns a long value
		// must be divided by 1000000000 to get accurate value
		// this is done later in reducer of next job
		Long newSinkPRSum = job.getCounters()
				.findCounter("newSinkPRSum" + ii, "").getValue();
		if (newSinkPRSum == -10) {
			throw new Error("Didn't propagate newSinkSum");
		}

		return newSinkPRSum;
	}

	// reads output of 10th iterate job
	// writes top 100 pages with highest page ranks
	public static void writeOutput(Configuration conf, int ii) throws Exception {
		conf.setInt("itr", ii);

		Job job = Job.getInstance(conf, "write output");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(FinalMapper.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setSortComparatorClass(DescKeyComparator.class);
		job.setReducerClass(FinalReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat
				.addInputPath(job, new Path(outputString + "/data" + ii));
		FileOutputFormat.setOutputPath(job, new Path(outputString + "/top100"));

		boolean ok = job.waitForCompletion(true);
		if (!ok) {
			throw new Exception("Job failed");
		}
	}
}