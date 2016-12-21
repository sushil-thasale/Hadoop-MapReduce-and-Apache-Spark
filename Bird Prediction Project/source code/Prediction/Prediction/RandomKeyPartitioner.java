package Prediction;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * Mapper emits a random integer value
 * This class assigns a partitioner for such keys
 */
public class RandomKeyPartitioner extends Partitioner<IntWritable, SamplingEventDetails> {

	@Override
	public int getPartition(IntWritable randomKey, SamplingEventDetails samplingDetails, int nrt) { 
	  return (randomKey.get()%nrt);
	}
}