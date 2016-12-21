package PageRank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// emits top 100 pages with highest page rank
public class FinalReducer extends
		Reducer<DoubleWritable, Text, Text, DoubleWritable> {

	private static int i;

	public void setup(Context ctx) {
		i = 0;
	}

	@Override
	public void reduce(DoubleWritable pageRank, Iterable<Text> vals, Context ctx)
			throws IOException, InterruptedException {

		// emit only top 100 pages
		for (Text val : vals) {
			if(i<100)
			{
				ctx.write(val, pageRank);
				i++;
			}
		}
	}
}