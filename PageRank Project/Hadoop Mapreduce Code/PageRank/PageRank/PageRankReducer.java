package PageRank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, PageData, Text, PageData> {

	private double sinkSum;
	private float alpha;
	private long totalPages;
	private long oldSinkPRSum;
	private int ii;

	public void setup(Context ctx) {
		sinkSum = 0.0d;

		// fetch teleportation factor for page rank
		alpha = ctx.getConfiguration().getFloat("alpha", -10);

		// fetch total pages for additional page rank calculations
		totalPages = ctx.getConfiguration().getLong("totalPages", -10);

		// fetch sum of old page ranks of sink nodes
		oldSinkPRSum = ctx.getConfiguration().getLong("oldSinkPRSum", -10);

		ii = ctx.getConfiguration().getInt("itr", -10);
	}

	// input to reducer will be a page-name as key
	// and list of PageData objects
	// in that list only 1 object will have outlinks != ""
	// all other objects will have outlinks = ""
	// this is done to reduce data transfer from mapper to reducer

	@Override
	public void reduce(Text page, Iterable<PageData> vals, Context ctx)
			throws IOException, InterruptedException {

		PageData newData = new PageData();
		double newPR = 0;

		// computing summation over PageRank(inlink) / outlinks(inlink)
		for (PageData val : vals) {
			newPR += val.pageRank;
			if (val.pageRank == 0 && (!val.outlinks.equals(""))) {
				newData.outlinks = val.outlinks;
			}
		}

		// additional pagerank calculation
		// adding teleportation factor
		double tele = (double) alpha / totalPages;
		newPR = tele + (double) (1 - alpha) * newPR;
		// handling sink nodes
		newPR += (1 - alpha) * oldSinkPRSum / (1000000000 * totalPages);

		newData.pageRank = newPR;

		if (newData.outlinks.equals("")) {
			sinkSum += newData.pageRank;
		}

		ctx.write(page, newData.clone());
	}

	public void cleanup(Context ctx) {
		// communicate sum of page rank of sink nodes
		// converting to long
		// maintaining precision upto 9 decimal points
		long newSinkPRSum = (long) (sinkSum * 1000000000);
		ctx.getCounter("newSinkPRSum" + ii, "").increment(newSinkPRSum);
	}
}
