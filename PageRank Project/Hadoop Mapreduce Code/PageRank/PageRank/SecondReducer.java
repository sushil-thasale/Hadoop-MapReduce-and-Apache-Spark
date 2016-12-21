package PageRank;
/*
 * SecondReducer : 
 * Assigns initial page rank of (1/total_pages) to all pages
 * Finds sum of page rank of sink nodes 
 * This sum is used to refine page rank in next job
 * */
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<Text, PageData, Text, PageData> {

	private static double sinkSum;
	private long totalPages;
	private double initialPageRank;

	public void setup(Context ctx) {

		sinkSum = 0.0d;

		// fetch total pages
		totalPages = ctx.getConfiguration().getLong("totalPages", -10);

		// communicate total pages to driver
		ctx.getCounter("totalPages", "").increment(totalPages);

		// compute initial page rank
		initialPageRank = (double) 1 / totalPages;
	}

	@Override
	public void reduce(Text page, Iterable<PageData> vals, Context ctx)
			throws IOException, InterruptedException {

		PageData receivedPD = new PageData();
		// only 1 value per page
		for (PageData val : vals) {
			receivedPD = val.clone();
		}
		
		receivedPD.pageRank = initialPageRank;		

		if (receivedPD.outlinks.equals("")) {
			sinkSum += receivedPD.pageRank;
		}
		
		ctx.write(page, receivedPD.clone());
	}
		
	public void cleanup(Context ctx) throws IOException, InterruptedException {

		// communicate sum of page rank of sink nodes
		// converting to long
		// maintaining precision upto 9 decimal points
		long sinkSumLong = (long) (sinkSum * 1000000000);
		ctx.getCounter("initialSinkSum", "").increment(sinkSumLong);
	}
}