package PageRank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// emits top 100 pages with highest page rank
public class InputReducer extends Reducer<Text, PageData, Text, PageData> {

	@Override
	public void reduce(Text page, Iterable<PageData> vals, Context ctx)
			throws IOException, InterruptedException {

		// only 1 value per page
		for (PageData val : vals) {
			ctx.write(page, val);
		}
	}
}