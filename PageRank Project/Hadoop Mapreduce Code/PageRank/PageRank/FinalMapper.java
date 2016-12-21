package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalMapper extends Mapper<Text, PageData, DoubleWritable, Text> {

	// emit (page_rank, page) to take advantage of built-in sorting
	@Override
	public void map(Text page, PageData pd, Context ctx) throws IOException,
			InterruptedException {

		ctx.write(new DoubleWritable(pd.pageRank), page);
	}
}
