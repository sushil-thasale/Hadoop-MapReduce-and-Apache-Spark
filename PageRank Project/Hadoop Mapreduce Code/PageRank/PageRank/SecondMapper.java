package PageRank;
/*
 * SecondMapper only emits(Page, PageData)
 * */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMapper extends Mapper<Text, PageData, Text, PageData> {

	@Override
	public void map(Text page, PageData pd, Context ctx) throws IOException,
			InterruptedException {

		ctx.write(page, pd);
	}
}
