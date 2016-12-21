package PageRank;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, PageData, Text, PageData> {

	// reads [page, (page_rank, outlinks)]
	// iterate over a page's outlinks
	// computes and writes current page's contribution towards page rank of each
	// of its outlinks

	@Override
	public void map(Text page, PageData pd, Context ctx) throws IOException,
			InterruptedException {

		PageData nullPR = new PageData();
		nullPR.outlinks = pd.outlinks;

		// this is done to fetch outlinks of a page in reducer
		// emit -> [page, (0, outlinks)]
		ctx.write(page, nullPR);

		String outlink[] = pd.outlinks.split("\t");

		// computing current page's contribution towards page rank of each of
		// its outlinks
		// do not perform this computation for sink nodes
		if (outlink.length != 0) {
			double pageRankContributed = (double) pd.pageRank / outlink.length;

			for (int i = 0; i < outlink.length; i++) {
				if (!outlink[i].equals("")) {
					PageData pdInlink = new PageData();
					pdInlink.pageRank = pageRankContributed;

					// emit -> [outlink, (pageRankContributed , "")]
					// so we emitted [page, (0, outlinks)] earlier to get
					// outlinks
					// in reducer for particular page
					ctx.write(new Text(outlink[i]), pdInlink.clone());
				}
			}
		}

	}
}
