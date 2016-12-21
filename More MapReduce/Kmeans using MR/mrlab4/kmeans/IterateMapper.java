package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import javax.xml.crypto.Data;

import org.apache.hadoop.mapreduce.Mapper;

public class IterateMapper extends
		Mapper<DataPoint, DataPoint, DataPoint, DataPoint> {
	public ArrayList<DataPoint> centers;
	int K;
	int itr;
	
	@Override
	public void setup(Context ctx) throws IOException {
		K = ctx.getConfiguration().getInt("K", -10);
		itr = ctx.getConfiguration().getInt("itr", -10);
		centers = DataPoint.readCenters(ctx.getConfiguration(), K, itr);
	}

	@Override
	public void map(DataPoint cc0, DataPoint dp, Context ctx)
			throws IOException, InterruptedException {

		// ignore lines from part-r-00000 files
		if (!dp.center) {
			
			ctx.write(cc0.clone(), cc0.clone());
			
			double minDistance = 10000000;
			DataPoint nearestCentroid = centers.get(0).clone();
			
			// iterate over all centers and find nearest center
			Iterator<DataPoint> centerItr = centers.iterator();
			while (centerItr.hasNext()) {
				DataPoint newDp = centerItr.next().clone();
				double distance = dp.getDistance(newDp);
				if (distance < minDistance) {
					minDistance = distance;
					nearestCentroid = newDp;
				}
			}

			// put given point into cluster with nearest center
			ctx.write(nearestCentroid, dp);
		}
	}

	public void cleanup(Context ctx) throws IOException, InterruptedException {		
		
	}
}
