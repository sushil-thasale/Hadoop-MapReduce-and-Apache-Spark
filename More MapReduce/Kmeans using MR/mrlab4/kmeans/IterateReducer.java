package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class IterateReducer extends
		Reducer<DataPoint, DataPoint, DataPoint, DataPoint> {
	int itr;
	DataPoint newCenter;
	DataPoint tempCenter;
	
	@Override
	public void setup(Context ctx) {
		newCenter = new DataPoint();
		tempCenter = new DataPoint();

		itr = ctx.getConfiguration().getInt("itr", -10);
		if (itr == -10) {
			throw new Error("Didn't propagate itr");
		}
	}

	@Override
	public void reduce(DataPoint cc, Iterable<DataPoint> vals, Context ctx)
			throws IOException, InterruptedException {

		// get first DataPoint from given list
		for(DataPoint val:vals)
		{
			tempCenter = val.clone();
			break;
		}
		
		// if first DataPoint is a center, then all the values will be centers
		// they need to be written to same part-r-00000
		if (tempCenter.center) {
			
			// store all centers in array list
			ArrayList<DataPoint> dps = new ArrayList<>();
			dps.add(tempCenter.clone());
			
			for(DataPoint val:vals)
			{
				dps.add(val.clone());
			}
			
			// Map is used to remove duplicates
			// this guarantees we get only 5 centers at end
			Map<String, DataPoint> map = new LinkedHashMap<>();
			for (DataPoint dp : dps) {
				map.put(dp.toString(), dp.clone());
			}
					
			dps.clear();
			dps.addAll(map.values());
			
			// write all centers to part-r-00000
			for(DataPoint dp : dps)
			{
				ctx.write(dp.clone(), dp.clone());
			}			
			
		} else {

			// values are not centers
			// these values need not be written to part-r-00000
			// compute new centers and emit all DataPoints
			double xsum = 0;
			double ysum = 0;
			long xcount = 0;
			long ycount = 0;

			// compute new cluster center
			ArrayList<DataPoint> points = new ArrayList<>();

			xsum += tempCenter.x;
			xcount++;
			ysum += tempCenter.y;
			ycount++;
			points.add(tempCenter.clone());
			
			for(DataPoint val:vals)
			{
				xsum += val.x;
				xcount++;
				ysum += val.y;
				ycount++;
				points.add(val.clone());
			}

			newCenter.x = (double) xsum / xcount;
			newCenter.y = (double) ysum / ycount;
			newCenter.center = true;			
						
			// emit all points with new cluster center
			for(DataPoint dataP : points) {
				ctx.write(newCenter, dataP);
			}
		}
	}
	
	public void cleanup(Context ctx)
	{
	}
}
