package kmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DefaultReducer extends
		Reducer<DataPoint, DataPoint, DataPoint, DataPoint> {
	
	@Override
	public void reduce(DataPoint cc, Iterable<DataPoint> vals, Context ctx)
			throws IOException, InterruptedException {
		
		for(DataPoint val : vals)
		{
			ctx.write(cc.clone(), val.clone());
		}			
	}	
}
