package cs6240;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class QuantReducer extends
Reducer<NullWritable, Text, Text, NullWritable> {
    NullWritable nw = NullWritable.get();

    public void reduce(NullWritable _k, Iterable<Text> lines, 
            Context context) throws IOException, InterruptedException {
        
    	ArrayList<String> xs = new ArrayList<String>();

        String qs = context.getConfiguration().get("num-quants");

        int numSamples = Integer.parseInt(qs);
        
        for(Text line : lines)
        {
        	xs.add(line.toString());
        }
        
        Collections.sort(xs);
        
        int arraySize = xs.size();
        int blockSize = (int) arraySize/numSamples;
        int index = 0;
        
        for (int i =0; i<numSamples; i++)
        {
        	index += blockSize;
        	if(index > arraySize-1)
        	{
        		index = arraySize-1;
        	}
        	context.write(new Text(xs.get(index)), nw);
        }                
    }       
}
