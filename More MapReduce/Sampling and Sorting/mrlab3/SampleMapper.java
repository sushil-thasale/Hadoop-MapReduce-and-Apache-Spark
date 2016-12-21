package cs6240;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


public class SampleMapper 
extends Mapper<Object, Text, NullWritable, Text > {
    public void map(Object key, Text line, 
            Context context) throws IOException, InterruptedException {

    	NullWritable nw = NullWritable.get();
    	
    	Random rn = new Random();
    	if(rn.nextInt(15) == 7)
    	{
    		context.write(nw, line);
    	}    
    }
}
 
