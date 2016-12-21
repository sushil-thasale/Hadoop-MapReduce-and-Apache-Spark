package cs6240;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

class SortPartitioner extends Partitioner<Text, NullWritable> implements
		Configurable {

	Configuration conf;

	ArrayList<String> samples;

	@Override
	public int getPartition(Text key, NullWritable value, int np) {

		int index=0;
		
		while(samples.get(index).compareTo(key.toString()) < 0)
		{			
			index++;
			if(index>8)
				break;
		}

//		System.out.println(key.toString() +" "+ index);
		if(index>9)
			return ((index-1)%np);
		
		return index%np;		
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		this.conf = arg0;

		String sampPath = conf.get("samps");
		samples = new ArrayList<String>();		

		BufferedReader rdr;
		try {
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream ss = fs.open(new Path(sampPath));
			rdr = new BufferedReader(new InputStreamReader(ss));

			String currentLine;

			while ((currentLine = rdr.readLine()) != null) {
//				System.out.println(currentLine);
				samples.add(currentLine);
			}
		} catch (Exception ee) {
			throw new Error(ee.toString());
		}
	}
}
