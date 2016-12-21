package cs6240;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
       
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static Pattern     nw1 = Pattern.compile("[^'a-zA-Z]");
    private final static Pattern     nw2 = Pattern.compile("(^'+|'+$)");
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            Matcher mm1 = nw1.matcher(itr.nextToken());
            Matcher mm2 = nw2.matcher(mm1.replaceAll("")); 
            String ww = mm2.replaceAll("").toLowerCase();

            if (!ww.equals("")) {
                word.set(ww);
                context.write(word, one);
            }
        }
    }
}
 
class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    LinkedHashMap<String, Integer> wordcount;
    
    public void setup(Context context)
    {
    	wordcount= new LinkedHashMap<String, Integer>();
    }
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

    	int sum = 0;        
        for (IntWritable val : values) {
            sum += val.get();
        }

        wordcount.put(key.toString(), sum);
    }
    
    public void cleanup(Context context) throws IOException, InterruptedException
    {
    	LinkedHashMap<String, Integer> sortedMap = sortHashMapByValues(wordcount);
    	Text word = new Text();
    	IntWritable result = new IntWritable();
    	
    	for (Entry<String, Integer> entry : sortedMap.entrySet()) {
    	    String key = entry.getKey();
    	    word.set(key);
    	    int count = entry.getValue();
    	    result.set(count);
    	    context.write(word, result);
    	}    	
    }

    //reference : Sorting LinkedHashMap based on values -> StackOverflow.com
    public LinkedHashMap<String, Integer> sortHashMapByValues(LinkedHashMap<String , Integer> passedMap) {
        
    	java.util.List<String> mapKeys = new ArrayList<String>(passedMap.keySet());
        java.util.List<Integer> mapValues = new ArrayList<Integer>(passedMap.values());
        Collections.sort(mapValues);
        Collections.sort(mapKeys);

        LinkedHashMap<String , Integer> sortedMap =  new LinkedHashMap<>();

        Iterator<Integer> valueIt = mapValues.iterator();
        while (valueIt.hasNext()) {
            Integer val = valueIt.next();
            Iterator<String> keyIt = mapKeys.iterator();

            while (keyIt.hasNext()) {
                String key = keyIt.next();
                Integer comp1 = passedMap.get(key);
                Integer comp2 = val;
                if (comp1 == comp2) {
                    keyIt.remove();
                    sortedMap.put(key, val);
                    break;
                }
            }
        }
        return sortedMap;
    }
}
