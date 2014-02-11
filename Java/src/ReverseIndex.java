package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ReverseIndex {

 public static class Map extends Mapper<Text, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text doc = new Text();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
	System.out.println("In Mapper: Key = " + key + " and value = " + value);
        System.out.println("this is line " + line);
        //#String[] all_words = new String[50];
        //all_words = line.split("\t");
        //System.out.println("document = " + all_words[0] + " and contents = " + all_words[1]);
        
	//doc.set(all_words[0]);

	StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {

            String temp = tokenizer.nextToken(); 
            //word.set(tokenizer.nextToken());

            //Strip last non-alphabet chars from a word
	    if ( ! temp.matches(".*[a-zA-Z]$") ) {
                word.set(temp.substring(0, temp.length()-1));
	    }
	    else
		word.set(temp);

            context.write(word, key);
        }
    }
 }

public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    	String doc_list = "";
       
        HashMap<String, Integer> map = new HashMap<String, Integer>(); 
 
	for (Text val : values) {
		map.put(val.toString(), 1);
        }

        Iterator<String> keySetIterator = map.keySet().iterator();

	while(keySetIterator.hasNext()){
  		String k = keySetIterator.next();
	        doc_list += k + ",";	
	}
        if (doc_list.length() > 0 && doc_list.charAt(doc_list.length()-1)==',') {
    		doc_list = doc_list.substring(0, doc_list.length()-1);
  	} 
        context.write(key, new Text(doc_list));
    }
 }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

        Job job = new Job(conf, "wordcount");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    //job.setInputFormatClass(TextInputFormat.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setJarByClass(org.myorg.ReverseIndex.class);
    job.waitForCompletion(true);
 }

}

