
package org.nyu.mapreduce;

import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.clueweb.clueweb09.mapreduce.*;
import org.clueweb.clueweb09.ClueWeb09WarcRecord;
//import org.clueweb.clueweb09.mapreduce.ClueWeb09InputFormat;

        
public class WarcMapReduce {
        
 public static class Map extends Mapper<LongWritable, ClueWeb09WarcRecord, Text, IntWritable> {
//    private final static IntWritable one = new IntWritable(1);
//    private Text word = 	new Text();
        
    public void map(LongWritable key, ClueWeb09WarcRecord doc, Context context) throws IOException, InterruptedException {
//        String line = value.toString();
//        StringTokenizer tokenizer = new StringTokenizer(line);
//        while (tokenizer.hasMoreTokens()) {
//            word.set(tokenizer.nextToken());
//            context.write(word, one);
//        }
//    	
    	int docSize = 0;
    	String docid = doc.getHeaderMetadataItem("WARC-TREC-ID");
    	if (doc.getHeaderMetadataItem("Content-Length") != null) {
    		docSize = Integer.parseInt(doc.getHeaderMetadataItem("Content-Length").toString());
    	}
    	
    	if (docid == null) {
    		docid = "unknownid";
    	}
    	
    	context.write(new Text(docid), new IntWritable(docSize));
    	
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }
        context.write(key, new IntWritable(sum));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "WarcMapReduce!");
    job.setJarByClass(WarcMapReduce.class);
    //job.setJarByClass(ClueWeb09InputFormat.class);
    
    job.setOutputKeyClass(Text.class);	
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
     
    job.setInputFormatClass(ClueWeb09InputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}

