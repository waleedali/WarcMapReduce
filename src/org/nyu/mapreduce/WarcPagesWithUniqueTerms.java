package org.nyu.mapreduce;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.clueweb.clueweb09.ClueWeb09WarcRecord;
import org.clueweb.clueweb09.mapreduce.ClueWeb09InputFormat;
import org.apache.log4j.Logger;
import org.jsoup.*;
import org.jsoup.nodes.*;

        
public class WarcPagesWithUniqueTerms {
	
	private static final Logger LOG = Logger.getLogger(WarcPagesWithUniqueTerms.class);
        
	public static class Map extends Mapper<LongWritable, ClueWeb09WarcRecord, IntWritable, Text> {
	    private IntWritable mapKey = new IntWritable();
		
	    public void map(LongWritable key, ClueWeb09WarcRecord doc, Context context) throws IOException, InterruptedException {    	
	    	Integer docSize = doc.getTotalRecordLength();
	    	String docid = doc.getHeaderMetadataItem("WARC-TREC-ID");
	    
	    	// Get the text from the HTML document 
	    	String docContent = doc.getContent();
	    	Document parsedDoc = Jsoup.parse(docContent);
	    	String docText = parsedDoc.text();
	    	
	    	// find the unique words
	    	String[] words = docText.split(" ");
	    	Set<String> uniqueWords = new HashSet<String>();

	    	for (String word : words) {
	    	    uniqueWords.add(word);
	    	}
	    	
	    	String[] docArray = new String[2];
	    	docArray[0] = docSize.toString();
	    	Integer uniqueWordsCount = uniqueWords.size();
	    	docArray[1] = uniqueWordsCount.toString();
	    	
	    	if (docid != null) {
	    		mapKey.set(uniqueWordsCount);
	    		context.write(mapKey, new Text(docid));
	    	}
	    	
	    	
	    }
	} 
	        
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		private Text result = new Text();
		
	    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {	
	    	String output = "";
	        
	        for (Text val : values) {
	        	output += val.toString() + " ";
	        }
	        
	        result.set(output);
	        context.write(key, result);
	    }
	}
	        
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	    Job job = new Job(conf, "Warc pages with unique terms!");
	    job.setJarByClass(WarcPagesWithUniqueTerms.class);
	    
	    job.setOutputKeyClass(IntWritable.class);	
	    job.setOutputValueClass(Text.class);
	        
	    job.setMapperClass(Map.class);
	    job.setCombinerClass(Reduce.class);
	    job.setReducerClass(Reduce.class);
	     
	    job.setInputFormatClass(ClueWeb09InputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    LOG.info(" - input: " + args[0]);
    	LOG.info(" - output directry: " + args[1]);
    	
	    job.waitForCompletion(true);
	}
        
}

