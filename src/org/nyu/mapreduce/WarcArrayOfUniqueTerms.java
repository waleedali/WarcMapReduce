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

        
public class WarcArrayOfUniqueTerms {
	
	private static final Logger LOG = Logger.getLogger(WarcArrayOfUniqueTerms.class);
        
	public static class Map extends Mapper<LongWritable, ClueWeb09WarcRecord, Text, Text> {
	        
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
	    		context.write(new Text(docid), new Text(uniqueWords.toString()));
	    	}
	    	
	    	
	    }
	} 
	        
	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
	    public void reduce(Text key, Iterator<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {	
	    	String output = "";
	        while (values.hasNext()) {
	        	output += " " +  values.next().get(); 
	        }
	        
	        context.write(key, new Text(output));
	    }
	}
	        
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	    Job job = new Job(conf, "WarcMapReduce!");
	    job.setJarByClass(WarcArrayOfUniqueTerms.class);
	    
	    job.setOutputKeyClass(Text.class);	
	    job.setOutputValueClass(Text.class);
	        
	    job.setMapperClass(Map.class);
	    // job.setReducerClass(Reduce.class);
	     
	    job.setInputFormatClass(ClueWeb09InputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    LOG.info(" - input: " + args[0]);
    	LOG.info(" - output directry: " + args[1]);
    	
	    job.waitForCompletion(true);
	}
        
}

