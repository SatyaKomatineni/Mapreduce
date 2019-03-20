package com.ai.learning.mapreduce.sample1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCountDriver {

	public static void main(String [] args) throws Exception
	{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		
		//Input and output paths
		//test
		Path inputPath=new Path(files[0]);
		Path outputPath=new Path(files[1]);
		
		//Job definition
		Job j=new Job(c,"wordcount");
		j.setJarByClass(WordCountDriver.class);
		j.setMapperClass(WordCountMapper.class);
		j.setReducerClass(WordCountReducer.class);
		
		//Output key and Value types
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		
		//Set input and output paths to the job
		FileInputFormat.addInputPath(j, inputPath);
		FileOutputFormat.setOutputPath(j, outputPath);
		
		//Execute
		System.exit(j.waitForCompletion(true)?0:1);
	}//eof-main
}//eof-class
