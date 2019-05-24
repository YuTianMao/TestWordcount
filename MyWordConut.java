package com.mao.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyWordConut {

	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		
		try {
			Job job = new Job(conf);
			job.setJarByClass(MyWordConut.class);
			
			job.setMapperClass(MyWordCountMapper.class);
			job.setReducerClass(MyWordCountReduce.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job, new Path("/usr/input/word.txt"));
			
			Path output = new Path("/output/data/wordcount");
			FileOutputFormat.setOutputPath(job, output);
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(output)) {
				fs.delete(output, true);
			}
			FileOutputFormat.setOutputPath(job, output);
			
			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("ok");
			}
		} catch ( Exception e) {
			e.printStackTrace();
		}
			
			
		
		
		
	}
	
}
