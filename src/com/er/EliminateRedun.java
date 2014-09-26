package com.er;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
public class ER4
{
	public static class ER4Map extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text values,Context context)
		{
			System.out.println(values.toString());
		}
	}
	
	public static class ER4Reduce extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> values,Context context)
		{
			
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf  =  new Configuration();
		Job job = new Job(conf,"ER4");
		job.setJarByClass(ER4.class);
		
		job.setMapperClass(ER4Map.class);
		job.setReducerClass(ER4Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.38.100:9000/user/longge/ER/data2"));
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.38.100:9000/user/longge/outER"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.38.100:9000/user/longge/outER2"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
