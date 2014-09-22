package com.test.er;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BatchImport
{
	   private final static  String TABLE_NAME="er";
	   private final static  String family = "entity";
	   private final static  String qualifier = "value";
		private static Long count=1L;
		public static class BatchImportMapper extends Mapper
		{
			public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException
			{
				String value = values.toString().split("\t")[0];
				
				context.write(key, new Text(value));
			}
		}
		
		public static class BatchImportReduce extends Reducer
		{
			public void reduce(LongWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException
			{
				for(Text value:values)
				{
					Put put = new Put(Bytes.toBytes(count));
					count++;
					put.add(family.getBytes(), qualifier.getBytes(),value.toString().getBytes());
					context.write(new ImmutableBytesWritable(TABLE_NAME.getBytes()), put);
				}
			}
		}
		
		public static Job configurationJob(Configuration conf,String[]args) throws IOException
		{
			Job job = new Job(conf,"ER");
			job.setJarByClass(BatchImport.class);
			job.setMapperClass(BatchImportMapper.class);
			job.setReducerClass(BatchImportReduce.class);
			job.setInputFormatClass(TextInputFormat.class);
			TableMapReduceUtil.initTableReducerJob(TABLE_NAME, null, job);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			return job;
		}
		
		public static void main(String[] args) throws Exception  
		{
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length != 1) {
			      System.err.println("Usage: useras  ");
			      System.exit(2);
			    }
			    Job job = configurationJob(conf, otherArgs);
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
}
