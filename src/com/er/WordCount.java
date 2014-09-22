package com.er;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Progressable;

public class WordCount {

public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);

	private Text word = new Text();
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		StringTokenizer str = new StringTokenizer(value.toString());
		while(str.hasMoreElements()){
			word.set(str.nextToken());
			context.write(word, one);
		}
	}
	
	}
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable num:values){
				sum+=num.get();
			}
			context.write(key, result);
		}
	}

	
   
 
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "word count");

		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.38.100:9000/user/longge/ER"));

		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.38.100:9000/user/longge/outER"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

}

}