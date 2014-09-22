package com.er;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class ER {

public static class ERMap extends Mapper<Object, Text, Text, Text>{
	//得到key
	public String getKey(String value){
		value=value.trim();
		String[]keyS = value.split("\t");
		StringBuffer k = new StringBuffer();
		for(int i=1;i<3;i++){
			if(keyS[i]!=null){
				k.append(keyS[i]+",");
			}
		}
		return k.toString();
	}
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		Text k = new Text(getKey(value.toString()));
		System.out.println(k.toString());
		context.write(k, value);
	}
	}
	public static class ERReduce extends Reducer<Text,Text,Text,Text> {
		public boolean compare(String valueOne,String valueTwo){
			if(valueOne.trim()==null||valueTwo.trim()==null){
				return false;
			}
			int count=0; //计算两个字符串中属性的
			String[]valueO = valueOne.split("\t");
			String[]valueT = valueTwo.split("\t");
			int len = valueO.length>valueT.length?valueO.length:valueT.length;
			
			for(int i=3;i<len;i++){
				if(valueO[i]!=null&&valueT[i]!=null&&valueO[i].equals(valueT[i])){
					count++;
				}
			}
			System.out.println("count:"+count);
			if(count>=len/2){
				System.out.println("valueTwo:"+valueTwo);
				return true;
			}				
			return false;
		}
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			Set<String> set = new HashSet<String>();
			List<String> list = new ArrayList<String>();
			String valueOne= "";
			String valueTwo="";
			String k="";
			String eq="";
			for(Text v:values){
				list.add(v.toString());
			}
			if(list.size()>1){
				
			}
			for(int i=0;i<list.size()-1;i++){
				 valueOne = list.get(i);
				if(set.isEmpty()){
					set.add(valueOne);
				}
				for(int j=0;j<list.size();j++){
					 valueTwo = list.get(j);
					if(compare(valueOne,valueTwo)){
						if(!set.contains(valueOne)){
							set.add(valueOne);
						}
						if(!set.contains(valueTwo)){
							set.add(valueTwo);
						}
					}
				}
				
			}
			if(set.size()>=1){
				Iterator<String> it =set.iterator();
				while(it.hasNext()){
					if(k.isEmpty()){
						k=it.next();
						eq+=k.split("\t")[0]+"-";
					}else{
						eq+=it.next().split("\t")[0]+"-";
					}
				}
			}
			
			
			context.write(new Text(k+":"+eq),null);
			
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf,"ER");
		job.setJarByClass(ER.class);
		
		job.setMapperClass(ERMap.class);
		job.setReducerClass(ERReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.38.100:9000/user/longge/ER"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.38.100:9000/user/longge/outER"));
		
		System.exit(job.waitForCompletion(true)?0:1);
}

}