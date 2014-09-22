package com.test.er;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class ER2 {

public static class ERMap extends Mapper<Object,Text,Text,ArrayWritable>{
	public List<String> getSort(String[] valueSplit){
		List<String> list = Arrays.asList(valueSplit);
		Collections.sort(list);
		return list;
	}
	
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		int index = value.toString().indexOf(":");
		String attrs = value.toString().substring(index+1);
		//排序
		String [] valueSplit = attrs.split(":");
		List<String> list = getSort(valueSplit);
		
		//设置key、value
		Text k = new Text(); //key值
		Text[] textVal= new Text[list.size()+1]; //value值
		textVal[0]=value;
		
		
		for(int i = 1; i <= list.size(); ++i) {
			textVal[i]=new Text();
		}
		
		ArrayWritable listValue = new ArrayWritable(Text.class);
		for(int i=0;i<list.size();i++){
			k.set(list.get(i));
			if(i==0){
				listValue.set(textVal);
				context.write(k,listValue);
				continue;
			}
			
			for(int j=0;j<i;j++){
				textVal[j+1]= new Text(list.get(j));
			}
			listValue.set(textVal);
			context.write(k,listValue);
			
		}
	}
}
	public static class ERReduce extends Reducer<Text,ArrayWritable,Text,Text> {
		public boolean isMustCompare(Text[] valueOne,Text[] valueTwo){
			if(valueOne.length==1||valueTwo.length==1){
				return true;
			}else{
				Text[] new_valueO= new Text[valueOne.length-1];
				Text[] new_valueT= new Text[valueTwo.length-1];
				System.arraycopy(valueOne,1, new_valueO,0, new_valueO.length);
				System.arraycopy(valueTwo, 1, new_valueT, 0, new_valueT.length);
				
				String strOne = new_valueO.toString();
				String strTwo = new_valueT.toString();
				if(strOne.equals(strTwo))
					return false;
				else
					return true;
			}
			
		}
		public boolean isSame(String valueOne,String valueTwo){
			int count=0; //统计属性相同的个数
			int index = valueOne.indexOf(":");
			String[] valueO = valueOne.substring(index).split(":");
			String[]valueT = valueTwo.substring(index).split(":");
			for(int i=0;i<valueO.length;i++){
				if(valueO[i].equals(valueT[i])){
					count++;
				}
			}
			if(count>index/2){
				return true;
			}
			return false;
		}
		public void reduce(Text key,Iterable<ArrayWritable> values,Context context) throws IOException, InterruptedException{
			List<ArrayWritable> list = new ArrayList<ArrayWritable>();
			//将迭代器转化为集合。
			for(ArrayWritable value:values){
				list.add(value);
			}
			
			Set set = new HashSet<String>();
			
			for(int i=0;i<list.size()-1;i++){
				ArrayWritable valueOne = list.get(i);
				Text[] valueO = (Text[]) valueOne.get();
				String valOne = valueO[0].toString();
				set.add(valOne);
				for(int j=1;j<list.size();j++){
					ArrayWritable valueTwo = list.get(j);
					Text[] valueT = (Text[]) valueTwo.get();
					String valTwo = valueT[0].toString();
					if(isMustCompare(valueO,valueT)){
						if(isSame(valOne,valTwo)){
							set.add(valTwo);
						}
					}
				}
			}
			String outputKey = "";
			String outputSame = " ";
			String tmp = "";
			if(set.size()>1){
				Iterator<String> vals  = set.iterator();
				outputKey = vals.next();
				int index = outputKey.indexOf(":");
				outputSame = outputKey.substring(0,index);
				while(vals.hasNext()){
					tmp = vals.next();
					outputSame+="=>"+tmp.substring(0,tmp.indexOf(":"));
				}
			}
			
			context.write(new Text(outputKey+outputSame),null);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf  =  new Configuration();
		Job job = new Job(conf,"ER2");
		job.setJarByClass(ER2.class);
		
		job.setMapperClass(ERMap.class);
		job.setReducerClass(ERReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.38.100:9000/user/longge/ER/dblp"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.38.100:9000/user/longge/outER"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}