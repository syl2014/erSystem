package com.er;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class EliminateRedun2{
	
	static enum Count{
		SAMECOUNT
	}
public static class ERMap extends Mapper<Object,Text,Text,TextArrayWritable>{
	
	//实体中的属性从小到大排序
	public List<String> getSort(String[] valueSplit){
		List<String> list = Arrays.asList(valueSplit);
		Collections.sort(list);
		return list;
	}
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		String val = value.toString().toLowerCase();
		int index = val.indexOf(":");
		//得到属性字符串
		String attrs = val.substring(index+1).toLowerCase();
		//排序
		String [] valueSplit = attrs.split(":");
		List<String> list = getSort(valueSplit);
		
		//设置key、value
		Text k = new Text(); //key值
		//textVal[0]放实体，textVal[1]放属性
		Text[] textVal= new Text[2]; //value值
		textVal[0] = new Text();
		textVal[1] = new Text();

		textVal[0].set(val); 
		TextArrayWritable listValue = new TextArrayWritable();
		for(int i=0;i<list.size();i++){
			String attr = "";
			k.set(list.get(i));
			//设置第一个属性时，textVal[1]中放特殊字符，进行区分。
			if(i==0){
				textVal[1]= new Text("&&&&&&&&");
				listValue.set(textVal);
				context.write(k,listValue);
				continue;
			}
			
			for(int j=0;j<i;j++){
				//textVal[j+1]= new Text(list.get(j));
				attr+=list.get(j)+":";
			}
			attr = attr.substring(0,attr.lastIndexOf(":"));
			textVal[1].set(attr); //存放 属性值
			listValue.set(textVal);
			context.write(k,listValue);
		}
	}
}
	public static class ERReduce extends Reducer<Text,TextArrayWritable,Text,Text> {

		//判断两个实体是否需要比较
		public boolean isMustCompare(String valueOne,String valueTwo,Context context){
			context.getCounter(Count.SAMECOUNT).increment(1);
			if("&&&&&&&&".equals(valueOne)&& "&&&&&&&&".equals(valueTwo)){
				return true;
			}
			String [] entityOneAttrs = valueOne.split(":");
			String [] entityTwoAttrs = valueTwo.split(":");
			int i=0;
			int j=0;
			while(i<entityOneAttrs.length && j<entityTwoAttrs.length){
				int compare = entityOneAttrs[i].compareToIgnoreCase(entityTwoAttrs[j]);
				if(compare==0)
					return false;
				else if (compare<0)
					i++;
				else
					j++;	
			}
			return true;
		}
		
		//判断是否是同一个实体
		public boolean isSame(String valueOne,String valueTwo){
			int count=0; //统计属性相同的个数
			int index = valueOne.indexOf(":");
			String[] valueO = valueOne.substring(index+1).split(":");
			String[]valueT = valueTwo.substring(index+1).split(":");
			int len = valueO.length>valueT.length?valueT.length:valueO.length;
			for(int i=0;i<len;i++){
				if(valueO[i].equals(valueT[i])){
					count++;
				}
			}
			//是同一个实体的依据：两个实体属性中有超过一般的属性是相同的
			if(count>len/2){
				return true;
			}
			return false;
		}
		
		//两个相同的实体，得到输出的字符串
		public static String getContent(String valueOne, String valueTwo)
		{
			String content = "";
			int index = valueOne.indexOf(":");		
			String valueO = valueOne.substring(0, index);
			String valueT = valueTwo.substring(0, index);
			content = valueO+"=>"+valueT;
			return content;
		}
		
		
		
		
		
		public void reduce(Text key,Iterable<TextArrayWritable> values,Context context) throws IOException, InterruptedException{
			 int compare = 0;
			List<TextArrayWritable> list = new ArrayList<TextArrayWritable>();
		
			//将迭代器转化为集合。
			for(TextArrayWritable value:values){
				TextArrayWritable a = new TextArrayWritable();
				a.set(value.get());
				list.add(a);
			}
			
			
			for(int i=0;i<list.size()-1;i++){
				TextArrayWritable valueOne = list.get(i);
				String entityOne =valueOne.toStrings()[0];
				String entityOneAttrs = valueOne.toStrings()[1];
				for(int j=i+1;j<list.size();j++){
					TextArrayWritable valueTwo = list.get(j);
					String entityTwo = valueTwo.toStrings()[0];
					String entityTwoAttrs = valueTwo.toStrings()[1];
					if(isMustCompare(entityOneAttrs,entityTwoAttrs,context)){
						if(isSame(entityOne, entityTwo)){
						String writeContent = getContent(entityOne,entityTwo);
						context.write(new Text(entityOne), new Text(writeContent));
						}
					}
				}
			}
			

			
			
			
//			
//			long count=0L;
//			String outputKey = "";
//			String outputSame = " ";
//			String tmp = "";
//			if(set.size()>=1){
//				Iterator<String> vals  = set.iterator();
//				outputKey = vals.next();
//				int index = outputKey.indexOf(":");
//				outputSame = outputKey.substring(0,index);
//				while(vals.hasNext()){
//					tmp = vals.next();
//					outputSame+="=>"+tmp.substring(0,tmp.indexOf(":"));
//				}
//				//mos.write("songyalong",new Text(outputKey+"\t"+outputSame),nullWritable);
//				
//				context.write(new Text(outputKey+"\t"+outputSame),null);
//			}
		}
		

	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		long start =System.currentTimeMillis();
		Configuration conf  =  new Configuration();
		Job job = new Job(conf,"EliminateRedun");
		job.setJarByClass(EliminateRedun2.class);
		job.setMapperClass(ERMap.class);
		job.setReducerClass(ERReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.38.100:9000/user/longge/ER/5.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.38.100:9000/user/longge/outER"));
		
		int exPro = job.waitForCompletion(true)?0:1;
		long end = System.currentTimeMillis();
		System.out.println("程序运行时间："+(end-start)+"ms");
		
		Counters counters = job.getCounters(); //counters 作业的所有计数器
	    Counter c1 = counters.findCounter(Count.SAMECOUNT);
	    System.out.println("-------------->>>>: " + c1.getDisplayName() + ": " + c1.getValue());
		
	    System.exit(exPro);
	}
}