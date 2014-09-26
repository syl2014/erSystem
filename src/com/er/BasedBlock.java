package com.er;
import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class BasedBlock {
	//统计调用相似函数的次数
	static enum Count{
		SAMEENTITY_COUNT
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
				for(int j=i+1;j<list.size();j++){
					TextArrayWritable valueTwo = list.get(j);
					String entityTwo = valueTwo.toStrings()[0];
					context.getCounter(Count.SAMEENTITY_COUNT).increment(1);
						if(isSame(entityOne,entityTwo)){
							String writeContent = getContent(entityOne,entityTwo);
							context.write(new Text(entityOne), new Text(writeContent));
					}
				}
			}
			

			
			
			
			
			
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
//				//context.write(new Text(outputKey+"\t"+outputSame),null);
//			}
		}
		
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		long start = System.currentTimeMillis();
		Configuration conf  =  new Configuration();
		Job job = new Job(conf,"BasedBlock");
		job.setJarByClass(BasedBlock.class);
		job.setMapperClass(ERMap.class);
		job.setReducerClass(ERReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job,"songyalong",TextOutputFormat.class,Text.class,NullWritable.class);
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.38.100:9000/user/longge/ER/data"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.38.100:9000/user/longge/outER"));
		long end = System.currentTimeMillis();
		int exPro = job.waitForCompletion(true)?0:1;
		Counters counters =job.getCounters();
		Counter counter = counters.findCounter(Count.SAMEENTITY_COUNT);
		System.out.println("时间："+(end-start)+"ms");
		System.out.println("比较次数："+counter.getDisplayName()+":"+counter.getValue());
		System.exit(exPro);
	}
}