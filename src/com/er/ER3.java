package com.er;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class ER3 {

public static class ERMap extends Mapper<Object,Text,Text,TextArrayWritable>{
	public List<String> getSort(String[] valueSplit){
		List<String> list = Arrays.asList(valueSplit);
		Collections.sort(list);
		return list;
	}
	
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		String val = value.toString().toLowerCase();
		int index = val.indexOf(":");
		String attrs = val.substring(index+1).toLowerCase();
		//排序
		String [] valueSplit = attrs.split(":");
		List<String> list = getSort(valueSplit);
		
		//设置key、value
		Text k = new Text(); //key值
		Text[] textVal= new Text[2]; //value值
		textVal[0] = new Text();
		textVal[1] = new Text();
//		for(int i =0; i <= textVal.length; ++i) {
//			textVal[i]=new Text();
//		}
		textVal[0].set(val); 
		TextArrayWritable listValue = new TextArrayWritable();
		for(int i=0;i<list.size();i++){
			List<String> attrsList = new ArrayList();
			k.set(list.get(i));
			if(i==0){
				textVal[1]= new Text("&&&&&&&&");
				listValue.set(textVal);
				context.write(k,listValue);
				continue;
			}
			
			for(int j=0;j<i;j++){
				//textVal[j+1]= new Text(list.get(j));
				attrsList.add(list.get(j));
			}
			textVal[1].set(attrsList.toString()); //存放 属性值
			listValue.set(textVal);
			context.write(k,listValue);
		}
	}
}
	public static class ERReduce extends Reducer<Text,TextArrayWritable,Text,NullWritable> {
		private NullWritable nullWritable = NullWritable.get();
		private MultipleOutputs<Text,NullWritable> mos;
		protected void setup(Context context){
			 mos = new MultipleOutputs<Text,NullWritable>(context);
		}
		public boolean isMustCompare(String valueOne,String valueTwo){
			
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
		public boolean isSame(String valueOne,String valueTwo){
			int count=0; //统计属性相同的个数
			int index = valueOne.indexOf(":");
			String[] valueO = valueOne.substring(index+1).split(":");
			String[]valueT = valueTwo.substring(index+1).split(":");
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
		public void reduce(Text key,Iterable<TextArrayWritable> values,Context context) throws IOException, InterruptedException{
			List<TextArrayWritable> list = new ArrayList<TextArrayWritable>();
			//将迭代器转化为集合。
			System.out.println("-----------------------");
			for(TextArrayWritable value:values){
				TextArrayWritable a = new TextArrayWritable();
				a.set(value.get());
				list.add(a);
			}
			
			Set<String> set = new HashSet<String>();
			for(int i=0;i<list.size()-1;i++){
				TextArrayWritable valueOne = list.get(i);
				String entityOne =valueOne.toStrings()[0];
				String entityOneAttrs = valueOne.toStrings()[1];
				for(int j=i+1;j<list.size();j++){
					TextArrayWritable valueTwo = list.get(j);
					String entityTwo = valueTwo.toStrings()[0];
					String entityTwoAttrs = valueTwo.toStrings()[1];
					if(isMustCompare(entityOneAttrs,entityTwoAttrs)){
						if(isSame(entityOne,entityTwo)){
							set.add(entityTwo);
							set.add(entityOne);
						}
					}
				}
			}
			
			String outputKey = "";
			String outputSame = " ";
			String tmp = "";
			if(set.size()>=1){
				Iterator<String> vals  = set.iterator();
				outputKey = vals.next();
				int index = outputKey.indexOf(":");
				outputSame = outputKey.substring(0,index);
				while(vals.hasNext()){
					tmp = vals.next();
					outputSame+=":"+tmp.substring(0,tmp.indexOf(":"));
				}
				mos.write("songyalong",new Text(outputKey+"\t"+outputSame),nullWritable);
				//context.write(new Text(outputKey+"\t"+outputSame),null);
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			mos.close();
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf  =  new Configuration();
		Job job = new Job(conf,"ER2");
		job.setJarByClass(ER3.class);
		job.setMapperClass(ERMap.class);
		job.setReducerClass(ERReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleOutputs.addNamedOutput(job,"songyalong",TextOutputFormat.class,Text.class,NullWritable.class);
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.38.100:9000/user/longge/ER/data"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.38.100:9000/user/longge/outEr"));
		
		
		if(job.waitForCompletion(true)){
			copyHdfsFile();
		}
	}
	
	public  static void copyHdfsFile() throws IOException{
		Configuration conf = new Configuration();
		Path path = new Path("/user/longge/outEr/songyalong-r-00000");
		FileSystem fs = path.getFileSystem(conf);
		if(fs.exists(path)){
			System.out.println("wenjiancunzai");
			fs.copyToLocalFile(true, path, new Path("/usr/local"));
		}else{
			System.out.println("文件不存在");
		} 
		fs.close();
	}
	
}