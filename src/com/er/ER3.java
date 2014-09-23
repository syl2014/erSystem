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
			List<String> attrsList = new ArrayList();
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
		//判断两个实体是否需要比较
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
		//判断是否是同一个实体
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
			//是同一个实体的依据：两个实体属性中有超过一般的属性是相同的
			if(count>valueO.length/2){
				System.out.println("count="+count);
				return true;
			}
			return false;
		}
		
		// 查找x 返回值为x所在集合的代表
		public String find(String x,Map map){
			String p=x;
			String t="";
			while(!map.get(p).equals(p))
				p=(String) map.get(p);   //找到集合中的代表元素，所有的元素比较其实都是代表元素的比较。
			//将集合中的元素都指向集合的代表
			while(!x.equals(p)){
				t=(String) map.get(p);
				map.put(x, p);
				x=t;
			}
				x=p;
			return x;
		}
		
		//集合的合并
		public void setMerge(String entityOne,String entityTwo,Map map){
			if((find(entityOne,map).equals(find(entityTwo,map))))
				return ;
			map.put(entityTwo, entityOne);
		}
		public void reduce(Text key,Iterable<TextArrayWritable> values,Context context) throws IOException, InterruptedException{
			List<TextArrayWritable> list = new ArrayList<TextArrayWritable>();
			//并查集
			Map<String,String> map =  new HashMap<String,String>();
			//将迭代器转化为集合。
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
				set.add(entityOne);
				for(int j=i+1;j<list.size();j++){
					TextArrayWritable valueTwo = list.get(j);
					String entityTwo = valueTwo.toStrings()[0];
					String entityTwoAttrs = valueTwo.toStrings()[1];
					if(isMustCompare(entityOneAttrs,entityTwoAttrs)){
						if(!map.containsKey(entityOne))
							map.put(entityOne, entityOne);
						if(!map.containsKey(entityTwo))
							map.put(entityTwo, entityTwo);
						if(!(find(entityOne,map).equals(find(entityTwo,map)))){
							if(isSame(entityOne,entityTwo)){
								setMerge(entityOne,entityTwo,map);
							}
						}
					}
				}
			}
			long count=0L;
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
					outputSame+="=>"+tmp.substring(0,tmp.indexOf(":"));
				}
				//mos.write("songyalong",new Text(outputKey+"\t"+outputSame),nullWritable);
				
				context.write(new Text(outputKey+"\t"+outputSame),null);
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
		FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.38.100:9000/user/longge/ER/dblp.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.38.100:9000/user/longge/outER"));
		
		System.exit(job.waitForCompletion(true)?0:1);

	}
	
	
}