package com.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CreateData
{
	public static void main(String[] args) throws IOException
	{
		copyDataFromFile();
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
	public static void testMapReduceER() throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream input = fs.open(new Path("/user/longge/outER/part-r-00000"));
		BufferedReader bis = new BufferedReader(new InputStreamReader(input,"GBK"));  
		int count=0;
		String s="";
		while((s=bis.readLine())!=null)
		{
			if(s.contains("=>"))
			{
				count++;
			}
		}
		
		System.out.println("count="+count);
	}
	
	
	public static void getCount() throws IOException
	{
		File src = new File("C:/Users/songyalong/Desktop/data.txt");
		FileReader filereader = new FileReader(src);
		BufferedReader bufferedReader = new BufferedReader(filereader);
		String s1 =bufferedReader.readLine();
		String s2=bufferedReader.readLine();
		List<String> list1 = Arrays.asList(s1.substring(s1.indexOf(":")+1).split(":"));
		List<String> list2 = Arrays.asList(s1.substring(s2.indexOf(":")+1).split(":"));
//		Collections.sort(list1);
//		Collections.sort(list2);
//		
		int count=0;
		for(int i=0;i<list1.size();i++)
		{
			if(list1.get(i).equals(list2.get(i)))
			{
				count++;
			}
				
		}
		
		System.out.println("count="+count);
		
//		while((s=bufferedReader.readLine())!=null)
//		{
//			String[] split=s.split(":");
//			System.out.println("count="+split.length);
//		}
	}
	
	public static void copyDataFromFile() throws IOException
	{
		File src = new File("e:/dblp.raw.txt");
		File dir = new File("e:/50.txt");
		FileReader filereader = new FileReader(src);
		FileWriter fileWriter = new FileWriter(dir);
		BufferedReader bufferedReader = new BufferedReader(filereader);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		String line = "";
		int count=0;
		while((line=bufferedReader.readLine())!=null)
		{
			if(count==500000)
				break;
			count++;
			bufferedWriter.write(line);
			bufferedWriter.newLine();
		}
		bufferedReader.close();
		bufferedWriter.close();
	}
}
