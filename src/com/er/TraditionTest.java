package com.er;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//按照传统方法判断实体
public class TraditionTest
{
	//判断是否是同一实体
	public static boolean isSame(String valueOne, String valueTwo)
	{
		int count = 0; // 统计属性相同的个数
		int index = valueOne.indexOf(":");
		String[] valueO = valueOne.substring(index + 1).split(":");
		String[] valueT = valueTwo.substring(index + 1).split(":");
		int len = valueO.length>valueT.length?valueT.length:valueO.length;
		for (int i = 0; i <len; i++)
		{
			if (valueO[i].equals(valueT[i]))
			{
				count++;
			}
		}
		if (count > len / 2)
		{
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
		content = valueOne+"\t"+valueO+"=>"+valueT+"\n";
		return content;
	}
	//将文件中的文本放到集合里
	public static List<String> getFileList() throws IOException
	{
		List<String> list = new ArrayList<String>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream dataInput = fs.open(new Path("/user/longge/ER/data"));
		InputStreamReader inputSteamReader = new InputStreamReader(dataInput,"UTF-8");
		BufferedReader bufferedReader = new BufferedReader(inputSteamReader);
		LineNumberReader lineNumberReader = new LineNumberReader(bufferedReader);
		String s = "";
		while((s=lineNumberReader.readLine())!=null)
		{
			bufferedReader.mark(10000);
			list.add(s);
		}
		lineNumberReader.close();
		bufferedReader.close();
		return list;
	}
	//集合中的实体两两比较
	public static void compareList(List<String> list) throws IOException
	{
		Configuration configuration = new Configuration();  
		FileSystem fs = FileSystem.get(configuration);  
		FSDataOutputStream  out = fs.create(new Path("/user/longge/ER/content"));
		String content="";
		for(int i=0;i<list.size()-1;i++)
		{
			String comOne = list.get(i);
			for(int j=i+1;j<list.size();j++)
			{
				String comTwo = list.get(j);
				if(isSame(comOne,comTwo))
				{
					content = getContent(comOne,comTwo);
					out.write(content.getBytes());
				}
			}
		}
		out.flush();
		out.close();
	}
	
	//第二种比较方式 
	public static void compare() throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream dataInput = fs.open(new Path("/user/longge/ER/5.txt"));
		InputStreamReader inputSteamReader = new InputStreamReader(dataInput,"UTF-8");
		BufferedReader bufferedReader = new BufferedReader(inputSteamReader);
		FSDataOutputStream  out = fs.create(new Path("/user/longge/ER/content"));
		String content = "";
		String nextContent = "";
		int len = 12689408;
		while((content=bufferedReader.readLine())!=null)
		{
			len = len-content.getBytes().length;
			bufferedReader.mark(len);
			while((nextContent=bufferedReader.readLine())!=null)
			{
				if(isSame(content,nextContent))
				{
					String writeContent = getContent(content,nextContent);
					System.out.println("writeContent="+writeContent);
					out.write(writeContent.getBytes());
				}
			}
			
			bufferedReader.reset();
		}
		
		out.flush();
		out.close();
		bufferedReader.close();
		dataInput.close();
		fs.close();
	}
	
	
	public static void main(String[] args) throws IOException
	{
		long start = System.currentTimeMillis();
		
//		List<String> list = getFileList();
//		compareList(list);
		
		compare();
		long end = System.currentTimeMillis();
		System.out.println("程序运行时间："+(end-start)+"ms");
	}
}
