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

public class TraditionTest
{

	public static boolean isSame(String valueOne, String valueTwo)
	{
		int count = 0; // 统计属性相同的个数
		int index = valueOne.indexOf(":");
		String[] valueO = valueOne.substring(index + 1).split(":");
		String[] valueT = valueTwo.substring(index + 1).split(":");
		for (int i = 0; i < valueO.length; i++)
		{
			if (valueO[i].equals(valueT[i]))
			{
				count++;
			}
		}
		if (count > index / 2)
		{
			return true;
		}
		return false;
	}
	
	public static String getContent(String valueOne, String valueTwo)
	{
		String content = "";
		int index = valueOne.indexOf(":");		
		String valueO = valueOne.substring(0, index);
		String valueT = valueTwo.substring(0, index);
		content = valueOne+"\t"+valueO+"=>"+valueT+"\n";
		return content;
	}
	public static List<String> getFileList() throws IOException
	{
		List<String> list = new ArrayList<String>();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream dataInput = fs.open(new Path("/user/longge/ER/data"));
		InputStreamReader inputSteamReader = new InputStreamReader(dataInput,
				"UTF-8");
		BufferedReader bufferedReader = new BufferedReader(inputSteamReader);
		LineNumberReader lineNumberReader = new LineNumberReader(bufferedReader);
		String s = "";
		while((s=lineNumberReader.readLine())!=null)
		{
			list.add(s);
		}
		return list;
	}
	
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
	public static void main(String[] args) throws IOException
	{
		List<String> list = getFileList();
		compareList(list);
	}
}
