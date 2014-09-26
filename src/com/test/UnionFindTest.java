package com.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.er.TextArrayWritable;

public class UnionFindTest
{
	public static String find(String x,Map map){
		String p=x;
		String t="";
		while(!map.get(p).equals(p))
			p=(String) map.get(p);   //找到集合中的代表元素，所有的元素比较其实都是代表元素的比较。
		while(!x.equals(p)){
			t=(String) map.get(p);
			map.put(x, p);
			x=t;
		}
		return x;
	}
	public static void setMerge(String entityOne,String entityTwo,Map map){
		if((find(entityOne,map).equals(find(entityTwo,map))))
			return ;
		map.put(entityTwo, entityOne);
	}
	public static boolean isSame(String valueOne,String valueTwo){
		int count=0; //统计属性相同的个数
		String[] valueO = valueOne.split(":");
		String[]valueT = valueTwo.split(":");
		for(int i=0;i<valueO.length;i++){
			if(valueO[i].equals(valueT[i])){
				count++;
			}
		}
		//是同一个实体的依据：两个实体属性中有超过一般的属性是相同的
		if(count>valueO.length/2){
			return true;
		}
		return false;
	}
	
	
	public static void main(String[] args) throws IOException
	{
		List<String> list = new ArrayList<String>();
		File file = new File("C:\\Users\\songyalong\\Desktop\\data2.txt");
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader  = new BufferedReader(fileReader);
		String s = "";
		while((s=bufferedReader.readLine())!=null){
			list.add(s);
		}
		Map<String,String> map = new HashMap<String,String>();
		for(int i=0;i<list.size()-1;i++){
			String entityO = list.get(i);
			String entityOne = entityO.substring(0,entityO.indexOf(":"));
			String entityOneAttrs = entityO.substring(entityO.indexOf(":")+1);
			for(int j=i+1;j<list.size();j++){
				String entityT = list.get(j);
				String entityTwo = entityT.substring(0,entityT.indexOf(":"));
				String entityTwoAttrs = entityT.substring(entityT.indexOf(":")+1);
				if(true){
					if(!map.containsKey(entityO))
						map.put(entityO, entityO);
					if(!map.containsKey(entityT))
						map.put(entityT, entityT);
					if(!(find(entityO,map).equals(find(entityT,map)))){
						if(isSame(entityOneAttrs,entityTwoAttrs)){
							setMerge(entityO,entityT,map);
						}
					}
				}
			}
		}
		
		Iterator<String> keys = map.keySet().iterator();
		while(keys.hasNext()){
			String key = keys.next();
			String value = map.get(key);
			System.out.println("key="+key);
			System.out.println("value="+value);
		}
		
	}//main
}
