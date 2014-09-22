package com.test.hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseBasicUtil
{
	private static HBaseConfiguration hbaseConfig = null;
	private final static String tablename = "er";
	private final static String tablename2 = "entity";
	private static long count=1;
	
	static
	{
		Configuration config = new Configuration();
		config.set("hbase.zookeeper.quorum", "192.168.38.100");

		config.set("hbase.zookeeper.property.clientPort", "2181");

		hbaseConfig = new HBaseConfiguration(config);
	}

	public static void selectByRowKey(String tablename, String rowKey)
			throws IOException
	{

		HTable table = new HTable(hbaseConfig, tablename);

		Get g = new Get(Bytes.toBytes(rowKey));

		Result r = table.get(g);

		for (KeyValue kv : r.raw())
		{
			System.out.println("column: " + new String(kv.getKey()));
			System.out.println("value: " + new String(kv.getValue()));
		}
	}
	
	public static void insert(String tablename1,String tablename2,String content) throws IOException
	{
		String value = content.split("\t")[0];
		String[] entitys = content.split("\t")[1].split(":");
		//data be inserted tablename1
		HTable tableOne = new HTable(hbaseConfig, tablename1);
		HTable tableTwo = new HTable(hbaseConfig,tablename2);
		Put put = new Put(Bytes.toBytes(count));
		
		put.add("entity".getBytes(),"value".getBytes(), value.getBytes());
		tableOne.put(put);
		
		//data be inserted into tablename2
		for(String entity:entitys)
		{
			Put put2 = new Put(entity.getBytes());
			put2.add("object".getBytes(),"value".getBytes(),Bytes.toBytes(count));
			tableTwo.put(put2);
		}
		count++;
		
	}
	public static void readAndInsertContent(String dir) throws IOException
	{
		Configuration conf = new Configuration();
		Path path = new Path("hdfs://hadoop:9000/user/longge/outER/part-r-00000");
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream input = fs.open(path);
		InputStreamReader isr = new InputStreamReader(input, "utf-8");  
        BufferedReader br = new BufferedReader(isr); 
        String content ="";
        while((content=br.readLine())!=null)
        {
        	System.out.println(content);
        	if(content.trim()!=null)
        		insert(tablename,tablename2,content);
        }
	}

	public static void main(String[] args) throws IOException
	{
		// 按rowkey查询，查询Tom行的所有cell
		//HBaseBasic.selectByRowKey("tablename", "Jim");
		//HBaseBasic.insert("tablename","syl");
		String dir = "hdfs://hadoop:9000/user/longge/ER/erdata";
		HBaseBasicUtil.readAndInsertContent(dir);
	}

}
