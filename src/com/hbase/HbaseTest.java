package com.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTest
{
	public static void main(String[] args) throws IOException
	{
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "192.168.38.100");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
		HTableDescriptor hTableDescriptor = new HTableDescriptor("test");
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("data");
		hTableDescriptor.addFamily(hColumnDescriptor);
		hbaseAdmin.createTable(hTableDescriptor); //创建表
		byte[]tableName = hTableDescriptor.getName();
		HTableDescriptor [] htabled = hbaseAdmin.listTables();
		if(htabled.length!=1 && Bytes.equals(tableName, htabled[0].getName()))
		{
			throw new IOException(" create table failed");
		}
		HTable htable = new HTable(conf, tableName);
		byte[] row1 = Bytes.toBytes("row1");
		byte[]datadcr = Bytes.toBytes("data");
		Put put = new Put(row1);
		put.add(datadcr,Bytes.toBytes("col1"),Bytes.toBytes("valueOne"));
		htable.put(put);
		
		//得到数据
		Get get = new Get(row1);
		Result result = htable.get(get);
		System.out.println("result="+result);
		
		Scan scan = new Scan();
		ResultScanner resultScanner = htable.getScanner(scan);
		for(Result resultscan : resultScanner)
		{
			System.out.println("resultscan="+resultscan);
		}
		
		resultScanner.close();
		hbaseAdmin.disableTable(tableName);
		hbaseAdmin.deleteTable(tableName);
	}
}
