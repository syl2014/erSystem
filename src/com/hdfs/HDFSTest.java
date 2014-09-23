package com.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSTest
{

	public static void main(String[] args) throws Exception
	{

		testCopyToLocalFile();

	}

	public static void testCopyToLocalFile() throws IOException
	{
		String file = "/user/longge/ER/erdata";// hdfs文件
																					// 地址
		Configuration config = new Configuration();
		config.addResource("/usr/local/hadoop/conf/core-site.xml");
		FileSystem fs = FileSystem.get(URI.create(file), config);// 构建FileSystem
		InputStream is = fs.open(new Path(file));// 读取文件
		FileOutputStream output = new FileOutputStream(new File("/usr/local/songyalong"));
		IOUtils.copyBytes(is, output, config, true);// 保存到本地
		is.close();
	}
	
	
	public static void listFileName() throws IOException
	{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path src = new Path("/user/longge/ER");
		FileStatus stats[] = hdfs.listStatus(src);
		System.out.println(stats.length);
		for (int i = 0; i < stats.length; ++i)
		{
			System.out.println(stats[i].getPath().toString());
		}

		hdfs.close();
	}

}