package com.test.hdfs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSTest {

    public static void main(String[] args) throws Exception {

    	testCopyToLocalFile();

    }
    
    public static void testCopyToLocalFile() throws IOException{
    		String dest = "/user/longge/ER/data";
    		Path path = new Path(dest);
    		  String local ="/usr/local/content";
    		  Configuration conf = new Configuration();
    		  FileSystem fs = path.getFileSystem(conf);
    		  FSDataInputStream fsdi = fs.open(path);
    		  OutputStream output = new FileOutputStream(local);
    		  IOUtils.copyBytes(fsdi,output,4096,true);
    }
    
    
    public static void listFileName() throws IOException{
    	Configuration conf=new Configuration();
        FileSystem hdfs=FileSystem.get(conf);
        Path src = new Path("/user/longge/ER"); 
        FileStatus stats[]=hdfs.listStatus(src);
        System.out.println(stats.length);
        for(int i = 0; i < stats.length; ++i)
        {
        	System.out.println(stats[i].getPath().toString());
        }

        hdfs.close();
    }

}