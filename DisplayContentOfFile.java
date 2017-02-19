package com.bigdata.acadgild;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class DisplayContentOfFile {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Configuration con = new Configuration();
		
		URI inputURI = URI.create(args[0]);
		FSDataInputStream fsDataInputStream = null;
		try {
			FileSystem fileSystem = FileSystem.get(inputURI, con);
			fsDataInputStream = fileSystem.open(new Path(inputURI));
			IOUtils.copyBytes(fsDataInputStream, System.out, 4096);
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally
		{
			IOUtils.closeStream(fsDataInputStream);
		}

	}
}
