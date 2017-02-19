package com.bigdata.acadgild;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class CopyFileToHdfs {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Configuration con = new Configuration();
		String inputPath = args[0];
		FSDataOutputStream fsDataOutputStream = null;
		try {
			InputStream is = new BufferedInputStream(new FileInputStream(new File(inputPath)));
			URI hdfsPath = URI.create(args[1]);
			FileSystem fs = FileSystem.get(hdfsPath, con);
			Path path = new Path(hdfsPath.toString());
			fsDataOutputStream = fs.create(path);
			IOUtils.copyBytes(is, fsDataOutputStream, con);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
