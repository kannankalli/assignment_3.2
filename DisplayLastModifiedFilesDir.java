package com.bigdata.acadgild;

import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DisplayLastModifiedFilesDir {

	public static void main(String[] args) {
		
		Configuration con = new Configuration();
		URI inputURI = URI.create(args[0]);
		Path path = new Path(inputURI.getPath());
		try {
			displayModifiedTimestampOfFilesDir(path,con);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void displayModifiedTimestampOfFilesDir(Path path, Configuration con) throws IOException {

		FileSystem fs = FileSystem.get(path.toUri(),con);
		FileStatus[] fileStatuses = fs.listStatus(path);
		
		for ( FileStatus fStatus : fileStatuses )
		{
			if ( fStatus.isFile() ) 
			{
				System.out.println("File Name***"+fStatus.getPath().getName()+ "  Modified time ***"+new Timestamp(fStatus.getModificationTime()));
			}
			else
			{
				System.out.println("Directory Name***"+fStatus.getPath().getName()+ "  Modified time ***"+new Timestamp(fStatus.getModificationTime()));
				displayModifiedTimestampOfFilesDir(fStatus.getPath(), con);
			}
		}
		
		
	}

}
