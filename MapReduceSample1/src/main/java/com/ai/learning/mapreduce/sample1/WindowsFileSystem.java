package com.ai.learning.mapreduce.sample1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class WindowsFileSystem extends LocalFileSystem {

	 public WindowsFileSystem() { super();
	}
	public boolean mkdirs( final Path path,  final FsPermission permission) 
			throws IOException 
	{ 
		final boolean result = super.mkdirs(path); 
		this.setPermission(path, permission); 
		return result; 
	}
	public void setPermission( final Path path,  final FsPermission permission) 
			throws IOException 
	{ 
		try { super.setPermission(path, permission); } 
		catch(final IOException e) 
		{ 
			System.err.println("Cant help it, hence ignoring IOException" + e.getMessage()); 
		} 
	}
	
	public static void setThisAsFS(Configuration conf)
	{
		conf.set("fs.default.name", "file: ///");
			
		conf.set("mapred.job.tracker", "local"); 
		conf.set("fs.file.impl", "com.ai.learning.mapreduce.sample1.WindowsFileSystem"); 
		conf.set("io.serializations", 
				"org.apache.hadoop.io.serializer.JavaSerialization, " 
						+ "org.apache.hadoop.io.serializer.WritableSerialization");
		return;
	}
}//eof-class