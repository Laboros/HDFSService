package org.hdfsservice.util;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 
 * @author svaduka 
 * The class HDFSUtil servers common functionalities required to
 *         perform operations on HDFS
 */
public class HDFSUtil {

	/**
	 * 
	 * @param localInputFileNameWithLoc
	 * @param hdfsDestinationLoc
	 * @return true/false: Successfully Written or failed
	 * @throws IOException
	 */
	public static boolean writeLocalFileOnHDFS(
			final String localInputFileNameWithLoc,
			final String hdfsDestinationLoc, final Configuration conf)
			 {

		boolean isFileCreated=Boolean.TRUE;
		
		try
		{
		
		InputStream is = new FileInputStream(localInputFileNameWithLoc);

		FileSystem hdfs = FileSystem.get(conf);
		
		Path hdfsFileDestPath = new Path(hdfsDestinationLoc 
				+ HDFSConstants.FILE_SEPARATOR_VALUE
				+ localInputFileNameWithLoc);

		FSDataOutputStream fsdos = hdfs.create(hdfsFileDestPath);

		IOUtils.copyBytes(is, fsdos, conf);

		if (is != null) {
			is.close();
			is = null;
		}
		if (fsdos != null) {
			fsdos.close();
			fsdos = null;
		}
		}catch (IOException e) {
			isFileCreated=Boolean.FALSE;
		}
		return isFileCreated;
	}
	
	public static String readDataFromHDFS(final String hdfsPathToReadFile, final Configuration conf) throws IOException
	{
		FileSystem hdfs = FileSystem.get(conf);
		Path fileReadPath=new Path(hdfsPathToReadFile);
		String data=null;
		if(hdfs.exists(fileReadPath))
		{
		FSDataInputStream inputStream=hdfs.open(fileReadPath);
		
		ByteArrayOutputStream outputStream=new ByteArrayOutputStream();
		
		IOUtils.copyBytes(inputStream, outputStream, conf);
		data=outputStream.toString(HDFSConstants.CHARSET.getValue());
		}
		return data; 
	}
	

}
