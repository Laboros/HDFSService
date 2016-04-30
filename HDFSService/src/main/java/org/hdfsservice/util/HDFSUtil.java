package org.hdfsservice.util;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.commonservice.FileUtil;
import com.commonservice.exception.InvalidArgException;
import com.commonservice.util.LoggerUtil;

/**
 * 
 * @author svaduka 
 * The class HDFSUtil servers common functionalities required to
 *         perform operations on HDFS
 */
public class HDFSUtil {
	
	private static LoggerUtil logger=new LoggerUtil(HDFSUtil.class);

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
				+ HDFSConstants.FILE_SEPARATOR_VALUE.getValue()
				+ getFileName(localInputFileNameWithLoc));

		if(hdfs.mkdirs(new Path(hdfsDestinationLoc)))
		{
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
		}
		}catch (IOException e) {
			e.printStackTrace();
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
	
	public static List<String> readDataFromHDFSAsLines(final String hdfsPathToReadFile, final Configuration conf) throws IOException
	{
		List<String> lines =null;
		FileSystem hdfs = FileSystem.get(conf);
		Path fileReadPath=new Path(hdfsPathToReadFile);
		if(hdfs.exists(fileReadPath))
		{
			lines=new ArrayList<String>();
			
		FSDataInputStream inputStream=hdfs.open(fileReadPath);
		BufferedReader reader =new BufferedReader(new InputStreamReader(inputStream, HDFSConstants.CHARSET.getValue()));
		String line=null;
		while((line=reader.readLine())!=null){
			lines.add(line);
		}
		}
		return lines; 
	}

	private static String getFileName(final String input){
		String[] tokens=StringUtils.splitPreserveAllTokens(input, HDFSConstants.FILE_SEPARATOR_VALUE.getValue());
		return tokens[tokens.length-1];
	}

	/**
	 * 
	 * @param hdfsInputFile
	 * @param hdfsDestDir
	 * @param removeInputLoc
	 * @param conf
	 * @throws InvalidArgException
	 * @return TRUE/FALSE, TRUE indicates the file was moved successfully.
	 * This method will use to move hdfs files from one location to another location.
	 * @throws IOException 
	 * 
	 */
	public static boolean moveHDFSFileToHDFSDestDir(final String hdfsInputFile,final String hdfsDestDir, boolean removeInputLoc,boolean overrideDest,final Configuration conf) throws InvalidArgException, IOException
	{
		if(!StringUtils.isEmpty(hdfsInputFile) && !StringUtils.isEmpty(hdfsInputFile) && conf!=null)
		{
			FileSystem hdfs=FileSystem.get(conf);
			
			final Path inputFilePath=new Path(hdfsInputFile);
			
			final Path destDirPath=new Path(hdfsDestDir);
			//Checking if output path exists or not
			if(!hdfs.exists(destDirPath))
			{
				hdfs.mkdirs(destDirPath);
			}
			final String fileName=FileUtil.getFileNameWithExt(hdfsInputFile);
			
			final String renamedFileName=hdfsDestDir+HDFSConstants.FILE_SEPARATOR_VALUE.getValue()+fileName;
			
			logger.debug("Creating destination file Name: "+renamedFileName);
			final Path renamedPath=new Path(renamedFileName);

			if(overrideDest){
				hdfs.deleteOnExit(renamedPath);
			}else{
				boolean isRenamedFileExists=hdfs.exists(renamedPath);
				
				if(isRenamedFileExists)
				{
					throw new FileAlreadyExistsException("Renamed File:"+renamedFileName+" is already exists");
				}
			}
				hdfs.rename(inputFilePath, renamedPath);
		}else{
			throw new InvalidArgException("invalid input arguments found: hdfsinputloc:"+hdfsInputFile+" hdfsOutputLoc:"+hdfsInputFile+" removeInputLoc:"+removeInputLoc+" conf:"+conf);
		}
		return Boolean.TRUE;
	}
}
