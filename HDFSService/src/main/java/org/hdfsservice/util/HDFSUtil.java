package org.hdfsservice.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
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
			throws IOException {

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
		return Boolean.TRUE;
	}

}
