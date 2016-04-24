package org.hdfsservice.util;

public enum HDFSConstants {
	
	FILE_SEPARATOR("file.separator"),
	FILE_SEPARATOR_VALUE(System.getProperty(HDFSConstants.FILE_SEPARATOR.getValue())), 
	CHARSET("UTF-8");
	
	String value;
	
	private HDFSConstants(final String value) 
	{
		this.value=value;
	}
	
	public String getValue(){
		return this.value;
	}

}
