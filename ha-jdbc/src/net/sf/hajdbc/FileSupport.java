/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class FileSupport
{
	private List fileList = new LinkedList();
	
	public File createFile(InputStream inputStream) throws java.sql.SQLException
	{
		File file = this.createTempFile();;
		
		try
		{
			OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file), 8192);
			byte[] chunk = new byte[8192];
			int byteCount = inputStream.read(chunk);
			
			while (byteCount >= 0)
			{
				outputStream.write(chunk, 0, byteCount);
				byteCount = inputStream.read(chunk);
			}
			
			outputStream.flush();
			outputStream.close();
			
			return file;
		}
		catch (IOException e)
		{
			throw new SQLException(e);
		}
	}
	
	public File createFile(Reader reader) throws java.sql.SQLException
	{
		File file = this.createTempFile();;
		
		try
		{
			Writer writer = new BufferedWriter(new FileWriter(file), 8192);
			char[] chunk = new char[8192];
			int byteCount = reader.read(chunk);
			
			while (byteCount >= 0)
			{
				writer.write(chunk, 0, byteCount);
				byteCount = reader.read(chunk);
			}
			
			writer.flush();
			writer.close();
			
			return file;
		}
		catch (IOException e)
		{
			throw new SQLException(e);
		}
	}
	
	private File createTempFile() throws SQLException
	{
		try
		{
			File file = File.createTempFile("ha-jdbc", "lob");
			
			this.fileList.add(file);
			
			return file;
		}
		catch (IOException e)
		{
			throw new SQLException(e);
		}
	}
	
	public void close()
	{
		while (!this.fileList.isEmpty())
		{
			File file = (File) this.fileList.remove(0);
			
			file.delete();
		}
	}
	
	/**
	 * @see java.lang.Object#finalize()
	 */
	protected void finalize() throws Throwable
	{
		this.close();
		
		super.finalize();
	}
}
