/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import net.sf.hajdbc.util.SQLExceptionFactory;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class FileSupportImpl implements FileSupport
{
	private static final String TEMP_FILE_PREFIX = "ha-jdbc-";
	private static final String TEMP_FILE_SUFFIX = ".lob";
	private static final int BUFFER_SIZE = 8192;
	
	private List<File> fileList = new LinkedList<File>();
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#createFile(java.io.InputStream)
	 */
	@Override
	public File createFile(InputStream inputStream) throws SQLException
	{
		File file = this.createTempFile();
		
		try
		{
			FileChannel fileChannel = new FileOutputStream(file).getChannel();
			ReadableByteChannel inputChannel = Channels.newChannel(inputStream);
			
			ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
			
			while (inputChannel.read(buffer) > 0)
			{
				buffer.flip();
				
				fileChannel.write(buffer);
				
				buffer.compact();
			}
			
			fileChannel.close();
			
			return file;
		}
		catch (IOException e)
		{
			throw SQLExceptionFactory.createSQLException(e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#createFile(java.io.Reader)
	 */
	@Override
	public File createFile(Reader reader) throws SQLException
	{
		File file = this.createTempFile();
		
		try
		{
			Writer writer = new FileWriter(file);
			
			CharBuffer buffer = CharBuffer.allocate(BUFFER_SIZE);
			
			while (reader.read(buffer) > 0)
			{
				buffer.flip();
				
				writer.append(buffer);
				
				buffer.clear();
			}

			writer.close();
			
			return file;
		}
		catch (IOException e)
		{
			throw SQLExceptionFactory.createSQLException(e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#getReader(java.io.File)
	 */
	@Override
	public Reader getReader(File file) throws SQLException
	{
		try
		{
			return new BufferedReader(new FileReader(file), BUFFER_SIZE);
		}
		catch (IOException e)
		{
			throw SQLExceptionFactory.createSQLException(e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#getInputStream(java.io.File)
	 */
	@Override
	public InputStream getInputStream(File file) throws SQLException
	{
		try
		{
			return Channels.newInputStream(new FileInputStream(file).getChannel());
		}
		catch (IOException e)
		{
			throw SQLExceptionFactory.createSQLException(e);
		}
	}
	
	/**
	 * Creates a temp file and stores a reference to it so that it can be deleted later.
	 * @return a temp file
	 * @throws SQLException if an IO error occurs
	 */
	private File createTempFile() throws SQLException
	{
		try
		{
			File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
			
			this.fileList.add(file);
			
			return file;
		}
		catch (IOException e)
		{
			throw SQLExceptionFactory.createSQLException(e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#close()
	 */
	@Override
	public void close()
	{
		for (File file: this.fileList)
		{
			if (!file.delete())
			{
				file.deleteOnExit();
			}
		}
	}
	
	/**
	 * @see java.lang.Object#finalize()
	 */
	@Override
	protected void finalize() throws Throwable
	{
		this.close();
		
		super.finalize();
	}
}
