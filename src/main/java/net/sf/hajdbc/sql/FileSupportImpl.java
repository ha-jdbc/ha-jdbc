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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import net.sf.hajdbc.ExceptionFactory;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class FileSupportImpl<E extends Exception> implements FileSupport<E>
{
	private static final String TEMP_FILE_PREFIX = "ha-jdbc-"; //$NON-NLS-1$
	private static final String TEMP_FILE_SUFFIX = ".lob"; //$NON-NLS-1$
	private static final int BUFFER_SIZE = 8192;
	
	private final List<File> fileList = new LinkedList<File>();
	private final ExceptionFactory<E> exceptionFactory;
	
	public FileSupportImpl(ExceptionFactory<E> exceptionFactory)
	{
		this.exceptionFactory = exceptionFactory;
	}
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#createFile(java.io.InputStream)
	 */
	@Override
	public File createFile(InputStream inputStream) throws E
	{
		try
		{
			File file = this.createTempFile();
			
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
			throw this.exceptionFactory.createException(e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#createFile(java.io.Reader)
	 */
	@Override
	public File createFile(Reader reader) throws E
	{
		try
		{
			File file = this.createTempFile();
			
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
			throw this.exceptionFactory.createException(e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#getReader(java.io.File)
	 */
	@Override
	public Reader getReader(File file) throws E
	{
		try
		{
			return new BufferedReader(new FileReader(file), BUFFER_SIZE);
		}
		catch (IOException e)
		{
			throw this.exceptionFactory.createException(e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#getInputStream(java.io.File)
	 */
	@Override
	public InputStream getInputStream(File file) throws E
	{
		try
		{
			return Channels.newInputStream(new FileInputStream(file).getChannel());
		}
		catch (IOException e)
		{
			throw this.exceptionFactory.createException(e);
		}
	}
	
	/**
	 * Creates a temp file and stores a reference to it so that it can be deleted later.
	 * @return a temp file
	 * @throws SQLException if an IO error occurs
	 */
	private File createTempFile() throws IOException
	{
		File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
		
		this.fileList.add(file);
		
		return file;
	}
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#close()
	 */
	@Override
	public void close()
	{
		Iterator<File> files = this.fileList.iterator();

		while (files.hasNext())
		{
			File file = files.next();

			if (!file.delete())
			{
				file.deleteOnExit();
			}

			files.remove();
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
