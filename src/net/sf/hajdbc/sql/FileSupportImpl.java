/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.util.LinkedList;
import java.util.List;

import net.sf.hajdbc.SQLException;

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
	public File createFile(InputStream inputStream) throws java.sql.SQLException
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
			throw new SQLException(e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#createFile(java.io.Reader)
	 */
	public File createFile(Reader reader) throws java.sql.SQLException
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
			throw new SQLException(e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#createFile(java.sql.Blob)
	 */
	public File createFile(Blob blob) throws java.sql.SQLException
	{
		return this.createFile(blob.getBinaryStream());
	}

	/**
	 * @see net.sf.hajdbc.sql.FileSupport#createFile(java.sql.Clob)
	 */
	public File createFile(Clob clob) throws java.sql.SQLException
	{
		return this.createFile(clob.getCharacterStream());
	}

	/**
	 * @see net.sf.hajdbc.sql.FileSupport#getReader(java.io.File)
	 */
	public Reader getReader(File file) throws java.sql.SQLException
	{
		try
		{
			return new BufferedReader(new FileReader(file), BUFFER_SIZE);
		}
		catch (IOException e)
		{
			throw new SQLException(e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#getInputStream(java.io.File)
	 */
	public InputStream getInputStream(File file) throws java.sql.SQLException
	{
		try
		{
			return Channels.newInputStream(new FileInputStream(file).getChannel());
		}
		catch (IOException e)
		{
			throw new SQLException(e);
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
			throw new SQLException(e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#close()
	 */
	public void close()
	{
		for (File file: this.fileList)
		{
			file.delete();
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

	/**
	 * @see net.sf.hajdbc.sql.FileSupport#getBlob(java.io.File)
	 */
	public Blob getBlob(File file) throws java.sql.SQLException
	{
		try
		{
			final FileChannel channel = new FileInputStream(file).getChannel();
		
			return new Blob()
			{
				public long length() throws java.sql.SQLException
				{
					try
					{
						return channel.size();
					}
					catch (IOException e)
					{
						throw new SQLException(e);
					}
				}
	
				public byte[] getBytes(long position, int length) throws java.sql.SQLException
				{
					ByteBuffer buffer = ByteBuffer.allocate(length);
					
					try
					{
						channel.read(buffer, position);
					}
					catch (IOException e)
					{
						throw new SQLException(e);
					}
					
					buffer.compact();
					
					return buffer.array();
				}
	
				public InputStream getBinaryStream()
				{
					return Channels.newInputStream(channel);
				}
	
				public long position(byte[] pattern, long start)
				{
					throw new UnsupportedOperationException();
				}
	
				public long position(Blob pattern, long start)
				{
					throw new UnsupportedOperationException();
				}
	
				public int setBytes(long position, byte[] bytes) throws java.sql.SQLException
				{
					return this.writeBuffer(position, ByteBuffer.wrap(bytes));
				}
	
				public int setBytes(long position, byte[] bytes, int offset, int length) throws java.sql.SQLException
				{
					return this.writeBuffer(position, ByteBuffer.wrap(bytes, offset, length));
				}

				private int writeBuffer(long position, ByteBuffer buffer) throws java.sql.SQLException
				{
					try
					{
						return channel.write(buffer, position);
					}
					catch (IOException e)
					{
						throw new SQLException(e);
					}
				}
				
				public OutputStream setBinaryStream(long position) throws java.sql.SQLException
				{
					try
					{
						return Channels.newOutputStream(channel.position(position));
					}
					catch (IOException e)
					{
						throw new SQLException(e);
					}
				}
	
				public void truncate(long length) throws java.sql.SQLException
				{
					try
					{
						channel.truncate(length);
					}
					catch (IOException e)
					{
						throw new SQLException(e);
					}
				}
				
				@Override
				protected void finalize() throws IOException
				{
					channel.close();
				}
			};
		}
		catch (IOException e)
		{
			throw new SQLException(e);
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.FileSupport#getClob(java.io.File)
	 */
	public Clob getClob(File file) throws java.sql.SQLException
	{
		try
		{
			FileInputStream inputStream = new FileInputStream(file);
			final Charset charset = Charset.forName(new InputStreamReader(inputStream).getEncoding());
			final FileChannel channel = inputStream.getChannel();
			
			// Calculate the number of bytes in a single character
			final int charBytes = "a".getBytes(charset.name()).length;
			
			return new Clob()
			{
				public long length() throws java.sql.SQLException
				{
					try
					{
						return this.charLength(channel.size());
					}
					catch (IOException e)
					{
						throw new SQLException(e);
					}
				}

				public String getSubString(long position, int length) throws java.sql.SQLException
				{
					ByteBuffer buffer = ByteBuffer.allocate(this.byteLength(length));
					
					try
					{
						channel.read(buffer, byteLength(position));
					}
					catch (IOException e)
					{
						throw new SQLException(e);
					}
					
					buffer.compact();
					
					return String.valueOf(charset.decode(buffer).array());
				}

				public Reader getCharacterStream()
				{
					return Channels.newReader(channel, charset.newDecoder(), -1);
				}

				public InputStream getAsciiStream()
				{
					return Channels.newInputStream(channel);
				}

				public long position(String pattern, long position)
				{
					throw new UnsupportedOperationException();
				}

				public long position(Clob pattern, long position)
				{
					throw new UnsupportedOperationException();
				}

				public int setString(long position, String value) throws java.sql.SQLException
				{
					CharBuffer buffer = CharBuffer.wrap(value);
					
					return writeBuffer(position, buffer);
				}

				public int setString(long position, String value, int offset, int length) throws java.sql.SQLException
				{
					CharBuffer buffer = CharBuffer.wrap(value, offset, length);
					
					return writeBuffer(position, buffer);
				}

				private int writeBuffer(long position, CharBuffer buffer) throws java.sql.SQLException
				{
					try
					{
						return channel.write(ByteBuffer.wrap(buffer.toString().getBytes(charset.name())), this.byteLength(position));
					}
					catch (IOException e)
					{
						throw new SQLException(e);
					}
				}

				public OutputStream setAsciiStream(long position) throws java.sql.SQLException
				{
					try
					{
						return Channels.newOutputStream(channel.position(this.byteLength(position)));
					}
					catch (IOException e)
					{
						throw new SQLException(e);
					}
				}

				public Writer setCharacterStream(long position) throws java.sql.SQLException
				{
					try
					{
						return Channels.newWriter(channel.position(this.byteLength(position)), charset.newEncoder(), -1);
					}
					catch (IOException e)
					{
						throw new SQLException(e);
					}
				}

				public void truncate(long length) throws java.sql.SQLException
				{
					try
					{
						channel.truncate(this.byteLength(length));
					}
					catch (IOException e)
					{
						throw new SQLException(e);
					}
				}
				
				@Override
				protected void finalize() throws IOException
				{
					channel.close();
				}
				
				private int byteLength(int charLength)
				{
					return charLength * charBytes;
				}
				
				private long byteLength(long charLength)
				{
					return charLength * charBytes;
				}
				
				private long charLength(long byteLength)
				{
					return byteLength / charBytes;
				}
			};
		}
		catch (IOException e)
		{
			throw new SQLException(e);
		}
	}
}
