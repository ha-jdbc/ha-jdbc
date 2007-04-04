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

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Arrays;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for {@link FileSupportImpl}.
 *
 * @author  Paul Ferraro
 * @since   1.1
 */
public class TestFileSupportImpl implements FileSupport
{
	private IMocksControl control = EasyMock.createStrictControl();
	
	private FileSupport fileSupport = new FileSupportImpl();
	
	@AfterMethod
	void reset()
	{
		this.fileSupport.close();
		this.control.reset();
	}

	/**
	 * @see net.sf.hajdbc.sql.FileSupport#close()
	 */
	@Test
	public void close()
	{
		try
		{
			File file = this.fileSupport.createFile(new ByteArrayInputStream(new byte[0]));
			
			assert file.exists();
			
			this.fileSupport.close();
			
			assert !file.exists();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	@DataProvider(name = "inputStream")
	Object[][] inputStreamProvider()
	{
		return new Object[][] { new Object[] { new ByteArrayInputStream(new byte[] { 1, 2, 3, 4 }) } };
	}
	
	/**
	 * @see net.sf.hajdbc.sql.FileSupport#createFile(java.io.InputStream)
	 */
	@Test(dataProvider = "inputStream")
	public File createFile(InputStream inputStream) throws SQLException
	{
		File file = this.fileSupport.createFile(inputStream);
		
		assert file != null;
		assert file.exists();
		assert file.getName().startsWith("ha-jdbc-") : file.getName();
		assert file.getName().endsWith(".lob") : file.getName();
		assert file.length() == 4 : file.length();
		
		return file;
	}

	@DataProvider(name = "reader")
	Object[][] readerProvider()
	{
		return new Object[][] { new Object[] { new CharArrayReader("abcd".toCharArray()) } };
	}

	/**
	 * @see net.sf.hajdbc.sql.FileSupport#createFile(java.io.Reader)
	 */
	@Test(dataProvider = "reader")
	public File createFile(Reader reader) throws SQLException
	{
		File file = this.fileSupport.createFile(reader);
		
		assert file != null;
		assert file.exists();
		assert file.getName().startsWith("ha-jdbc-") : file.getName();
		assert file.getName().endsWith(".lob") : file.getName();
		assert file.length() == 4 : file.length();
		
		return file;
	}

	@DataProvider(name = "file")
	Object[][] fileProvider() throws IOException
	{
		File file = File.createTempFile("test", ".test");

		Writer writer = new FileWriter(file);
		writer.write("abcd");
		writer.flush();
		writer.close();
		
		return new Object[][] { new Object[] { file } };
	}

	/**
	 * @see net.sf.hajdbc.sql.FileSupport#getInputStream(java.io.File)
	 */
	@Test(dataProvider = "file")
	public InputStream getInputStream(File file) throws SQLException
	{
		InputStream inputStream = this.fileSupport.getInputStream(file);
		
		byte[] buffer = new byte[4];
		
		assert inputStream != null;
		
		try
		{
			assert inputStream.read(buffer) == 4;
			assert Arrays.equals(buffer, "abcd".getBytes());
			assert inputStream.read(buffer) < 0;
		}
		catch (IOException e)
		{
			assert false : e;
		}
		
		return inputStream;
	}

	/**
	 * @see net.sf.hajdbc.sql.FileSupport#getReader(java.io.File)
	 */
	@Test(dataProvider = "file")
	public Reader getReader(File file) throws SQLException
	{
		Reader reader = this.fileSupport.getReader(file);
		
		char[] buffer = new char[4];
		
		assert reader != null;
		
		try
		{
			assert reader.read(buffer) == 4;
			assert new String(buffer).equals("abcd");
			assert reader.read(buffer) < 0;
		}
		catch (IOException e)
		{
			assert false : e;
		}
		
		return reader;
	}
}
