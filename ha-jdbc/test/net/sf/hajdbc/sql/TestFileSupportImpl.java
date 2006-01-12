/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2005 Paul Ferraro
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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import net.sf.hajdbc.EasyMockTestCase;

/**
 * Unit test for {@link FileSupportImpl}.
 *
 * @author  Paul Ferraro
 * @since   1.1
 */
public class TestFileSupportImpl extends EasyMockTestCase
{
	private FileSupport fileSupport;

	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		this.fileSupport = new FileSupportImpl();
	}
	
	/**
	 * @see net.sf.hajdbc.EasyMockTestCase#tearDown()
	 */
	protected void tearDown() throws Exception
	{
		this.fileSupport.close();

		super.tearDown();
	}

	/**
	 * Test method for {@link FileSupportImpl#createFile(InputStream)}
	 */
	public void testCreateFileInputStream()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		try
		{
			File file = this.fileSupport.createFile(inputStream);
			
			assertNotNull(file);
			assertTrue(file.exists());
			assertTrue(file.getName().startsWith("ha-jdbc-"));
			assertTrue(file.getName().endsWith(".lob"));
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link FileSupportImpl#createFile(Reader)}
	 */
	public void testCreateFileReader()
	{
		Reader reader = new StringReader("");
		
		try
		{
			File file = this.fileSupport.createFile(reader);
			
			assertNotNull(file);
			assertTrue(file.exists());
			assertTrue(file.getName().startsWith("ha-jdbc-"));
			assertTrue(file.getName().endsWith(".lob"));
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link FileSupportImpl#getReader(File)}
	 */
	public void testGetReader()
	{
		try
		{
			File file = File.createTempFile("test", ".test");
			Writer writer = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file)));
			writer.write("test");
			writer.flush();
			writer.close();
			
			Reader reader = this.fileSupport.getReader(file);
			
			char[] buffer = new char[4];
			
			assertNotNull(reader);
			assertEquals(4, reader.read(buffer));
			assertEquals("test", new String(buffer));
			assertEquals(-1, reader.read(buffer));
		}
		catch (IOException e)
		{
			fail(e);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link FileSupportImpl#getInputStream(File)}
	 */
	public void testGetInputStream()
	{
		try
		{
			File file = File.createTempFile("test", ".test");
			OutputStream outputStream = new GZIPOutputStream(new FileOutputStream(file));
			outputStream.write(new byte[] { 1, 2, 3, 4 });
			outputStream.flush();
			outputStream.close();
			
			InputStream inputStream = this.fileSupport.getInputStream(file);
			
			byte[] buffer = new byte[4];
			
			assertNotNull(inputStream);
			assertEquals(4, inputStream.read(buffer));
			assertTrue(Arrays.equals(new byte[] { 1, 2, 3, 4 }, buffer));
			assertEquals(-1, inputStream.read(buffer));
		}
		catch (IOException e)
		{
			fail(e);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link FileSupportImpl#close()}
	 */
	public void testClose()
	{
		Reader reader = new StringReader("");
		
		try
		{
			File file = this.fileSupport.createFile(reader);
			
			assertTrue(file.exists());
			
			this.fileSupport.close();
			
			assertFalse(file.exists());
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}
}
