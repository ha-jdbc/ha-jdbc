/**
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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;

import javax.naming.NamingException;
import javax.naming.Reference;

import org.testng.annotations.Configuration;
import org.testng.annotations.Test;

import net.sf.hajdbc.DatabaseClusterTestCase;

/**
 * Unit test for {@link TestDataSource}.
 * @author  Paul Ferraro
 * @since   1.1
 */
@Test
public class TestDataSource extends DatabaseClusterTestCase
{
	@Configuration(beforeTestClass = true)
	public void setUp() throws Exception
	{
		super.setUp();
		
		DataSource dataSource = new DataSource();
		
		dataSource.setName("test-datasource-cluster");
		
		this.context.bind("datasource", dataSource);
	}

	@Configuration(afterTestClass = true)
	public void tearDown() throws Exception
	{
		this.context.unbind("datasource");
		
		super.tearDown();
	}

	private DataSource getDataSource() throws NamingException
	{
		return (DataSource) this.context.lookup("datasource");
	}
	
	/**
	 * Test method for {@link DataSource#getLoginTimeout()}
	 */
	public void testGetLoginTimeout()
	{
		try
		{
			int timeout = this.getDataSource().getLoginTimeout();
			
			assert timeout == 0 : timeout;
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link DataSource#setLoginTimeout(int)}
	 */
	public void testSetLoginTimeout()
	{
		try
		{
			this.getDataSource().setLoginTimeout(1000);
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link DataSource#getLogWriter()}
	 */
	public void testGetLogWriter()
	{
		try
		{
			PrintWriter writer = this.getDataSource().getLogWriter();
			
			assert writer != null;
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link DataSource#setLogWriter(PrintWriter)}
	 */
	public void testSetLogWriter()
	{
		try
		{
			PrintWriter writer = new PrintWriter(new StringWriter());
			
			this.getDataSource().setLogWriter(writer);
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link DataSource#getConnection()}
	 */
	public void testGetConnection()
	{
		try
		{
			Connection connection = this.getDataSource().getConnection();
			
			assert connection != null;
			assert net.sf.hajdbc.sql.Connection.class.equals(connection.getClass()) : connection.getClass().getName();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link DataSource#getConnection(String, String)}
	 */
	public void testGetConnectionStringString()
	{
		try
		{
			Connection connection = this.getDataSource().getConnection("sa", "");
			
			assert connection != null;
			assert net.sf.hajdbc.sql.Connection.class.equals(connection.getClass()) : connection.getClass().getName();
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link DataSource#getName()}
	 */
	public void testGetName()
	{
		try
		{
			String name = this.getDataSource().getName();
			
			assert name.equals("test-datasource-cluster");
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link DataSource#setName(String)}
	 */
	public void testSetName()
	{
		try
		{
			this.getDataSource().setName("test");
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link DataSource#getReference()}
	 */
	public void testGetReference()
	{
		try
		{
			Reference reference = this.getDataSource().getReference();
			
			assert reference.getClassName().equals("net.sf.hajdbc.sql.DataSource");
			assert reference.getFactoryClassName().equals("net.sf.hajdbc.sql.DataSourceFactory");
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}
}
