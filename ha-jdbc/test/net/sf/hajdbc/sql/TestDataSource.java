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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.Reference;

import net.sf.hajdbc.AbstractTestCase;

public class TestDataSource extends AbstractTestCase
{
	private MockDriver mockDriver = new MockDriver();
	private Context context;
	
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		DriverManager.registerDriver(this.mockDriver);
		
		Properties properties = new Properties();
		
		properties.setProperty(Context.INITIAL_CONTEXT_FACTORY, "net.sf.hajdbc.sql.MockInitialContextFactory");
		
		this.context = new InitialContext(properties);
		
		Reference reference = new Reference(DataSource.class.toString(), "net.sf.hajdbc.sql.MockDataSourceFactory", null);
		
		this.context.rebind("datasource1", reference);
		this.context.rebind("datasource2", reference);
		
		DataSource dataSource = new DataSource();
		
		dataSource.setName("datasource-cluster");
		
		this.context.bind("datasource", dataSource);
	}

	/**
	 * @see junit.framework.TestCase#tearDown()
	 */
	protected void tearDown() throws Exception
	{
		DriverManager.deregisterDriver(this.mockDriver);
		
		this.context.unbind("datasource");
		this.context.unbind("datasource1");
		this.context.unbind("datasource2");
	}

	private DataSource getDataSource() throws NamingException
	{
		return (DataSource) this.context.lookup("datasource");
	}
	
	/*
	 * Test method for 'net.sf.hajdbc.sql.DataSource.getLoginTimeout()'
	 */
	public void testGetLoginTimeout()
	{
		try
		{
			int timeout = this.getDataSource().getLoginTimeout();
			
			assertEquals(0, timeout);
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.DataSource.setLoginTimeout(int)'
	 */
	public void testSetLoginTimeout()
	{
		try
		{
			this.getDataSource().setLoginTimeout(1000);
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.DataSource.getLogWriter()'
	 */
	public void testGetLogWriter()
	{
		try
		{
			PrintWriter writer = this.getDataSource().getLogWriter();
			
			assertNotNull(writer);
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.DataSource.setLogWriter(PrintWriter)'
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
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.DataSource.getConnection()'
	 */
	public void testGetConnection()
	{
		try
		{
			Connection connection = this.getDataSource().getConnection();
			
			assertNotNull(connection);
			assertEquals("net.sf.hajdbc.sql.Connection", connection.getClass().getName());
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.DataSource.getConnection(String, String)'
	 */
	public void testGetConnectionStringString()
	{
		try
		{
			Connection connection = this.getDataSource().getConnection("sa", "");
			
			assertNotNull(connection);
			assertEquals("net.sf.hajdbc.sql.Connection", connection.getClass().getName());
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.AbstractDataSource.getName()'
	 */
	public void testGetName()
	{
		try
		{
			String name = this.getDataSource().getName();
			
			assertEquals("datasource-cluster", name);
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.AbstractDataSource.setName(String)'
	 */
	public void testSetName()
	{
		try
		{
			this.getDataSource().setName("test");
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.AbstractDataSource.getReference()'
	 */
	public void testGetReference()
	{
		try
		{
			Reference reference = this.getDataSource().getReference();
			
			assertEquals("net.sf.hajdbc.sql.DataSource", reference.getClassName());
			assertEquals("net.sf.hajdbc.sql.DataSourceFactory", reference.getFactoryClassName());
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}
}
