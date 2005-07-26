/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
package net.sf.hajdbc.sql.pool;

import java.io.PrintWriter;
import java.sql.SQLException;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.sql.PooledConnection;

import net.sf.hajdbc.DatabaseClusterTestCase;

public class TestConnectionPoolDataSource extends DatabaseClusterTestCase
{
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		super.setUp();
		
		ConnectionPoolDataSource dataSource = new ConnectionPoolDataSource();
		
		dataSource.setName("pool-datasource-cluster");
		
		this.context.bind("datasource", dataSource);
	}
	
	/**
	 * @see junit.framework.TestCase#tearDown()
	 */
	protected void tearDown() throws Exception
	{
		this.context.unbind("datasource");
		
		super.tearDown();
	}

	private ConnectionPoolDataSource getDataSource() throws NamingException
	{
		return (ConnectionPoolDataSource) this.context.lookup("datasource");
	}
	
	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.ConnectionPoolDataSource.getPooledConnection()'
	 */
	public void testGetPooledConnection()
	{
		try
		{
			PooledConnection connection = this.getDataSource().getPooledConnection();
			
			assertNotNull(connection);
			assertEquals("net.sf.hajdbc.sql.pool.PooledConnection", connection.getClass().getName());
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.ConnectionPoolDataSource.getPooledConnection(String, String)'
	 */
	public void testGetPooledConnectionStringString()
	{
		try
		{
			PooledConnection connection = this.getDataSource().getPooledConnection("test", "test");
			
			assertNotNull(connection);
			assertEquals("net.sf.hajdbc.sql.pool.PooledConnection", connection.getClass().getName());
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.ConnectionPoolDataSource.getLoginTimeout()'
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
	 * Test method for 'net.sf.hajdbc.sql.pool.ConnectionPoolDataSource.setLoginTimeout(int)'
	 */
	public void testSetLoginTimeout()
	{
		try
		{
			this.getDataSource().setLoginTimeout(1);
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.ConnectionPoolDataSource.getLogWriter()'
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
	 * Test method for 'net.sf.hajdbc.sql.pool.ConnectionPoolDataSource.setLogWriter(PrintWriter)'
	 */
	public void testSetLogWriter()
	{
		try
		{
			this.getDataSource().setLogWriter(null);
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
			
			assertEquals("pool-datasource-cluster", name);
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
			
			assertEquals("net.sf.hajdbc.sql.pool.ConnectionPoolDataSource", reference.getClassName());
			assertEquals("net.sf.hajdbc.sql.pool.ConnectionPoolDataSourceFactory", reference.getFactoryClassName());
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}
}
