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
package net.sf.hajdbc.sql.pool.xa;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.sql.XAConnection;

import net.sf.hajdbc.DatabaseClusterTestCase;

public class TestXADataSource extends DatabaseClusterTestCase
{
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		super.setUp();
		
		XADataSource dataSource = new XADataSource();
		
		dataSource.setName("test-xa-datasource-cluster");
		
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

	private XADataSource getDataSource() throws NamingException
	{
		return (XADataSource) this.context.lookup("datasource");
	}
	
	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.xa.XADataSource.getXAConnection()'
	 */
	public void testGetXAConnection()
	{
		try
		{
			XAConnection connection = this.getDataSource().getXAConnection();
			
			assertNotNull(connection);
			assertEquals("net.sf.hajdbc.sql.pool.xa.XAConnection", connection.getClass().getName());
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.xa.XADataSource.getXAConnection(String, String)'
	 */
	public void testGetXAConnectionStringString()
	{
		try
		{
			XAConnection connection = this.getDataSource().getXAConnection("test", "test");
			
			assertNotNull(connection);
			assertEquals("net.sf.hajdbc.sql.pool.xa.XAConnection", connection.getClass().getName());
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
			
			assertEquals("test-xa-datasource-cluster", name);
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
			
			assertEquals("net.sf.hajdbc.sql.pool.xa.XADataSource", reference.getClassName());
			assertEquals("net.sf.hajdbc.sql.pool.xa.XADataSourceFactory", reference.getFactoryClassName());
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}
}
