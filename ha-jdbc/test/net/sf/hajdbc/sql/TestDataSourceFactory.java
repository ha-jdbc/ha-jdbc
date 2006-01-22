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

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.StringRefAddr;

import net.sf.hajdbc.DatabaseClusterTestCase;

/**
 * Unit test for {@link DataSourceFactory}.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public class TestDataSourceFactory extends DatabaseClusterTestCase
{
	private DataSourceFactory factory = new DataSourceFactory();
	
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		super.setUp();
		
		DataSource dataSource = new DataSource();
		
		dataSource.setName("test-datasource-cluster");
		
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

	/**
	 * Test method for {@link DataSourceFactory#getObjectInstance(Object, Name, Context, Hashtable)}
	 */
	public void testGetObjectInstance()
	{
		Reference reference = new Reference("net.sf.hajdbc.sql.DataSource", new StringRefAddr(DataSource.NAME, "test-datasource-cluster"));
		
		try
		{
			Object object = this.factory.getObjectInstance(reference, null, null, null);
			
			assertNotNull(object);
			assertEquals("net.sf.hajdbc.sql.DataSource", object.getClass().getName());
		}
		catch (Exception e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link DataSourceFactory#getObjectInstance(Object, Name, Context, Hashtable)}
	 */
	public void testNullReferenceGetObjectInstance()
	{
		try
		{
			Object object = this.factory.getObjectInstance(null, null, null, null);
			
			assertNull(object);
		}
		catch (Exception e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link DataSourceFactory#getObjectInstance(Object, Name, Context, Hashtable)}
	 */
	public void testWrongReferenceGetObjectInstance()
	{
		Reference reference = new Reference("net.sf.hajdbc.sql.Driver", new StringRefAddr(DataSource.NAME, "test-datasource-cluster"));
		
		try
		{
			Object object = this.factory.getObjectInstance(reference, null, null, null);
			
			assertNull(object);
		}
		catch (Exception e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link DataSourceFactory#getObjectInstance(Object, Name, Context, Hashtable)}
	 */
	public void testMissingRefAddrReferenceGetObjectInstance()
	{
		Reference reference = new Reference("net.sf.hajdbc.sql.DataSource");
		
		try
		{
			Object object = this.factory.getObjectInstance(reference, null, null, null);
			
			assertNull(object);
		}
		catch (Exception e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link DataSourceFactory#getObjectInstance(Object, Name, Context, Hashtable)}
	 */
	public void testInvalidRefAddrReferenceGetObjectInstance()
	{
		Reference reference = new Reference("net.sf.hajdbc.sql.DataSource", new StringRefAddr(DataSource.NAME, "invalid-name"));
		
		try
		{
			Object object = this.factory.getObjectInstance(reference, null, null, null);
			
			assertNull(object);
		}
		catch (Exception e)
		{
			fail(e);
		}
	}
}
