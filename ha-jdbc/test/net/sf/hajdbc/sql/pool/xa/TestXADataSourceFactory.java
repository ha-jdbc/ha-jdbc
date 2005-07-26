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

import javax.naming.Reference;
import javax.naming.StringRefAddr;

import net.sf.hajdbc.DatabaseClusterTestCase;
import net.sf.hajdbc.sql.AbstractDataSource;
import net.sf.hajdbc.sql.pool.ConnectionPoolDataSource;
import net.sf.hajdbc.sql.pool.ConnectionPoolDataSourceFactory;

public class TestXADataSourceFactory extends DatabaseClusterTestCase
{
	private ConnectionPoolDataSourceFactory factory = new ConnectionPoolDataSourceFactory();
	
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

	/*
	 * Test method for 'net.sf.hajdbc.sql.AbstractDataSourceFactory.getObjectInstance(Object, Name, Context, Hashtable)'
	 */
	public void testGetObjectInstance()
	{
		Reference reference = new Reference("net.sf.hajdbc.sql.pool.ConnectionPoolDataSource", new StringRefAddr(AbstractDataSource.NAME, "pool-datasource-cluster"));
		
		try
		{
			Object object = this.factory.getObjectInstance(reference, null, null, null);
			
			assertNotNull(object);
			assertEquals("net.sf.hajdbc.sql.pool.ConnectionPoolDataSource", object.getClass().getName());
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	public void testNullReferenceGetObjectInstance()
	{
		try
		{
			Object object = this.factory.getObjectInstance(null, null, null, null);
			
			assertNull(object);
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	public void testWrongReferenceGetObjectInstance()
	{
		Reference reference = new Reference("net.sf.hajdbc.sql.DataSource", new StringRefAddr(AbstractDataSource.NAME, "pool-datasource-cluster"));
		
		try
		{
			Object object = this.factory.getObjectInstance(reference, null, null, null);
			
			assertNull(object);
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	public void testMissingRefAddrReferenceGetObjectInstance()
	{
		Reference reference = new Reference("net.sf.hajdbc.sql.pool.xa.XADataSource");
		
		try
		{
			Object object = this.factory.getObjectInstance(reference, null, null, null);
			
			assertNull(object);
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}

	public void testInvalidRefAddrReferenceGetObjectInstance()
	{
		Reference reference = new Reference("net.sf.hajdbc.sql.pool.xa.XADataSource", new StringRefAddr(AbstractDataSource.NAME, "invalid-name"));
		
		try
		{
			Object object = this.factory.getObjectInstance(reference, null, null, null);
			
			assertNull(object);
		}
		catch (Exception e)
		{
			this.fail(e);
		}
	}
}
