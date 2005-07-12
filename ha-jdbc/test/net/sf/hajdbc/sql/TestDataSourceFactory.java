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

import java.sql.DriverManager;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Reference;
import javax.naming.StringRefAddr;

import net.sf.hajdbc.AbstractTestCase;

public class TestDataSourceFactory extends AbstractTestCase
{
	private DataSourceFactory factory = new DataSourceFactory();
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

	/*
	 * Test method for 'net.sf.hajdbc.sql.AbstractDataSourceFactory.getObjectInstance(Object, Name, Context, Hashtable)'
	 */
	public void testGetObjectInstance()
	{
		Reference reference = new Reference("net.sf.hajdbc.sql.DataSource", new StringRefAddr(AbstractDataSource.NAME, "datasource-cluster"));
		
		try
		{
			Object object = this.factory.getObjectInstance(reference, null, null, null);
			
			assertNotNull(object);
			assertEquals("net.sf.hajdbc.sql.DataSource", object.getClass().getName());
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
		Reference reference = new Reference("net.sf.hajdbc.sql.pool.ConnectionPoolDataSource", new StringRefAddr(AbstractDataSource.NAME, "datasource-cluster"));
		
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
		Reference reference = new Reference("net.sf.hajdbc.sql.DataSource");
		
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
		Reference reference = new Reference("net.sf.hajdbc.sql.DataSource", new StringRefAddr(AbstractDataSource.NAME, null));
		
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
