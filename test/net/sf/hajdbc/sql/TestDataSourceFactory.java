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

import javax.naming.Reference;
import javax.naming.StringRefAddr;

import net.sf.hajdbc.DatabaseClusterTestCase;

import org.testng.annotations.Configuration;
import org.testng.annotations.Test;

/**
 * Unit test for {@link DataSourceFactory}.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
@Test
public class TestDataSourceFactory extends DatabaseClusterTestCase
{
	private DataSourceFactory factory = new DataSourceFactory();
	
	@Configuration(beforeTestClass = true)
	public void setUp() throws Exception
	{
		super.setUp();
		
		DataSource dataSource = new DataSource();
		
		dataSource.setCluster("test-datasource-cluster");
		
		this.context.bind("datasource", dataSource);
	}

	@Configuration(afterTestClass = true)
	public void tearDown() throws Exception
	{
		this.context.unbind("datasource");
		
		super.tearDown();
	}

	/**
	 * Test method for {@link DataSourceFactory#getObjectInstance(Object, Name, Context, Hashtable)}
	 */
	public void testGetObjectInstance()
	{
		Reference reference = new Reference("net.sf.hajdbc.sql.DataSource", new StringRefAddr(DataSource.DATABASE_CLUSTER, "test-datasource-cluster"));
		
		try
		{
			Object object = this.factory.getObjectInstance(reference, null, null, null);
			
			assert object != null;
			assert net.sf.hajdbc.sql.DataSource.class.equals(object.getClass());
		}
		catch (Exception e)
		{
			assert false : e;
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
			
			assert object == null;
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link DataSourceFactory#getObjectInstance(Object, Name, Context, Hashtable)}
	 */
	public void testWrongReferenceGetObjectInstance()
	{
		Reference reference = new Reference("net.sf.hajdbc.sql.Driver", new StringRefAddr(DataSource.DATABASE_CLUSTER, "test-datasource-cluster"));
		
		try
		{
			Object object = this.factory.getObjectInstance(reference, null, null, null);
			
			assert object == null;
		}
		catch (Exception e)
		{
			assert false : e;
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
			
			assert object == null;
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link DataSourceFactory#getObjectInstance(Object, Name, Context, Hashtable)}
	 */
	public void testInvalidRefAddrReferenceGetObjectInstance()
	{
		Reference reference = new Reference("net.sf.hajdbc.sql.DataSource", new StringRefAddr(DataSource.DATABASE_CLUSTER, "invalid-name"));
		
		try
		{
			this.factory.getObjectInstance(reference, null, null, null);
			
			assert false;
		}
		catch (Exception e)
		{
			assert true;
		}
	}
}
