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
import javax.naming.spi.ObjectFactory;

import net.sf.hajdbc.DatabaseClusterTestCase;

import org.testng.annotations.Configuration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for {@link DataSourceFactory}.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public class TestDataSourceFactory extends DatabaseClusterTestCase implements ObjectFactory
{
	private DataSourceFactory factory = new DataSourceFactory();
	
	@Override
	@Configuration(beforeTestClass = true)
	public void setUp() throws Exception
	{
		super.setUp();
		
		DataSource dataSource = new DataSource();
		
		dataSource.setCluster("test-datasource-cluster");
		
		this.context.bind("datasource", dataSource);
	}

	@Override
	@Configuration(afterTestClass = true)
	public void tearDown() throws Exception
	{
		this.context.unbind("datasource");
		
		super.tearDown();
	}

	@DataProvider(name = "object-instance")
	Object[][] objectInstanceProvider()
	{
		return new Object[][] { new Object[] { new Reference("net.sf.hajdbc.sql.DataSource", new StringRefAddr(DataSource.DATABASE_CLUSTER, "test-datasource-cluster")), null, null, null } };
	}
	
	/**
	 * @see javax.naming.spi.ObjectFactory#getObjectInstance(java.lang.Object, javax.naming.Name, javax.naming.Context, java.util.Hashtable)
	 */
	@Test(dataProvider = "object-instance")
	public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment) throws Exception
	{
		Object object = this.factory.getObjectInstance(obj, name, nameCtx, environment);
		
		String className = object.getClass().getName();
		
		assert className.equals("net.sf.hajdbc.sql.DataSource") : className;
		
		object = this.factory.getObjectInstance(null, name, nameCtx, environment);
		
		assert object == null;
		
		Reference reference = new Reference("net.sf.hajdbc.sql.Driver", new StringRefAddr(DataSource.DATABASE_CLUSTER, "test-datasource-cluster"));
			
		object = this.factory.getObjectInstance(reference, name, nameCtx, environment);
		
		assert object == null;
		
		reference = new Reference("net.sf.hajdbc.sql.DataSource");
			
		object = this.factory.getObjectInstance(reference, name, nameCtx, environment);
		
		assert object == null;

		try
		{
			reference = new Reference("net.sf.hajdbc.sql.DataSource", new StringRefAddr(DataSource.DATABASE_CLUSTER, "invalid-name"));
			
			this.factory.getObjectInstance(reference, name, nameCtx, environment);
			
			assert false;
		}
		catch (Exception e)
		{
			assert true;
		}
		
		return object;
	}
}
