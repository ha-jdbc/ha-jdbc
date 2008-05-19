/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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

import javax.management.DynamicMBean;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import javax.sql.CommonDataSource;

import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public abstract class TestCommonDataSourceDatabase<C extends CommonDataSourceDatabase<D>, D extends CommonDataSource> extends TestDatabase<C, D> implements InactiveDataSourceDatabaseMBean
{
	private Class<D> dataSourceClass;
	
	protected TestCommonDataSourceDatabase(C database, Class<D> dataSourceClass)
	{
		super(database);
		
		this.dataSourceClass = dataSourceClass;
	}

	@Test
	@Override
	public void testCreateConnectionFactory()
	{
		// Test JNDI-based DataSource
		this.database.setName("test");
		this.database.setProperty(Context.INITIAL_CONTEXT_FACTORY, MockInitialContextFactory.class.getName());
		
		try
		{
			Reference reference = new Reference(this.dataSourceClass.getName(), this.objectFactoryClass().getName(), null);
			
			Context context = new InitialContext(this.database.getProperties());
			
			context.rebind("test", reference);
		}
		catch (NamingException e)
		{
			assert false : e;
		}
		
		D dataSource = this.createConnectionFactory();
		
		assert dataSource.getClass().equals(this.mockDataSourceClass()) : dataSource.getClass();

		this.database.getProperties().clear();
		
		// Test explicit DataSource creation
		this.database.setName(this.mockDataSourceClass().getName());

		dataSource = this.createConnectionFactory();
		
		assert dataSource.getClass().equals(this.mockDataSourceClass()) : dataSource.getClass();
	}

	protected abstract Class<? extends ObjectFactory> objectFactoryClass();
	
	protected abstract Class<? extends D> mockDataSourceClass();
	
	@Test
	@Override
	public void testGetActiveMBean()
	{
		DynamicMBean mbean = this.getActiveMBean();
		
		String className = mbean.getMBeanInfo().getClassName();
		
		try
		{
			assert ActiveDataSourceDatabaseMBean.class.isAssignableFrom(Class.forName(className)) : className;
		}
		catch (ClassNotFoundException e)
		{
			assert false : e;
		}
	}

	@Test
	@Override
	public void testGetInactiveMBean()
	{
		DynamicMBean mbean = this.getInactiveMBean();
		
		String className = mbean.getMBeanInfo().getClassName();
		
		try
		{
			assert InactiveDataSourceDatabaseMBean.class.isAssignableFrom(Class.forName(className)) : className;
		}
		catch (ClassNotFoundException e)
		{
			assert false : e;
		}
	}

	@Test
	public void testGetName()
	{
		String name = this.getName();
		
		assert name == null : name;
		
		this.database.setName("name");
		
		name = this.database.getName();
		
		assert name.equals("name") : name;
	}
	
	/**
	 * @see net.sf.hajdbc.sql.ActiveDataSourceDatabaseMBean#getName()
	 */
	@Override
	public final String getName()
	{
		return this.database.getName();
	}

	@Test(dataProvider = "string")
	@Override
	public final void setName(String name)
	{
		this.database.setName(name);
		
		String value = this.database.getName();
		
		assert value.equals(name) : value;
		
		this.database.clean();
		
		this.database.setName(name);

		value = this.database.getName();
		
		assert value.equals(name) : value;
		
		assert !this.database.isDirty();
		
		this.database.setName(null);
		
		assert this.database.isDirty();
		
		value = this.database.getName();
		
		assert value == null : value;
	}
}
