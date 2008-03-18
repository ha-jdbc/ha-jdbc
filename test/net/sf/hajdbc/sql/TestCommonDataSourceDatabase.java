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

import org.easymock.EasyMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public abstract class TestCommonDataSourceDatabase<C extends CommonDataSourceDatabase<D>, D extends CommonDataSource> extends AbstractTestDatabase<C, D> implements InactiveDataSourceDatabaseMBean
{
	protected D dataSource;

	protected TestCommonDataSourceDatabase(D dataSource)
	{
		this.dataSource = dataSource;
	}
	
	protected Object[] objects()
	{
		return new Object[] { this.dataSource };
	}
	
	protected void replay()
	{
		EasyMock.replay(this.objects());
	}
	
	protected void verify()
	{
		EasyMock.verify(this.objects());
	}
	
	@AfterMethod
	protected void reset()
	{
		EasyMock.reset(this.objects());
	}
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractTestDatabase#createDatabase(java.lang.String)
	 */
	@Override
	protected final C createDatabase(String id)
	{
		C database = this.createDatabase();
		
		database.setId(id);
		
		return database;
	}

	protected abstract C createDatabase();
	
	@DataProvider(name = "datasource")
	Object[][] dataSourceProvider()
	{
		return new Object[][] { new Object[] { this.dataSource } };
	}

	/**
	 * @see net.sf.hajdbc.Database#createConnectionFactory()
	 */
	@Test
	public final D createConnectionFactory()
	{
		C database = this.createDatabase("1");
		database.setName("test");
		database.setProperty(Context.INITIAL_CONTEXT_FACTORY, MockInitialContextFactory.class.getName());
		
		try
		{
			Reference reference = new Reference(this.dataSource.getClass().getName(), this.objectFactoryClass().getName(), null);
			
			Context context = new InitialContext(database.getProperties());
			
			context.rebind("test", reference);
		}
		catch (NamingException e)
		{
			assert false;
		}
		
		D dataSource = database.createConnectionFactory();
		
		assert dataSource.getClass().equals(this.mockDataSourceClass()) : dataSource.getClass();
		
		return dataSource;
	}

	protected abstract Class<? extends ObjectFactory> objectFactoryClass();
	
	protected abstract Class<? extends D> mockDataSourceClass();
	
	/**
	 * @see net.sf.hajdbc.Database#getActiveMBean()
	 */
	@Test
	public final DynamicMBean getActiveMBean()
	{
		C database = this.createDatabase("1");
		
		DynamicMBean mbean = database.getActiveMBean();
		
		String className = mbean.getMBeanInfo().getClassName();
		
		try
		{
			assert ActiveDataSourceDatabaseMBean.class.isAssignableFrom(Class.forName(className)) : className;
		}
		catch (ClassNotFoundException e)
		{
			assert false : e;
		}
		
		return mbean;
	}

	/**
	 * @see net.sf.hajdbc.Database#getInactiveMBean()
	 */
	@Test
	public final DynamicMBean getInactiveMBean()
	{
		C database = this.createDatabase("1");
		
		DynamicMBean mbean = database.getInactiveMBean();
		
		String className = mbean.getMBeanInfo().getClassName();
		
		try
		{
			assert InactiveDataSourceDatabaseMBean.class.isAssignableFrom(Class.forName(className)) : className;
		}
		catch (ClassNotFoundException e)
		{
			assert false : e;
		}
		
		return mbean;
	}

	/**
	 * @see net.sf.hajdbc.sql.ActiveDataSourceDatabaseMBean#getName()
	 */
	@Test
	public final String getName()
	{
		C database = this.createDatabase("1");

		String name = database.getName();
		
		assert name == null : name;
		
		database.setName("name");
		
		name = database.getName();
		
		assert name.equals("name") : name;
		
		return name;
	}
	
	@Test(dataProvider = "string")
	public final void setName(String name)
	{
		C database = this.createDatabase("1");
		
		database.setName(name);
		
		String value = database.getName();
		
		assert value.equals(name) : value;
		
		database.clean();
		
		database.setName(name);

		value = database.getName();
		
		assert value.equals(name) : value;
		
		assert !database.isDirty();
		
		database.setName(null);
		
		assert database.isDirty();
		
		value = database.getName();
		
		assert value == null : value;
	}
}
