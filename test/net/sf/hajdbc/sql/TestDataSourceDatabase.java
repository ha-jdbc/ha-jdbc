/**
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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

import java.sql.Connection;
import java.sql.SQLException;

import javax.management.DynamicMBean;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.sql.DataSource;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for {@link DataSourceDatabase}.
 * @author  Paul Ferraro
 * @since   1.1
 */
public class TestDataSourceDatabase extends AbstractTestDatabase<DataSourceDatabase, DataSource> implements InactiveDataSourceDatabaseMBean
{
	private DataSource dataSource = EasyMock.createStrictMock(DataSource.class);

	void replay()
	{
		EasyMock.replay(this.dataSource);
	}
	
	void verify()
	{
		EasyMock.verify(this.dataSource);
		EasyMock.reset(this.dataSource);
	}
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractTestDatabase#createDatabase(java.lang.String)
	 */
	@Override
	protected DataSourceDatabase createDatabase(String id)
	{
		DataSourceDatabase database = new DataSourceDatabase();
		
		database.setId(id);
		
		return database;
	}

	@DataProvider(name = "datasource")
	Object[][] dataSourceProvider()
	{
		return new Object[][] { new Object[] { this.dataSource } };
	}
	
	/**
	 * @see net.sf.hajdbc.Database#connect(T)
	 */
	@Test(dataProvider = "datasource")
	public Connection connect(DataSource dataSource) throws SQLException
	{
		DataSourceDatabase database = this.createDatabase("1");
		
		Connection connection = EasyMock.createMock(Connection.class);
		
		EasyMock.expect(this.dataSource.getConnection()).andReturn(connection);
		
		this.replay();
		
		Connection result = database.connect(dataSource);
		
		this.verify();
		
		assert result == connection : result.getClass().getName();
		
		database.setUser("user");
		database.setPassword("password");

		EasyMock.expect(this.dataSource.getConnection("user", "password")).andReturn(connection);
		
		this.replay();
		
		result = database.connect(dataSource);
		
		this.verify();
		
		assert result == connection : result.getClass().getName();
		
		return result;
	}

	/**
	 * @see net.sf.hajdbc.Database#createConnectionFactory()
	 */
	@Test
	public DataSource createConnectionFactory()
	{
		DataSourceDatabase database = this.createDatabase("1");
		database.setName("test");
		database.setProperty(Context.INITIAL_CONTEXT_FACTORY, MockInitialContextFactory.class.getName());
		
		try
		{
			Reference reference = new Reference(this.dataSource.getClass().getName(), MockDataSourceFactory.class.getName(), null);
			
			Context context = new InitialContext(database.getProperties());
			
			context.rebind("test", reference);
		}
		catch (NamingException e)
		{
			assert false;
		}
		
		DataSource dataSource = database.createConnectionFactory();
		
		assert dataSource.getClass().equals(MockDataSource.class) : dataSource.getClass();
		
		return dataSource;
	}

	/**
	 * @see net.sf.hajdbc.Database#getActiveMBean()
	 */
	@Test
	public DynamicMBean getActiveMBean()
	{
		DataSourceDatabase database = this.createDatabase("1");
		
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
	public DynamicMBean getInactiveMBean()
	{
		DataSourceDatabase database = this.createDatabase("1");
		
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
	public String getName()
	{
		DataSourceDatabase database = this.createDatabase("1");

		String name = database.getName();
		
		assert name == null : name;
		
		database.setName("name");
		
		name = database.getName();
		
		assert name.equals("name") : name;
		
		return name;
	}
	
	@Test(dataProvider = "string")
	public void setName(String name)
	{
		DataSourceDatabase database = this.createDatabase("1");
		
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
