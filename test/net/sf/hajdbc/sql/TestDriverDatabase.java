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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import net.sf.hajdbc.ActiveDatabaseMBean;
import net.sf.hajdbc.InactiveDatabaseMBean;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit test for {@link DriverDatabase}
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestDriverDatabase extends AbstractTestDatabase<DriverDatabase, Driver> implements InactiveDriverDatabaseMBean
{
	static
	{
		try
		{
			DriverManager.registerDriver(new MockDriver());
		}
		catch (SQLException e)
		{
			assert false : e;;
		}
	}
	
	private Driver driver = this.control.createMock(Driver.class);
	
	@Override
	protected DriverDatabase createDatabase(String id)
	{
		DriverDatabase database = new DriverDatabase();
		
		database.setId(id);
		database.setUrl("jdbc:mock:test");
		database.setDriver("net.sf.hajdbc.sql.MockDriver");
		
		return database;
	}
	
	@DataProvider(name = "driver")
	public Object[][] driverProvider()
	{
		return new Object[][] { new Object[] { "net.sf.hajdbc.sql.MockDriver" } };
	}
	
	@Test(dataProvider = "driver")
	public void setDriver(String driver)
	{
		this.database.setDriver(null);
		
		String driverClass = this.database.getDriver();
		
		assert driverClass == null : driverClass;
		
		this.database.clean();
		
		this.database.setDriver("");
		
		driverClass = this.database.getDriver();
		
		assert driverClass == null : driverClass;
		
		assert !this.database.isDirty();

		this.database.setDriver(driver);
		
		assert this.database.isDirty();

		this.database.setDriver(driver);
		
		assert this.database.isDirty();

		this.database.clean();
		
		this.database.setDriver("java.sql.Driver");
		
		assert this.database.isDirty();
		
		try
		{
			this.database.setDriver("java.lang.Class");
			
			assert false;
		}
		catch (IllegalArgumentException e)
		{
			assert true;
		}
		
		try
		{
			this.database.setDriver("not.a.valid.Class");
			
			assert false;
		}
		catch (IllegalArgumentException e)
		{
			assert true;
		}
	}
	
	@DataProvider(name = "connection-factory")
	protected Object[][] connectionFactoryProvider()
	{
		return new Object[][] { new Object[] { this.driver } };
	}
	
	/**
	 * @see net.sf.hajdbc.Database#connect(T)
	 */
	@SuppressWarnings("unchecked")
	@Test(dataProvider = "connection-factory")
	public Connection connect(Driver connectionFactory) throws SQLException
	{
		Connection connection = EasyMock.createMock(Connection.class);

		EasyMock.expect(this.driver.connect("jdbc:mock:test", new Properties())).andReturn(connection);
		
		this.control.replay();
		
		Connection conn = this.database.connect(connectionFactory);
		
		this.control.verify();
		
		assert connection == conn;
		
		this.control.reset();
		
		EasyMock.expect(this.driver.connect("jdbc:mock:test", new Properties())).andReturn(null);
		
		this.control.replay();
		
		conn = this.database.connect(connectionFactory);
		
		assert conn == null : conn.getClass().getName();
		
		return conn;
	}

	/**
	 * @see net.sf.hajdbc.Database#createConnectionFactory()
	 */
	@Test
	public Driver createConnectionFactory()
	{
		Driver connectionFactory = this.database.createConnectionFactory();
		
		String connectionFactoryClassName = connectionFactory.getClass().getName();
		
		assert connectionFactoryClassName.equals("net.sf.hajdbc.sql.MockDriver") : connectionFactoryClassName;
		
		return connectionFactory;
	}

	/**
	 * @see net.sf.hajdbc.Database#getActiveMBeanClass()
	 */
	@Test
	public Class<? extends ActiveDatabaseMBean> getActiveMBeanClass()
	{
		Class<? extends ActiveDatabaseMBean> mbeanClass = this.database.getActiveMBeanClass();
		
		assert mbeanClass.equals(ActiveDriverDatabaseMBean.class) : mbeanClass.getName();
		
		return mbeanClass;
	}

	/**
	 * @see net.sf.hajdbc.Database#getInactiveMBeanClass()
	 */
	@Test
	public Class<? extends InactiveDatabaseMBean> getInactiveMBeanClass()
	{
		Class<? extends InactiveDatabaseMBean> mbeanClass = this.database.getInactiveMBeanClass();
		
		assert mbeanClass.equals(InactiveDriverDatabaseMBean.class) : mbeanClass.getName();
		
		return mbeanClass;
	}

	@DataProvider(name = "url")
	protected Object[][] urlProvider()
	{
		return new Object[][] { new Object[] { "jdbc:mock:test" } };
	}
	
	/**
	 * @see net.sf.hajdbc.sql.InactiveDriverDatabaseMBean#setUrl(java.lang.String)
	 */
	@Test(dataProvider = "url")
	public void setUrl(String url)
	{
		this.database.setUrl(url);
		
		assert this.database.getUrl().equals(url) : this.database.getUrl();
		
		this.database.clean();
		
		assert !this.database.isDirty();
		
		this.database.setUrl(url);
		
		assert !this.database.isDirty();
		
		try
		{
			this.database.setUrl("jdbc:test");
			
			assert false;
		}
		catch (IllegalArgumentException e)
		{
			assert true;
		}
		
		assert !this.database.isDirty();
	}

	/**
	 * @see net.sf.hajdbc.sql.ActiveDriverDatabaseMBean#getUrl()
	 */
	@Test
	public String getUrl()
	{
		String url = this.database.getUrl();
		
		assert url.equals("jdbc:mock:test") : url;
		
		return url;
	}

	/**
	 * @see net.sf.hajdbc.sql.ActiveDriverDatabaseMBean#getDriver()
	 */
	@Test
	public String getDriver()
	{
		String driver = this.database.getDriver();
		
		assert driver.equals("net.sf.hajdbc.sql.MockDriver") : driver;
		
		return driver;
	}
}
