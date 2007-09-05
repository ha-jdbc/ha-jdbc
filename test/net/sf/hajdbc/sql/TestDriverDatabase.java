/*
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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.management.DynamicMBean;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit test for {@link DriverDatabase}
 * @author  Paul Ferraro
 * @since   1.0
 */
@SuppressWarnings("nls")
public class TestDriverDatabase extends AbstractTestDatabase<DriverDatabase, Driver> implements InactiveDriverDatabaseMBean
{
	static
	{
		try
		{
			DriverManager.registerDriver(new MockDriver(null));
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
	
	private Driver driver = EasyMock.createStrictMock(Driver.class);
	
	void replay()
	{
		EasyMock.replay(this.driver);
	}
	
	void verify()
	{
		EasyMock.verify(this.driver);
		EasyMock.reset(this.driver);
	}
	
	@Override
	protected DriverDatabase createDatabase(String id)
	{
		DriverDatabase database = new DriverDatabase();
		
		database.setId(id);
		
		return database;
	}
	
	@DataProvider(name = "driver-class")
	public Object[][] driverClassProvider()
	{
		return new Object[][] { new Object[] { MockDriver.class.getName() }, new Object[] { Object.class.getName() }, new Object[] { "invalid.class" }, new Object[] { "" }, new Object[] { null } };
	}
	
	@Test(dataProvider = "driver-class")
	public void setDriver(String driver)
	{
		boolean isDriver = false;
		
		try
		{
			isDriver = (driver == null) || driver.isEmpty() || Driver.class.isAssignableFrom(Class.forName(driver));
		}
		catch (ClassNotFoundException e)
		{
			// Ignore
		}
		
		DriverDatabase database = this.createDatabase("1");
		
		database.clean();
		
		try
		{
			database.setDriver(driver);
			
			assert isDriver : driver;
			
			if ((driver == null) || driver.isEmpty())
			{
				assert !database.isDirty();
			}
			else
			{
				assert database.isDirty();
				
				database.clean();
			}
			
			database.setDriver(driver);
			
			assert !database.isDirty();
		}
		catch (IllegalArgumentException e)
		{
			assert !isDriver : driver;
		
			assert !database.isDirty();
		}
	}
	
	@DataProvider(name = "driver")
	Object[][] driverProvider()
	{
		return new Object[][] { new Object[] { this.driver } };
	}
	
	/**
	 * @see net.sf.hajdbc.Database#connect(T)
	 */
	@SuppressWarnings("unchecked")
	@Test(dataProvider = "driver")
	public Connection connect(Driver driver) throws SQLException
	{
		DriverDatabase database = this.createDatabase("1");
		
		database.setUrl("jdbc:mock:test");
		
		Connection connection = EasyMock.createMock(Connection.class);

		EasyMock.expect(this.driver.connect("jdbc:mock:test", new Properties())).andReturn(connection);
		
		this.replay();
		
		Connection result = database.connect(driver);
		
		this.verify();
		
		assert connection == result;
		
		database.setUser("a");
		database.setPassword("b");
		
		Properties properties = new Properties();
		
		properties.setProperty("user", "a");
		properties.setProperty("password", "b");
		
		EasyMock.expect(this.driver.connect("jdbc:mock:test", properties)).andReturn(connection);
		
		this.replay();
		
		result = database.connect(driver);
		
		this.verify();
		
		assert result == connection;

		database.setProperty("ssl", "true");
		
		EasyMock.expect(this.driver.connect("jdbc:mock:test", properties)).andReturn(connection);
		
		this.replay();
		
		result = database.connect(driver);
		
		this.verify();
		
		assert result == connection;
		
		return result;
	}

	/**
	 * @see net.sf.hajdbc.Database#createConnectionFactory()
	 */
	@Test
	public Driver createConnectionFactory()
	{
		DriverDatabase database = this.createDatabase("1");
		database.setDriver(MockDriver.class.getName());
		database.setUrl("jdbc:mock:test");
		
		Driver driver = database.createConnectionFactory();
		
		assert driver.getClass().equals(MockDriver.class) : driver.getClass().getName();
		
		return driver;
	}

	@DataProvider(name = "url")
	Object[][] urlProvider()
	{
		return new Object[][] { new Object[] { "jdbc:mock:test" }, new Object[] { "jdbc:invalid" } };
	}
	
	/**
	 * @see net.sf.hajdbc.sql.InactiveDriverDatabaseMBean#setUrl(java.lang.String)
	 */
	@Test(dataProvider = "url")
	public void setUrl(String url)
	{
		DriverDatabase database = this.createDatabase("1");
		
		database.clean();
		
		boolean accepted = url.startsWith("jdbc:mock");
		
		try
		{
			database.setUrl(url);
			
			assert accepted : url;
			
			String value = database.getUrl();
			
			assert value.equals(url) : value;
			
			assert database.isDirty();
			
			database.clean();
			
			assert !database.isDirty();
			
			database.setUrl(url);
			
			assert !database.isDirty();
		}
		catch (IllegalArgumentException e)
		{
			assert !accepted : url;
		
			assert !database.isDirty();
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.ActiveDriverDatabaseMBean#getUrl()
	 */
	@Test
	public String getUrl()
	{
		DriverDatabase database = this.createDatabase("1");

		String url = database.getUrl();
		
		assert url == null : url;
		
		database.setUrl("jdbc:mock:test");
		
		url = database.getUrl();
		
		assert url.equals("jdbc:mock:test") : url;
		
		return url;
	}

	/**
	 * @see net.sf.hajdbc.sql.ActiveDriverDatabaseMBean#getDriver()
	 */
	@Test
	public String getDriver()
	{
		DriverDatabase database = this.createDatabase("1");

		String driver = database.getDriver();
		
		assert driver == null : driver;
		
		database.setDriver(MockDriver.class.getName());

		driver = database.getDriver();

		assert driver.equals(MockDriver.class.getName()) : driver;
		
		return driver;
	}

	/**
	 * @see net.sf.hajdbc.Database#getActiveMBean()
	 */
	@Test
	public DynamicMBean getActiveMBean()
	{
		DriverDatabase database = this.createDatabase("1");
		
		DynamicMBean mbean = database.getActiveMBean();
		
		String className = mbean.getMBeanInfo().getClassName();
		
		try
		{
			assert ActiveDriverDatabaseMBean.class.isAssignableFrom(Class.forName(className)) : className;
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
		DriverDatabase database = this.createDatabase("1");
		
		DynamicMBean mbean = database.getInactiveMBean();
		
		String className = mbean.getMBeanInfo().getClassName();
		
		try
		{
			assert InactiveDriverDatabaseMBean.class.isAssignableFrom(Class.forName(className)) : className;
		}
		catch (ClassNotFoundException e)
		{
			assert false : e;
		}
		
		return mbean;
	}
}
