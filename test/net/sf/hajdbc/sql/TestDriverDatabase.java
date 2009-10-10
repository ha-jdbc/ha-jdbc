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
public class TestDriverDatabase extends TestDatabase<DriverDatabase, Driver> implements InactiveDriverDatabaseMBean
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
	
	/**
	 * @see net.sf.hajdbc.sql.TestDatabase#createDatabase(java.lang.String)
	 */
	@Override
	protected DriverDatabase createDatabase(String id)
	{
		DriverDatabase database = new DriverDatabase();
		
		database.setId(id);
		
		return database;
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
		
		this.database.clean();
		
		try
		{
			this.database.setDriver(driver);
			
			assert isDriver : driver;
			
			if ((driver == null) || driver.isEmpty())
			{
				assert !this.database.isDirty();
			}
			else
			{
				assert this.database.isDirty();
				
				this.database.clean();
			}
			
			this.database.setDriver(driver);
			
			assert !this.database.isDirty();
		}
		catch (IllegalArgumentException e)
		{
			assert !isDriver : driver;
		
			assert !this.database.isDirty();
		}
	}
	
	@DataProvider(name = "driver")
	Object[][] driverProvider()
	{
		return new Object[][] { new Object[] { this.driver } };
	}
	
	/**
	 * @see net.sf.hajdbc.sql.TestDatabase#testConnect()
	 */
	@Override
	public void testConnect() throws SQLException
	{
		this.database.setUrl("jdbc:mock:test");
		
		Connection connection = EasyMock.createMock(Connection.class);

		EasyMock.expect(this.driver.connect("jdbc:mock:test", new Properties())).andReturn(connection);
		
		this.replay();
		
		Connection result = this.database.connect(this.driver);
		
		this.verify();
		
		assert connection == result;
		
		this.database.setUser("a");
		this.database.setPassword("b");
		
		Properties properties = new Properties();
		
		properties.setProperty("user", "a");
		properties.setProperty("password", "b");
		
		EasyMock.expect(this.driver.connect("jdbc:mock:test", properties)).andReturn(connection);
		
		this.replay();
		
		result = this.database.connect(this.driver);
		
		this.verify();
		
		assert result == connection;

		this.database.setProperty("ssl", "true");
		
		EasyMock.expect(this.driver.connect("jdbc:mock:test", properties)).andReturn(connection);
		
		this.replay();
		
		result = this.database.connect(this.driver);
		
		this.verify();
		
		assert result == connection;
	}

	/**
	 * @see net.sf.hajdbc.sql.TestDatabase#testCreateConnectionFactory()
	 */
	@Override
	public void testCreateConnectionFactory()
	{
		this.database.setDriver(MockDriver.class.getName());
		this.database.setUrl("jdbc:mock:test");
		
		Driver driver = this.database.createConnectionSource();
		
		assert driver.getClass().equals(MockDriver.class) : driver.getClass().getName();
	}

	/**
	 * @see net.sf.hajdbc.sql.TestDatabase#testGetActiveMBean()
	 */
	@Override
	public void testGetActiveMBean()
	{
		DynamicMBean mbean = this.getActiveMBean();
		
		String className = mbean.getMBeanInfo().getClassName();
		
		try
		{
			assert ActiveDriverDatabaseMBean.class.isAssignableFrom(Class.forName(className)) : className;
		}
		catch (ClassNotFoundException e)
		{
			assert false : e;
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.TestDatabase#testGetInactiveMBean()
	 */
	@Override
	public void testGetInactiveMBean()
	{
		DynamicMBean mbean = this.getInactiveMBean();
		
		String className = mbean.getMBeanInfo().getClassName();
		
		try
		{
			assert InactiveDriverDatabaseMBean.class.isAssignableFrom(Class.forName(className)) : className;
		}
		catch (ClassNotFoundException e)
		{
			assert false : e;
		}
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
		this.database.clean();
		
		boolean accepted = url.startsWith("jdbc:mock");
		
		try
		{
			this.database.setUrl(url);
			
			assert accepted : url;
			
			String value = this.database.getUrl();
			
			assert value.equals(url) : value;
			
			assert this.database.isDirty();
			
			this.database.clean();
			
			assert !this.database.isDirty();
			
			this.database.setUrl(url);
			
			assert !this.database.isDirty();
		}
		catch (IllegalArgumentException e)
		{
			assert !accepted : url;
		
			assert !this.database.isDirty();
		}
	}

	public void testGetUrl()
	{
		String url = this.getUrl();
		
		assert url == null : url;
		
		this.database.setUrl("jdbc:mock:test");
		
		url = this.getUrl();
		
		assert url.equals("jdbc:mock:test") : url;
	}
	
	/**
	 * @see net.sf.hajdbc.sql.ActiveDriverDatabaseMBean#getUrl()
	 */
	@Override
	public String getUrl()
	{
		return this.database.getUrl();
	}

	public void testGetDriver()
	{
		String driver = this.getDriver();
		
		assert driver == null : driver;
		
		this.database.setDriver(MockDriver.class.getName());

		driver = this.getDriver();

		assert driver.equals(MockDriver.class.getName()) : driver;
	}
	
	/**
	 * @see net.sf.hajdbc.sql.ActiveDriverDatabaseMBean#getDriver()
	 */
	@Override
	public String getDriver()
	{
		return this.database.getDriver();
	}
}
