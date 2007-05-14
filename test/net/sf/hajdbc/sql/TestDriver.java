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

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.prefs.Preferences;

import net.sf.hajdbc.local.LocalStateManager;

import org.easymock.EasyMock;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit test for {@link Driver}.
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestDriver implements java.sql.Driver
{
	private java.sql.Driver driver = new Driver();
	private Connection connection = EasyMock.createMock(Connection.class);
	
	@BeforeClass
	protected void setUp() throws Exception
	{
		DriverManager.registerDriver(new MockDriver(this.connection));
		Preferences.userNodeForPackage(LocalStateManager.class).put("test-database-cluster", "database1,database2");
	}

	@AfterClass
	protected void tearDown() throws Exception
	{
		DriverManager.deregisterDriver(new MockDriver(this.connection));
		Preferences.userNodeForPackage(LocalStateManager.class).remove("test-database-cluster");
	}

	/**
	 * Test method for {@link Driver} static initialization.
	 */
	@Test
	public void register()
	{
		boolean registered = false;
		
		for (java.sql.Driver driver: Collections.list(DriverManager.getDrivers()))
		{
			if (Driver.class.isInstance(driver))
			{
				registered = true;
			}
		}
		
		assert registered;
	}
	
	@DataProvider(name = "connect")
	public Object[][] getConnectProvider()
	{
		return new Object[][] { new Object[] { "jdbc:ha-jdbc:test-database-cluster", new Properties() }, new Object[] { "jdbc:ha-jdbc:invalid-cluster", new Properties() }, new Object[] { "jdbc:mock", new Properties() } };
	}

	/**
	 * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
	 */
	@Test(dataProvider = "connect")
	public Connection connect(String url, Properties properties)
	{
		try
		{
			Connection connection = this.driver.connect(url, properties);
			
			if (this.driver.acceptsURL(url))
			{
				assert connection != null;
				
				assert Proxy.isProxyClass(connection.getClass());
				assert Proxy.getInvocationHandler(connection).getClass().equals(ConnectionInvocationHandler.class);
			}
			else
			{
				assert connection == null;
			}
		}
		catch (SQLException e)
		{
			assert !url.equals("jdbc:ha-jdbc:test-database-cluster") : e.getMessage();
//			e.printStackTrace();
		}
		
		return null;
	}

	@DataProvider(name = "url")
	public Object[][] getUrlProvider()
	{
		return new Object[][] { new Object[] { "jdbc:ha-jdbc:test-database-cluster" }, new Object[] { "jdbc:ha-jdbc:" }, new Object[] { "jdbc:ha-jdbc" }, new Object[] { "jdbc:mock" } };
	}

	/**
	 * @see java.sql.Driver#acceptsURL(java.lang.String)
	 */
	@Test(dataProvider = "url")
	public boolean acceptsURL(String url) throws SQLException
	{
		boolean accepted = url.startsWith("jdbc:ha-jdbc:") && url.length() > 13;
		
		boolean result = this.driver.acceptsURL(url);
		
		assert result == accepted : url;

		return result;
	}

	/**
	 * @see java.sql.Driver#getPropertyInfo(java.lang.String, java.util.Properties)
	 */
	@Test(dataProvider = "connect")
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties)
	{
		try
		{
			DriverPropertyInfo[] info = this.driver.getPropertyInfo(url, properties);
			
			if (this.driver.acceptsURL(url))
			{
				assert info != null;
			}
			else
			{
				assert info == null;
			}
		}
		catch (SQLException e)
		{
			assert !url.equals("jdbc:ha-jdbc:test-database-cluster") : url;
		}
		
		return null;
	}

	/**
	 * @see java.sql.Driver#getMajorVersion()
	 */
	@Test
	public int getMajorVersion()
	{
		int version = this.driver.getMajorVersion();
		
		assert version == 2 : version;
		
		return version;
	}

	/**
	 * @see java.sql.Driver#getMinorVersion()
	 */
	@Test
	public int getMinorVersion()
	{
		int version = this.driver.getMinorVersion();
		
		assert version == 0 : version;
		
		return version;
	}

	/**
	 * @see java.sql.Driver#jdbcCompliant()
	 */
	@Test
	public boolean jdbcCompliant()
	{
		boolean compliant = this.driver.jdbcCompliant();
		
		assert compliant;
		
		return compliant;
	}
}
