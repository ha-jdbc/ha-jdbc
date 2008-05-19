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
@SuppressWarnings("nls")
@Test
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
	public void testRegister()
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
		return new Object[][] {
			new Object[] { "jdbc:ha-jdbc:test-database-cluster", new Properties() },
			new Object[] { "jdbc:ha-jdbc:invalid-cluster", new Properties() }, 
			new Object[] { "jdbc:mock", new Properties() }
		};
	}

	@Test(dataProvider = "connect")
	public void testConnect(String url, Properties properties)
	{
		try
		{
			Connection connection = this.connect(url, properties);
			
			if (url.equals("jdbc:mock"))
			{
				assert connection == null : url;
			}
			else
			{
				assert connection != null : url;
				
				assert Proxy.isProxyClass(connection.getClass());
				assert Proxy.getInvocationHandler(connection).getClass().equals(ConnectionInvocationHandler.class);
			}
		}
		catch (SQLException e)
		{
			assert !url.equals("jdbc:ha-jdbc:test-database-cluster") : e;
		}
	}
	
	/**
	 * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
	 */
	@Override
	public Connection connect(String url, Properties properties) throws SQLException
	{
		return this.driver.connect(url, properties);
	}

	@DataProvider(name = "url")
	public Object[][] getUrlProvider()
	{
		return new Object[][] {
			new Object[] { "jdbc:ha-jdbc:test-database-cluster" },
			new Object[] { "jdbc:ha-jdbc:" },
			new Object[] { "jdbc:ha-jdbc" },
			new Object[] { "jdbc:mock" }
		};
	}

	@Test(dataProvider = "url")
	public void testAcceptsURL(String url)
	{
		boolean accepted = url.startsWith("jdbc:ha-jdbc:") && url.length() > 13;
		
		boolean result = this.acceptsURL(url);
		
		assert result == accepted : url;
	}
	
	/**
	 * @see java.sql.Driver#acceptsURL(java.lang.String)
	 */
	@Override
	public boolean acceptsURL(String url)
	{
		try
		{
			return this.driver.acceptsURL(url);
		}
		catch (SQLException e)
		{
			throw new AssertionError(e);
		}
	}

	@Test(dataProvider = "connect")
	public void testGetPropertyInfo(String url, Properties properties)
	{
		try
		{
			DriverPropertyInfo[] info = this.getPropertyInfo(url, properties);
			
			if (url.equals("jdbc:mock"))
			{
				assert info == null : url;
			}
			else
			{
				assert info != null : url;
			}
		}
		catch (SQLException e)
		{
			assert !url.equals("jdbc:ha-jdbc:test-database-cluster") : e;
		}
	}
	
	/**
	 * @see java.sql.Driver#getPropertyInfo(java.lang.String, java.util.Properties)
	 */
	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) throws SQLException
	{
		return this.driver.getPropertyInfo(url, properties);
	}

	public void testGetMajorVersion()
	{
		int version = this.getMajorVersion();
		
		assert version == 2 : version;
	}
	
	/**
	 * @see java.sql.Driver#getMajorVersion()
	 */
	@Override
	public int getMajorVersion()
	{
		return this.driver.getMajorVersion();
	}

	public void testGetMinorVersion()
	{
		int version = this.getMinorVersion();
		
		assert version == 0 : version;
	}
	
	/**
	 * @see java.sql.Driver#getMinorVersion()
	 */
	@Override
	public int getMinorVersion()
	{
		return this.driver.getMinorVersion();
	}

	public void testJdbcCompliant()
	{
		boolean compliant = this.jdbcCompliant();
		
		assert compliant;
	}
	
	/**
	 * @see java.sql.Driver#jdbcCompliant()
	 */
	@Override
	public boolean jdbcCompliant()
	{
		return this.driver.jdbcCompliant();
	}
}
