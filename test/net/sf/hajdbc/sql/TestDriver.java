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
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import net.sf.hajdbc.DatabaseClusterTestCase;

/**
 * Unit test for {@link Driver}.
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestDriver extends DatabaseClusterTestCase implements java.sql.Driver
{
	private java.sql.Driver driver = new Driver();

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
		return new Object[][] { new Object[] { "jdbc:ha-jdbc:test-database-cluster", null } };
	}

	@DataProvider(name = "url")
	public Object[][] getUrlProvider()
	{
		return new Object[][] { new Object[] { "jdbc:ha-jdbc:test-database-cluster" } };
	}

	/**
	 * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
	 */
	@Test(dataProvider = "connect")
	public Connection connect(String url, Properties info) throws SQLException
	{
		Connection connection = this.driver.connect("jdbc:ha-jdbc:test-database-cluster", null);
		
		assert connection != null;
		
		assert net.sf.hajdbc.sql.Connection.class.equals(connection.getClass()) : connection.getClass().getName();
		
		return connection;
	}

	/**
	 * @see java.sql.Driver#acceptsURL(java.lang.String)
	 */
	@Test(dataProvider = "url")
	public boolean acceptsURL(String url) throws SQLException
	{
		boolean accepted = this.driver.acceptsURL(url);
		
		assert accepted;

		try
		{
			accepted = this.driver.acceptsURL("jdbc:ha-jdbc:no-such-cluster");

			assert false : accepted;
		}
		catch (IllegalArgumentException e)
		{
			assert true;
		}
		
		accepted = this.driver.acceptsURL("jdbc:ha-jdbc:");

		assert !accepted;
		
		accepted = this.driver.acceptsURL("jdbc:ha-jdbc");
		
		assert !accepted;

		accepted = this.driver.acceptsURL("jdbc:test:database1");
		
		assert !accepted;
		
		return accepted;
	}

	/**
	 * @see java.sql.Driver#getPropertyInfo(java.lang.String, java.util.Properties)
	 */
	@Test(dataProvider = "connect")
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) throws SQLException
	{
		DriverPropertyInfo[] info = this.driver.getPropertyInfo("jdbc:ha-jdbc:test-database-cluster", null);
		
		assert info != null;
		
		return info;
	}

	/**
	 * @see java.sql.Driver#getMajorVersion()
	 */
	@Test
	public int getMajorVersion()
	{
		int version = this.driver.getMajorVersion();
		
		assert version == 1 : version;
		
		return version;
	}

	/**
	 * @see java.sql.Driver#getMinorVersion()
	 */
	@Test
	public int getMinorVersion()
	{
		int version = this.driver.getMinorVersion();
		
		assert version == 2 : version;
		
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
