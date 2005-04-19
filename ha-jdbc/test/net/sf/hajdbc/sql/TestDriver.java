/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.prefs.Preferences;

import net.sf.hajdbc.local.LocalDatabaseCluster;

public class TestDriver
{
	private Driver driver;
	
	/**
	 * @testng.configuration beforeTestMethod = "true"
	 */
	public void setUp() throws Exception
	{
		Preferences.userNodeForPackage(LocalDatabaseCluster.class).remove("cluster");
		
		this.driver = new Driver();
	}

	/**
	 * @testng.test
	 */
	public void testAcceptsURL()
	{
		try
		{
			boolean accepted = this.driver.acceptsURL("jdbc:ha-jdbc:cluster");
			
			assert accepted;

			accepted = this.driver.acceptsURL("jdbc:ha-jdbc:no-such-cluster");
			
			assert !accepted;

			accepted = this.driver.acceptsURL("jdbc:ha-jdbc:");
			
			assert !accepted;

			accepted = this.driver.acceptsURL("jdbc:ha-jdbc");
			
			assert !accepted;

			accepted = this.driver.acceptsURL("jdbc:hsqldb:mem:database1");
			
			assert !accepted;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testConnect()
	{
		try
		{
			Properties properties = new Properties();
			properties.setProperty("user", "sa");
			properties.setProperty("password", "");
			
			java.sql.Connection connection = this.driver.connect("jdbc:ha-jdbc:cluster", properties);
			
			assert Connection.class.isInstance(connection);
			
			connection.close();
		}
		catch (SQLException e)
		{
			assert false;
		}
	}

	/**
	 * @testng.test
	 */
	public void testGetMajorVersion()
	{
		int major = this.driver.getMajorVersion();
		
		assert major == 1;
	}

	/**
	 * @testng.test
	 */
	public void testGetMinorVersion()
	{
		int minor = this.driver.getMinorVersion();
		
		assert minor == 0;
	}

	/**
	 * @testng.test
	 */
	public void testGetPropertyInfo()
	{
		try
		{
			Properties properties = new Properties();
			properties.setProperty("user", "sa");
			properties.setProperty("password", "");
			
			DriverPropertyInfo[] info = this.driver.getPropertyInfo("jdbc:ha-jdbc:cluster", properties);
			
			assert info != null;
		}
		catch (SQLException e)
		{
			assert false : e.getMessage();
		}
	}

	/**
	 * @testng.test
	 */
	public void testJdbcCompliant()
	{
		boolean compliant = this.driver.jdbcCompliant();
		
		assert compliant;
	}
}
