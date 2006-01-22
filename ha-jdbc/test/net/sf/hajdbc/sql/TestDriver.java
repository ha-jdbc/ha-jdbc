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

import net.sf.hajdbc.DatabaseClusterTestCase;

/**
 * Unit test for {@link Driver}.
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestDriver extends DatabaseClusterTestCase
{
	private Driver driver = new Driver();

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
		
		assertTrue(registered);
	}

	/**
	 * Test method for {@link Driver#acceptsURL(String)}.
	 */
	public void testAcceptsURL()
	{
		try
		{
			boolean accepted = this.driver.acceptsURL("jdbc:ha-jdbc:test-database-cluster");
			
			assertTrue(accepted);

			accepted = this.driver.acceptsURL("jdbc:ha-jdbc:no-such-cluster");

			assertFalse(accepted);

			accepted = this.driver.acceptsURL("jdbc:ha-jdbc:");
			
			assertFalse(accepted);

			accepted = this.driver.acceptsURL("jdbc:ha-jdbc");
			
			assertFalse(accepted);

			accepted = this.driver.acceptsURL("jdbc:test:database1");
			
			assertFalse(accepted);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Driver#connect(String, Properties)}
	 */
	public void testConnect()
	{
		try
		{
			Connection connection = this.driver.connect("jdbc:ha-jdbc:test-database-cluster", null);
			
			assertNotNull(connection);
			assertEquals("net.sf.hajdbc.sql.Connection", connection.getClass().getName());
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Driver#getMajorVersion()}
	 */
	public void testGetMajorVersion()
	{
		int major = this.driver.getMajorVersion();
		
		assertEquals(1, major);
	}

	/**
	 * Test method for {@link Driver#getMinorVersion()}
	 */
	public void testGetMinorVersion()
	{
		int minor = this.driver.getMinorVersion();
		
		assertEquals(0, minor);
	}

	/**
	 * Test method for {@link Driver#getPropertyInfo(String, Properties)}
	 */
	public void testGetPropertyInfo()
	{
		try
		{
			DriverPropertyInfo[] info = this.driver.getPropertyInfo("jdbc:ha-jdbc:test-database-cluster", null);
			
			assertNotNull(info);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Driver#jdbcCompliant()}
	 */
	public void testJdbcCompliant()
	{
		boolean compliant = this.driver.jdbcCompliant();
		
		assertTrue(compliant);
	}
}
