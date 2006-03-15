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

import org.testng.annotations.Test;

import net.sf.hajdbc.DatabaseClusterTestCase;

/**
 * Unit test for {@link Driver}.
 * @author  Paul Ferraro
 * @since   1.0
 */
@Test
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
		
		assert registered;
	}

	/**
	 * Test method for {@link Driver#acceptsURL(String)}.
	 */
	public void testAcceptsURL()
	{
		boolean accepted = this.driver.acceptsURL("jdbc:ha-jdbc:test-database-cluster");
		
		assert accepted;

		try
		{
			this.driver.acceptsURL("jdbc:ha-jdbc:no-such-cluster");

			assert false;
		}
		catch (IllegalArgumentException e)
		{
			assert true;
		}
		
		try
		{
			this.driver.acceptsURL("jdbc:ha-jdbc:");

			assert false;
		}
		catch (IllegalArgumentException e)
		{
			assert true;
		}
		
		accepted = this.driver.acceptsURL("jdbc:ha-jdbc");
		
		assert !accepted;

		accepted = this.driver.acceptsURL("jdbc:test:database1");
		
		assert !accepted;
	}

	/**
	 * Test method for {@link Driver#connect(String, Properties)}
	 */
	public void testConnect()
	{
		try
		{
			Connection connection = this.driver.connect("jdbc:ha-jdbc:test-database-cluster", null);
			
			assert connection != null;
			
			assert net.sf.hajdbc.sql.Connection.class.equals(connection.getClass()) : connection.getClass().getName();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Driver#getPropertyInfo(String, Properties)}
	 */
	public void testGetPropertyInfo()
	{
		try
		{
			DriverPropertyInfo[] info = this.driver.getPropertyInfo("jdbc:ha-jdbc:test-database-cluster", null);
			
			assert info != null;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Driver#jdbcCompliant()}
	 */
	public void testJdbcCompliant()
	{
		boolean compliant = this.driver.jdbcCompliant();
		
		assert compliant;
	}
}
