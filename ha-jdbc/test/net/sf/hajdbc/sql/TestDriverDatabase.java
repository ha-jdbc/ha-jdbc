/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2005 Paul Ferraro
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

import net.sf.hajdbc.EasyMockTestCase;

import org.easymock.MockControl;

/**
 * Unit test for {@link DriverDatabase}
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestDriverDatabase extends EasyMockTestCase
{
	private MockControl driverControl = this.createControl(Driver.class);
	private Driver driver = (Driver) driverControl.getMock();

	/**
	 * Test method for {@link DriverDatabase#connect(Object)}
	 */
	public void testConnect()
	{
		DriverDatabase database = new DriverDatabase();
		
		Connection connection = (Connection) MockControl.createControl(Connection.class).getMock();

		String url = "jdbc:test";
		
		database.setDriver(driver.getClass().getName());
		database.setUrl(url);
		
		try
		{
			this.driver.connect(url, new Properties());
			this.driverControl.setReturnValue(connection);
			
			this.replay();
			
			Connection conn = database.connect(this.driver);
			
			this.verify();
			
			assertSame(connection, conn);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/**
	 * Test method for {@link DriverDatabase#connect(Object)}
	 */
	public void testUnacceptedConnect()
	{
		DriverDatabase database = new DriverDatabase();
		
		String url = "jdbc:test";
		
		database.setDriver(driver.getClass().getName());
		database.setUrl(url);
		
		try
		{
			this.driver.connect(url, new Properties());
			this.driverControl.setReturnValue(null);
			
			this.replay();
			
			Connection connection = database.connect(this.driver);
			
			this.verify();
			
			assertNull(connection);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}
	
	/**
	 * Test method for {@link DriverDatabase#createConnectionFactory()}
	 */
	public void testCreateConnectionFactory()
	{
		String url = "jdbc:test";		
		DriverDatabase database = new DriverDatabase();
		
		database.setDriver(this.driver.getClass().getName());
		database.setUrl(url);
		
		try
		{
			DriverManager.registerDriver(this.driver);
			
			this.driver.acceptsURL(url);
			this.driverControl.setReturnValue(true);
			
			replay();
			
			Object connectionFactory = database.createConnectionFactory();
			
			verify();
			
			assertSame(driver, connectionFactory);
		}
		catch (SQLException e)
		{
			fail();
		}
	}

	/**
	 * Test method for {@link net.sf.hajdbc.sql.AbstractDatabase#equals(Object)}
	 */
	public void testEqualsObject()
	{
		DriverDatabase database1 = new DriverDatabase();
		database1.setId("test1");
		
		DriverDatabase database2 = new DriverDatabase();
		database2.setId("test1");
		
		assertTrue(database1.equals(database2));
		
		database2.setId("test2");
		
		assertFalse(database1.equals(database2));
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.AbstractDatabase.hashCode()'
	 */
	public void testHashCode()
	{
		DriverDatabase database = new DriverDatabase();
		database.setId("test");
		
		int hashCode = database.hashCode();
		
		assertEquals("test".hashCode(), hashCode);
	}
}
