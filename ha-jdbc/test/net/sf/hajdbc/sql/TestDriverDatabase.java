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

import org.easymock.EasyMock;

import net.sf.hajdbc.EasyMockTestCase;

/**
 * Unit test for {@link DriverDatabase}
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestDriverDatabase extends EasyMockTestCase
{
	private Driver driver = this.control.createMock(Driver.class);

	/**
	 * Test method for {@link DriverDatabase#connect(Driver)}
	 */
	public void testConnect()
	{
		DriverDatabase database = new DriverDatabase();
		
		Connection connection = EasyMock.createMock(Connection.class);

		String url = "jdbc:test";
		
		database.setDriver(this.driver.getClass().getName());
		database.setUrl(url);
		
		try
		{
			EasyMock.expect(this.driver.connect(url, new Properties())).andReturn(connection);
			
			this.control.replay();
			
			Connection conn = database.connect(this.driver);
			
			this.control.verify();
			
			assertSame(connection, conn);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link DriverDatabase#connect(Driver)}
	 */
	public void testUnacceptedConnect()
	{
		DriverDatabase database = new DriverDatabase();
		
		String url = "jdbc:test";
		
		database.setDriver(this.driver.getClass().getName());
		database.setUrl(url);
		
		try
		{
			EasyMock.expect(this.driver.connect(url, new Properties())).andReturn(null);
			
			this.control.replay();
			
			Connection connection = database.connect(this.driver);
			
			this.control.verify();
			
			assertNull(connection);
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			EasyMock.expect(this.driver.acceptsURL(url)).andReturn(true);
			
			this.control.replay();
			
			Object connectionFactory = database.createConnectionFactory();
			
			this.control.verify();
			
			assertSame(this.driver, connectionFactory);
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

	/**
	 * Test method for {@link net.sf.hajdbc.sql.AbstractDatabase#hashCode()}
	 */
	public void testHashCode()
	{
		DriverDatabase database = new DriverDatabase();
		database.setId("test");
		
		int hashCode = database.hashCode();
		
		assertEquals("test".hashCode(), hashCode);
	}
}
