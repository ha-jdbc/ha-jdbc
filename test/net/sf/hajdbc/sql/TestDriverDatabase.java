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

import net.sf.hajdbc.Database;

import org.easymock.EasyMock;
import org.testng.annotations.Test;


/**
 * Unit test for {@link DriverDatabase}
 * @author  Paul Ferraro
 * @since   1.0
 */
@Test
public class TestDriverDatabase extends AbstractTestDatabase
{
	private Driver driver = this.control.createMock(Driver.class);

	protected Database createDatabase(String id)
	{
		DriverDatabase database = new DriverDatabase();
		
		database.setId(id);
		
		return database;
	}
	
	/**
	 * Test method for {@link DriverDatabase#connect(Driver)}
	 */
	public void testConnect()
	{
		Connection connection = EasyMock.createMock(Connection.class);

		String url = "jdbc:test";
		
		DriverDatabase database = new DriverDatabase();		
		database.setUrl(url);
		
		try
		{
			EasyMock.expect(this.driver.connect(url, new Properties())).andReturn(connection);
			
			this.control.replay();
			
			Connection conn = database.connect(this.driver);
			
			this.control.verify();
			
			assert connection == conn;
			
			this.control.reset();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
	
	/**
	 * Test method for {@link DriverDatabase#connect(Driver)}
	 */
	public void testConnectWithAuthentication()
	{
		DriverDatabase database = new DriverDatabase();
		
		Connection connection = EasyMock.createMock(Connection.class);

		String url = "jdbc:test";
		
		database.setUrl(url);
		database.setUser("user");
		database.setPassword("password");

		Properties properties = new Properties();
		properties.setProperty("user", database.getUser());
		properties.setProperty("password", database.getPassword());

		try
		{
			EasyMock.expect(this.driver.connect(url, properties)).andReturn(connection);
			
			this.control.replay();
			
			Connection conn = database.connect(this.driver);
			
			this.control.verify();
			
			assert connection == conn;
			
			this.control.reset();
		}
		catch (SQLException e)
		{
			assert false : e;
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
			
			assert connection == null;
		}
		catch (SQLException e)
		{
			assert false : e;
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
			
			Driver driver = database.createConnectionFactory();
			
			this.control.verify();
			
			assert this.driver == driver;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
	
	/**
	 * Test method for {@link DriverDatabase#setUrl(String)}
	 */
	public void testSetUrl()
	{
		DriverDatabase database = new DriverDatabase();
		
		assert !database.isDirty();
		
		database.setUrl(null);
		
		assert !database.isDirty();
		
		database.setUrl("test");
		
		assert database.isDirty();

		database.setUrl("test");
		
		assert database.isDirty();

		database.clean();
		
		assert !database.isDirty();
		
		database.setUrl(null);
		
		assert database.isDirty();
		
		database.setUrl("test");
		
		assert database.isDirty();
		
		database.clean();
		
		assert !database.isDirty();
		
		database.setUrl("different");
		
		assert database.isDirty();
	}
	
	/**
	 * Test method for {@link DriverDatabase#setDriver(String)}
	 */
	public void testSetDriver()
	{
		DriverDatabase database = new DriverDatabase();
		
		assert !database.isDirty();
		
		database.setDriver(null);
		
		assert !database.isDirty();
		
		database.setDriver("test");
		
		assert database.isDirty();

		database.setDriver("test");
		
		assert database.isDirty();

		database.clean();
		
		assert !database.isDirty();
		
		database.setDriver(null);
		
		assert database.isDirty();
		
		database.setDriver("test");
		
		assert database.isDirty();
		
		database.clean();
		
		assert !database.isDirty();
		
		database.setDriver("different");
		
		assert database.isDirty();
	}
}
