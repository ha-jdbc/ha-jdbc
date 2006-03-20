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
import org.testng.annotations.Configuration;
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
	
	@Configuration(beforeTestClass = true)
	protected void setup()
	{
		try
		{
			DriverManager.registerDriver(this.driver);
		}
		catch (SQLException e)
		{
			assert false : e;;
		}
	}
	
	protected Database createDatabase(String id)
	{
		DriverDatabase database = new DriverDatabase();
		
		database.setId(id);
		
		return database;
	}
	
	private void setUrl(DriverDatabase database, String url, boolean accepted)
	{
		try
		{
			EasyMock.expect(this.driver.acceptsURL(url)).andReturn(accepted);
			
			this.control.replay();
			
			database.setUrl(url);

			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
		finally
		{
			this.control.reset();
		}
	}
	
	private void setUrl(DriverDatabase database, String url)
	{
		this.setUrl(database, url, true);
	}
	
	/**
	 * Test method for {@link DriverDatabase#connect(Driver)}
	 */
	public void testConnect()
	{
		Connection connection = EasyMock.createMock(Connection.class);

		DriverDatabase database = new DriverDatabase();
		String url = "jdbc:test";
		
		this.setUrl(database, url);
		
		try
		{
			EasyMock.expect(this.driver.connect(url, new Properties())).andReturn(connection);
			
			this.control.replay();
			
			Connection conn = database.connect(this.driver);
			
			this.control.verify();
			
			assert connection == conn;
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
		Connection connection = EasyMock.createMock(Connection.class);

		DriverDatabase database = new DriverDatabase();
		String url = "jdbc:test";
		
		this.setUrl(database, url);
		
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
		
		this.setUrl(database, url);
		
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
		DriverDatabase database = new DriverDatabase();
		String url = "jdbc:test";
		
		this.setUrl(database, url);
		
		try
		{
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

		try
		{
			this.setUrl(database, "bad", false);
			
			assert false;
		}
		catch (IllegalArgumentException e)
		{
			assert true;
		}
		
		assert !database.isDirty();
		
		this.setUrl(database, "good");
		
		assert database.isDirty();

		this.setUrl(database, "test");
		
		assert database.isDirty();

		database.clean();
		
		assert !database.isDirty();
		
		this.setUrl(database, "different");
		
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
		
		database.setDriver("");
		
		assert database.getDriver() == null : database.getDriver();
		assert !database.isDirty();

		database.setDriver("net.sf.hajdbc.sql.MockDriver");
		
		assert database.isDirty();

		database.setDriver("net.sf.hajdbc.sql.MockDriver");
		
		assert database.isDirty();

		database.clean();
		
		assert !database.isDirty();
		
		database.setDriver(null);
		
		assert database.isDirty();
		
		database.setDriver("net.sf.hajdbc.sql.MockDriver");
		
		assert database.isDirty();
		
		database.clean();
		
		assert !database.isDirty();
		
		database.setDriver("java.sql.Driver");
		
		assert database.isDirty();
		
		try
		{
			database.setDriver("java.lang.Class");
			
			assert false;
		}
		catch (IllegalArgumentException e)
		{
			assert true;
		}
		
		try
		{
			database.setDriver("not.a.valid.Class");
			
			assert false;
		}
		catch (IllegalArgumentException e)
		{
			assert true;
		}
	}
}
