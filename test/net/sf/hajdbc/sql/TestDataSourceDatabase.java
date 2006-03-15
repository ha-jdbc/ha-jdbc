/**
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
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

import net.sf.hajdbc.Database;

/**
 * Unit test for {@link DataSourceDatabase}.
 * @author  Paul Ferraro
 * @since   1.1
 */
@Test
public class TestDataSourceDatabase extends AbstractTestDatabase
{
	private DataSource dataSource = this.control.createMock(DataSource.class);

	/**
	 * Test method for {@link net.sf.hajdbc.sql.DataSourceDatabase.connect(Object)}
	 */
	public void testConnect()
	{
		DataSourceDatabase database = new DataSourceDatabase();
		
		Connection connection = EasyMock.createMock(Connection.class);

		try
		{
			EasyMock.expect(this.dataSource.getConnection()).andReturn(connection);
			
			this.control.replay();
			
			Connection conn = database.connect(this.dataSource);
			
			this.control.verify();
			
			assert connection == conn;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link net.sf.hajdbc.sql.DataSourceDatabase.connect(Object)}
	 */
	public void testConnectAsUser()
	{
		DataSourceDatabase database = new DataSourceDatabase();
		database.setUser("test-user");
		database.setPassword("test-password");
		
		Connection connection = EasyMock.createMock(Connection.class);

		try
		{
			EasyMock.expect(this.dataSource.getConnection("test-user", "test-password")).andReturn(connection);
			
			this.control.replay();
			
			Connection conn = database.connect(this.dataSource);
			
			this.control.verify();
			
			assert connection == conn;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link net.sf.hajdbc.sql.DataSourceDatabase.createConnectionFactory()}
	 */
	public void testCreateConnectionFactory()
	{
		try
		{
			Reference reference = new Reference(this.dataSource.getClass().getName(), DataSourceFactory.class.getName(), null);
			
			DataSourceDatabase database = new DataSourceDatabase();
			database.setName("test");
			database.setProperties(new Properties());
			database.getProperties().setProperty(Context.INITIAL_CONTEXT_FACTORY, MockInitialContextFactory.class.getName());
			
			Context context = new InitialContext(database.getProperties());
			
			context.rebind("test", reference);
			
			DataSource dataSource = database.createConnectionFactory();
			
			assert dataSource != null;
			assert DataSource.class.isInstance(dataSource);
			
			context.unbind("test");
		}
		catch (Exception e)
		{
			assert false : e;
		}
	}
	
	/**
	 * Test method for {@link net.sf.hajdbc.sql.DataSourceDatabase.createConnectionFactory()}
	 */
	public void testSetName()
	{
		DataSourceDatabase database = new DataSourceDatabase();
		
		assert !database.isDirty();
		
		database.setName(null);
		
		assert !database.isDirty();
		
		database.setName("test");
		
		assert database.isDirty();

		database.setName("test");
		
		assert database.isDirty();

		database.clean();
		
		assert !database.isDirty();
		
		database.setName(null);
		
		assert database.isDirty();
		
		database.setName("test");
		
		assert database.isDirty();
		
		database.clean();
		
		assert !database.isDirty();
		
		database.setName("different");
		
		assert database.isDirty();
	}
	
	/**
	 * Object factory that returns mock DataSource objects.
	 */
	public static class DataSourceFactory implements ObjectFactory
	{
		/**
		 * @see javax.naming.spi.ObjectFactory#getObjectInstance(java.lang.Object, javax.naming.Name, javax.naming.Context, java.util.Hashtable)
		 */
		public Object getObjectInstance(Object object, Name name, Context context, Hashtable environment) throws Exception
		{
			return EasyMock.createMock(DataSource.class);
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractTestDatabase#createDatabase(java.lang.String)
	 */
	@Override
	protected Database createDatabase(String id)
	{
		DataSourceDatabase database = new DataSourceDatabase();
		
		database.setId(id);
		
		return database;
	}
}
