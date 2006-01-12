/**
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

import net.sf.hajdbc.EasyMockTestCase;

/**
 * Unit test for {@link DataSourceDatabase}.
 * @author  Paul Ferraro
 * @since   1.1
 */
public class TestDataSourceDatabase extends EasyMockTestCase
{
	private DataSource dataSource = this.control.createMock(DataSource.class);

	/**
	 * Test method for 'net.sf.hajdbc.sql.DataSourceDatabase.connect(Object)}
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
			
			assertSame(connection, conn);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for 'net.sf.hajdbc.sql.DataSourceDatabase.connect(Object)}
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
			
			assertSame(connection, conn);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for 'net.sf.hajdbc.sql.DataSourceDatabase.createConnectionFactory()}
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
			
			Object connectionFactory = database.createConnectionFactory();
			
			assertNotNull(connectionFactory);
			assertTrue(DataSource.class.isInstance(connectionFactory));
			
			context.unbind("test");
		}
		catch (Exception e)
		{
			fail(e);
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
	 * Test method for {@link AbstractDatabase#hashCode()}
	 */
	public void testHashCode()
	{
		DriverDatabase database = new DriverDatabase();
		database.setId("test");
		
		int hashCode = database.hashCode();
		
		assertEquals("test".hashCode(), hashCode);
	}
	
	private static class DataSourceFactory implements ObjectFactory
	{
		/**
		 * @see javax.naming.spi.ObjectFactory#getObjectInstance(java.lang.Object, javax.naming.Name, javax.naming.Context, java.util.Hashtable)
		 */
		public Object getObjectInstance(Object object, Name name, Context context, Hashtable environment) throws Exception
		{
			return EasyMock.createMock(DataSource.class);
		}
	}
}
