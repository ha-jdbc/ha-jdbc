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
package net.sf.hajdbc.sql.pool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.sql.ConnectionEventListener;

import org.easymock.EasyMock;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.EasyMockTestCase;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLObject;
import net.sf.hajdbc.sql.ConnectionFactory;

import java.util.concurrent.Executors;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestPooledConnection extends EasyMockTestCase
{
	protected DatabaseCluster databaseCluster = this.control.createMock(DatabaseCluster.class);
	
	protected javax.sql.PooledConnection sqlConnection = this.control.createMock(this.getConnectionClass());
	
	protected Database database = this.control.createMock(Database.class);
	
	protected Balancer balancer = this.control.createMock(Balancer.class);
	
	protected PooledConnection connection;
	protected List<Database> databaseList = Collections.singletonList(this.database);
	
	protected Class<? extends javax.sql.PooledConnection> getConnectionClass()
	{
		return javax.sql.PooledConnection.class;
	}
	
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		Map map = Collections.singletonMap(this.database, this.sqlConnection);
		
		EasyMock.expect(this.databaseCluster.getConnectionFactoryMap()).andReturn(map);
		
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer).times(2);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList).times(2);
		
		this.control.replay();
		
		this.connection = this.createConnection();

		this.control.verify();
		this.control.reset();
	}
	
	protected PooledConnection createConnection() throws SQLException
	{
		ConnectionFactory<javax.sql.PooledConnection> connectionFactory = new ConnectionFactory<javax.sql.PooledConnection>(this.databaseCluster, javax.sql.PooledConnection.class);
		
		Operation<javax.sql.PooledConnection, javax.sql.PooledConnection> operation = new Operation<javax.sql.PooledConnection, javax.sql.PooledConnection>()
		{
			public javax.sql.PooledConnection execute(Database database, javax.sql.PooledConnection connection) throws SQLException
			{
				return connection;
			}
		};
		
		return new PooledConnection(connectionFactory, operation);
	}
	
	/**
	 * Test method for {@link SQLObject#getObject(Database)}
	 */
	public void testGetObject()
	{
		this.control.replay();
		
		Object connection = this.connection.getObject(this.database);
		
		this.control.verify();
		
		assertSame(this.sqlConnection, connection);
	}

	/**
	 * Test method for {@link SQLObject#getDatabaseCluster()}
	 */
	public void testGetDatabaseCluster()
	{
		this.control.replay();
		
		DatabaseCluster databaseCluster = this.connection.getDatabaseCluster();
		
		this.control.verify();
		
		assertSame(this.databaseCluster, databaseCluster);
	}

	/**
	 * Test method for {@link SQLObject#handleExceptions(Map)}
	 */
	public void testHandleException()
	{
		try
		{
			EasyMock.expect(this.databaseCluster.deactivate(this.database)).andReturn(false);
			
			this.control.replay();
			
			this.connection.handleExceptions(Collections.singletonMap(this.database, new SQLException()));
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}
	
	/**
	 * Test method for {@link PooledConnection#getConnection()}
	 */
	public void testGetConnection()
	{
		Connection sqlConnection = EasyMock.createMock(Connection.class);

		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlConnection.getConnection()).andReturn(sqlConnection);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			Connection connection = this.connection.getConnection();
			
			this.control.verify();
			
			assertNotNull(connection);
			assertTrue(SQLObject.class.isInstance(connection));
			assertSame(sqlConnection, SQLObject.class.cast(connection).getObject(this.database));
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link PooledConnection#close()}
	 */
	public void testClose()
	{
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.close();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			this.connection.close();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link PooledConnection#addConnectionEventListener(ConnectionEventListener)}
	 */
	public void testAddConnectionEventListener()
	{
		ConnectionEventListener listener = EasyMock.createMock(ConnectionEventListener.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlConnection.addConnectionEventListener(listener);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.control.replay();
		
		this.connection.addConnectionEventListener(listener);
		
		this.control.verify();
	}

	/**
	 * Test method for {@link PooledConnection#removeConnectionEventListener(ConnectionEventListener)}
	 */
	public void testRemoveConnectionEventListener()
	{
		ConnectionEventListener listener = EasyMock.createMock(ConnectionEventListener.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlConnection.removeConnectionEventListener(listener);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.control.replay();
		
		this.connection.removeConnectionEventListener(listener);
		
		this.control.verify();
	}
}
