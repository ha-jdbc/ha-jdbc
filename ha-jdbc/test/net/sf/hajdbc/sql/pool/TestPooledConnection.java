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
package net.sf.hajdbc.sql.pool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;

import javax.sql.ConnectionEventListener;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.ConnectionFactory;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.EasyMockTestCase;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLObject;

import org.easymock.MockControl;

import edu.emory.mathcs.backport.java.util.concurrent.Executors;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestPooledConnection extends EasyMockTestCase
{
	protected MockControl databaseClusterControl = this.createControl(DatabaseCluster.class);
	protected DatabaseCluster databaseCluster = (DatabaseCluster) this.databaseClusterControl.getMock();
	
	protected MockControl sqlConnectionControl = this.createControl(this.getConnectionClass());
	private javax.sql.PooledConnection sqlConnection = (javax.sql.PooledConnection) this.sqlConnectionControl.getMock();
	
	protected MockControl databaseControl = this.createControl(Database.class);
	protected Database database = (Database) this.databaseControl.getMock();
	
	protected MockControl balancerControl = this.createControl(Balancer.class);
	protected Balancer balancer = (Balancer) this.balancerControl.getMock();
	
	protected PooledConnection connection;
	protected Database[] databases = new Database[] { this.database };
	
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		this.databaseCluster.getConnectionFactoryMap();
		this.databaseClusterControl.setReturnValue(Collections.singletonMap(this.database, this.sqlConnection));
		
		this.databaseCluster.getExecutor();
		this.databaseClusterControl.setReturnValue(Executors.newSingleThreadExecutor());
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		this.replay();
		
		this.connection = this.createConnection();

		this.verify();
		this.reset();
	}

	protected PooledConnection createConnection() throws SQLException
	{
		ConnectionFactory connectionFactory = new ConnectionFactory(this.databaseCluster);
		
		Operation operation = new Operation()
		{
			public Object execute(Database database, Object sqlObject) throws SQLException
			{
				return sqlObject;
			}
		};
		
		return new PooledConnection(connectionFactory, operation);
	}
	
	protected Class getConnectionClass()
	{
		return javax.sql.PooledConnection.class;
	}
	
	public void testGetObject()
	{
		this.replay();
		
		Object connection = this.connection.getObject(this.database);
		
		this.verify();
		
		assertSame(this.sqlConnection, connection);
	}

	public void testGetDatabaseCluster()
	{
		this.replay();
		
		DatabaseCluster databaseCluster = this.connection.getDatabaseCluster();
		
		this.verify();
		
		assertSame(this.databaseCluster, databaseCluster);
	}

	public void testHandleException()
	{
		try
		{
			this.databaseCluster.deactivate(this.database);
			this.databaseClusterControl.setReturnValue(false);
			
			this.replay();
			
			this.connection.handleExceptions(Collections.singletonMap(this.database, new SQLException()));
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}
	
	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.PooledConnection.getConnection()'
	 */
	public void testGetConnection()
	{
		Connection connection1 = (Connection) this.createMock(Connection.class);

		this.databaseCluster.getExecutor();
		this.databaseClusterControl.setReturnValue(Executors.newSingleThreadExecutor());
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		try
		{
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.getConnection();
			this.sqlConnectionControl.setReturnValue(connection1);
			
			this.replay();
			
			Connection connection = this.connection.getConnection();
			
			this.verify();
			
			assertNotNull(connection);
			assertTrue(SQLObject.class.isInstance(connection));
			assertSame(connection1, ((SQLObject) connection).getObject(this.database));
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.PooledConnection.close()'
	 */
	public void testClose()
	{
		this.databaseCluster.getExecutor();
		this.databaseClusterControl.setReturnValue(Executors.newSingleThreadExecutor());
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		try
		{
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.close();
			this.sqlConnectionControl.setVoidCallable();
			
			this.replay();
			
			this.connection.close();
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.PooledConnection.addConnectionEventListener(ConnectionEventListener)'
	 */
	public void testAddConnectionEventListener()
	{
		ConnectionEventListener listener = (ConnectionEventListener) this.createMock(ConnectionEventListener.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		this.sqlConnection.addConnectionEventListener(listener);
		this.sqlConnectionControl.setVoidCallable();
		
		this.replay();
		
		this.connection.addConnectionEventListener(listener);
		
		this.verify();
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.PooledConnection.removeConnectionEventListener(ConnectionEventListener)'
	 */
	public void testRemoveConnectionEventListener()
	{
		ConnectionEventListener listener = (ConnectionEventListener) this.createMock(ConnectionEventListener.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		this.sqlConnection.removeConnectionEventListener(listener);
		this.sqlConnectionControl.setVoidCallable();
		
		this.replay();
		
		this.connection.removeConnectionEventListener(listener);
		
		this.verify();
	}
}
