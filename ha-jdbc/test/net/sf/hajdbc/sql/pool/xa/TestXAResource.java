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
package net.sf.hajdbc.sql.pool.xa;

import java.sql.SQLException;
import java.util.Collections;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.ConnectionFactory;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.EasyMockTestCase;
import net.sf.hajdbc.Operation;

import org.easymock.MockControl;

import edu.emory.mathcs.backport.java.util.concurrent.Executors;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestXAResource extends EasyMockTestCase
{
	private MockControl databaseClusterControl = this.createControl(DatabaseCluster.class);
	private DatabaseCluster databaseCluster = (DatabaseCluster) this.databaseClusterControl.getMock();
	
	private javax.sql.XAConnection connection = (javax.sql.XAConnection) this.createMock(javax.sql.XAConnection.class);
	
	private MockControl sqlResourceControl = this.createControl(javax.transaction.xa.XAResource.class);
	private javax.transaction.xa.XAResource sqlResource = (javax.transaction.xa.XAResource) this.sqlResourceControl.getMock();
	
	private MockControl databaseControl = this.createControl(Database.class);
	private Database database = (Database) this.databaseControl.getMock();
	
	private MockControl balancerControl = this.createControl(Balancer.class);
	private Balancer balancer = (Balancer) this.balancerControl.getMock();
	
	private XAResource resource;
	private Database[] databases = new Database[] { this.database };
	
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		this.databaseCluster.getExecutor();
		this.databaseClusterControl.setReturnValue(Executors.newSingleThreadExecutor(), 2);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 4);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 4);
		
		this.replay();

		ConnectionFactory connectionFactory = new ConnectionFactory(this.databaseCluster, Collections.singletonMap(this.database, new Object()));
		
		Operation operation = new Operation()
		{
			public Object execute(Database database, Object sqlObject) throws SQLException
			{
				return TestXAResource.this.connection;
			}
		};

		XAConnection connection = new XAConnection(connectionFactory, operation);
		
		XAConnectionOperation connectionOperation = new XAConnectionOperation()
		{			
			public Object execute(javax.sql.XAConnection connection) throws SQLException
			{
				return TestXAResource.this.sqlResource;
			}
		};
		
		this.resource = new XAResource(connection, connectionOperation);

		this.verify();
		this.reset();
	}
	
	public void testGetObject()
	{
		this.replay();
		
		Object resource = this.resource.getObject(this.database);
		
		this.verify();
		
		assertSame(this.sqlResource, resource);
	}

	public void testGetDatabaseCluster()
	{
		this.replay();
		
		DatabaseCluster databaseCluster = this.resource.getDatabaseCluster();
		
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
			
			this.resource.handleExceptions(Collections.singletonMap(this.database, new SQLException()));
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.xa.XAResource.getTransactionTimeout()'
	 */
	public void testGetTransactionTimeout()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.sqlResource.getTransactionTimeout();
			this.sqlResourceControl.setReturnValue(1);
			
			this.replay();
			
			int timeout = this.resource.getTransactionTimeout();
			
			this.verify();
			
			assertEquals(1, timeout);
		}
		catch (XAException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.xa.XAResource.setTransactionTimeout(int)'
	 */
	public void testSetTransactionTimeout()
	{
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResource.setTransactionTimeout(1);
			this.sqlResourceControl.setReturnValue(true);
			
			this.replay();
			
			boolean value = this.resource.setTransactionTimeout(1);
			
			this.verify();
			
			assertEquals(true, value);
		}
		catch (XAException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.xa.XAResource.isSameRM(XAResource)'
	 */
	public void testIsSameRM()
	{
		javax.transaction.xa.XAResource resource = (javax.transaction.xa.XAResource) this.createMock(javax.transaction.xa.XAResource.class);
		
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer);
			
			this.balancer.first();
			this.balancerControl.setReturnValue(this.database);
			
			this.sqlResource.isSameRM(resource);
			this.sqlResourceControl.setReturnValue(true);
			
			this.replay();
			
			boolean same = this.resource.isSameRM(resource);
			
			this.verify();
			
			assertEquals(true, same);
		}
		catch (XAException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.xa.XAResource.recover(int)'
	 */
	public void testRecover()
	{
		Xid id = (Xid) this.createMock(Xid.class);
		
		Xid[] ids = new Xid[] { id };
		
		try
		{
			this.databaseCluster.getExecutor();
			this.databaseClusterControl.setReturnValue(Executors.newSingleThreadExecutor());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResource.recover(1);
			this.sqlResourceControl.setReturnValue(ids);
			
			this.replay();
			
			Xid[] value = this.resource.recover(1);
			
			this.verify();
			
			assertSame(ids, value);
		}
		catch (XAException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.xa.XAResource.prepare(Xid)'
	 */
	public void testPrepare()
	{
		Xid id = (Xid) this.createMock(Xid.class);
		
		try
		{
			this.databaseCluster.getExecutor();
			this.databaseClusterControl.setReturnValue(Executors.newSingleThreadExecutor());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResource.prepare(id);
			this.sqlResourceControl.setReturnValue(1);
			
			this.replay();
			
			int value = this.resource.prepare(id);
			
			this.verify();
			
			assertEquals(1, value);
		}
		catch (XAException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.xa.XAResource.forget(Xid)'
	 */
	public void testForget()
	{
		Xid id = (Xid) this.createMock(Xid.class);
		
		try
		{
			this.databaseCluster.getExecutor();
			this.databaseClusterControl.setReturnValue(Executors.newSingleThreadExecutor());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResource.forget(id);
			this.sqlResourceControl.setVoidCallable();
			
			this.replay();
			
			this.resource.forget(id);
			
			this.verify();
		}
		catch (XAException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.xa.XAResource.rollback(Xid)'
	 */
	public void testRollback()
	{
		Xid id = (Xid) this.createMock(Xid.class);
		
		try
		{
			this.databaseCluster.getExecutor();
			this.databaseClusterControl.setReturnValue(Executors.newSingleThreadExecutor());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResource.rollback(id);
			this.sqlResourceControl.setVoidCallable();
			
			this.replay();
			
			this.resource.rollback(id);
			
			this.verify();
		}
		catch (XAException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.xa.XAResource.end(Xid, int)'
	 */
	public void testEnd()
	{
		Xid id = (Xid) this.createMock(Xid.class);
		
		try
		{
			this.databaseCluster.getExecutor();
			this.databaseClusterControl.setReturnValue(Executors.newSingleThreadExecutor());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResource.end(id, 1);
			this.sqlResourceControl.setVoidCallable();
			
			this.replay();
			
			this.resource.end(id, 1);
			
			this.verify();
		}
		catch (XAException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.xa.XAResource.start(Xid, int)'
	 */
	public void testStart()
	{
		Xid id = (Xid) this.createMock(Xid.class);
		
		try
		{
			this.databaseCluster.getExecutor();
			this.databaseClusterControl.setReturnValue(Executors.newSingleThreadExecutor());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResource.start(id, 1);
			this.sqlResourceControl.setVoidCallable();
			
			this.replay();
			
			this.resource.start(id, 1);
			
			this.verify();
		}
		catch (XAException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.pool.xa.XAResource.commit(Xid, boolean)'
	 */
	public void testCommit()
	{
		Xid id = (Xid) this.createMock(Xid.class);
		
		try
		{
			this.databaseCluster.getExecutor();
			this.databaseClusterControl.setReturnValue(Executors.newSingleThreadExecutor());
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlResource.commit(id, true);
			this.sqlResourceControl.setVoidCallable();
			
			this.replay();
			
			this.resource.commit(id, true);
			
			this.verify();
		}
		catch (XAException e)
		{
			this.fail(e);
		}
	}
}
