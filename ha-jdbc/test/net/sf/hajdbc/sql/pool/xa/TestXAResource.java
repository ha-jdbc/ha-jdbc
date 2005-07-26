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
import java.util.HashMap;
import java.util.Map;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.ConnectionFactory;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.EasyMockTestCase;
import net.sf.hajdbc.Operation;

import org.easymock.MockControl;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestXAResource extends EasyMockTestCase
{
	private MockControl databaseClusterControl = this.createControl(DatabaseCluster.class);
	private DatabaseCluster databaseCluster = (DatabaseCluster) this.databaseClusterControl.getMock();
	
	private MockControl resource1Control = this.createControl(javax.transaction.xa.XAResource.class);
	private javax.transaction.xa.XAResource resource1 = (javax.transaction.xa.XAResource) this.resource1Control.getMock();
	
	private MockControl resource2Control = this.createControl(javax.transaction.xa.XAResource.class);
	private javax.transaction.xa.XAResource resource2 = (javax.transaction.xa.XAResource) this.resource2Control.getMock();
	
	private MockControl database1Control = this.createControl(Database.class);
	private Database database1 = (Database) this.database1Control.getMock();
	
	private MockControl database2Control = this.createControl(Database.class);
	private Database database2 = (Database) this.database2Control.getMock();
	
	private MockControl balancerControl = this.createControl(Balancer.class);
	private Balancer balancer = (Balancer) this.balancerControl.getMock();
	
	private XAResource resource;
	private Database[] databases = new Database[] { this.database1, this.database2 };
	
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 4);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 4);
		
		this.replay();

		Map connectionMap = new HashMap(2);
		connectionMap.put(this.database1, new MockXAConnection(this.resource1));
		connectionMap.put(this.database2, new MockXAConnection(this.resource2));
		
		ConnectionFactory connectionFactory = new ConnectionFactory(this.databaseCluster, connectionMap);
		
		Operation operation = new Operation()
		{
			public Object execute(Database database, Object sqlObject) throws SQLException
			{
				return sqlObject;
			}
		};

		XAConnection connection = new XAConnection(connectionFactory, operation);
		
		XAConnectionOperation connectionOperation = new XAConnectionOperation()
		{			
			public Object execute(javax.sql.XAConnection connection) throws SQLException
			{
				return connection.getXAResource();
			}
		};
		
		this.resource = new XAResource(connection, connectionOperation);

		this.verify();
		this.reset();
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
			this.balancerControl.setReturnValue(this.database1);
			
			this.resource1.getTransactionTimeout();
			this.resource1Control.setReturnValue(1);
			
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
			
			this.resource1.setTransactionTimeout(1);
			this.resource1Control.setReturnValue(true);
			
			this.resource2.setTransactionTimeout(1);
			this.resource2Control.setReturnValue(true);
			
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
			this.balancerControl.setReturnValue(this.database1);
			
			this.resource1.isSameRM(resource);
			this.resource1Control.setReturnValue(true);
			
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
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.resource1.recover(1);
			this.resource1Control.setReturnValue(ids);
			
			this.resource2.recover(1);
			this.resource2Control.setReturnValue(ids);
			
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
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.resource1.prepare(id);
			this.resource1Control.setReturnValue(1);
			
			this.resource2.prepare(id);
			this.resource2Control.setReturnValue(1);
			
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
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.resource1.forget(id);
			this.resource1Control.setVoidCallable();
			
			this.resource2.forget(id);
			this.resource2Control.setVoidCallable();
			
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
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.resource1.rollback(id);
			this.resource1Control.setVoidCallable();
			
			this.resource2.rollback(id);
			this.resource2Control.setVoidCallable();
			
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
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.resource1.end(id, 1);
			this.resource1Control.setVoidCallable();
			
			this.resource2.end(id, 1);
			this.resource2Control.setVoidCallable();
			
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
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.resource1.start(id, 1);
			this.resource1Control.setVoidCallable();
			
			this.resource2.start(id, 1);
			this.resource2Control.setVoidCallable();
			
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
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.resource1.commit(id, true);
			this.resource1Control.setVoidCallable();
			
			this.resource2.commit(id, true);
			this.resource2Control.setVoidCallable();
			
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
