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
package net.sf.hajdbc.sql.pool.xa;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.EasyMockTestCase;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLObject;
import net.sf.hajdbc.sql.ConnectionFactory;

import org.easymock.EasyMock;

import java.util.concurrent.Executors;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestXAResource extends EasyMockTestCase
{
	private DatabaseCluster databaseCluster = this.control.createMock(DatabaseCluster.class);
	
	private javax.sql.XAConnection connection = this.control.createMock(javax.sql.XAConnection.class);
	
	private javax.transaction.xa.XAResource sqlResource = this.control.createMock(javax.transaction.xa.XAResource.class);
	
	private Database database = this.control.createMock(Database.class);
	
	private Balancer balancer = this.control.createMock(Balancer.class);
	
	private XAResource resource;
	private List<Database> databaseList = Collections.singletonList(this.database);
	
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		Map map = Collections.singletonMap(this.database, new Object());
		EasyMock.expect(this.databaseCluster.getConnectionFactoryMap()).andReturn(map);
		
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor()).times(2);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer).times(4);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList).times(4);
		
		this.control.replay();

		ConnectionFactory connectionFactory = new ConnectionFactory(this.databaseCluster, Object.class);
		
		Operation operation = new Operation()
		{
			public Object execute(Database database, Object sqlObject) throws SQLException
			{
				return TestXAResource.this.connection;
			}
		};

		XAConnection connection = new XAConnection(connectionFactory, operation);
		
		Operation connectionOperation = new Operation()
		{
			public Object execute(Database database, Object sqlObject) throws SQLException
			{
				return TestXAResource.this.sqlResource;
			}
		};
		
		this.resource = new XAResource(connection, connectionOperation);

		this.control.verify();
		this.control.reset();
	}
	
	/**
	 * Test method for {@link SQLObject#getObject(Database)}
	 */
	public void testGetObject()
	{
		this.control.replay();
		
		Object resource = this.resource.getObject(this.database);
		
		this.control.verify();
		
		assertSame(this.sqlResource, resource);
	}

	/**
	 * Test method for {@link SQLObject#getDatabaseCluster()}
	 */
	public void testGetDatabaseCluster()
	{
		this.control.replay();
		
		DatabaseCluster databaseCluster = this.resource.getDatabaseCluster();
		
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
			
			this.resource.handleExceptions(Collections.singletonMap(this.database, new SQLException()));
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link XAResource#getTransactionTimeout()}
	 */
	public void testGetTransactionTimeout()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResource.getTransactionTimeout()).andReturn(1);
			
			this.control.replay();
			
			int timeout = this.resource.getTransactionTimeout();
			
			this.control.verify();
			
			assertEquals(1, timeout);
		}
		catch (XAException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link XAResource#setTransactionTimeout(int)}
	 */
	public void testSetTransactionTimeout()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlResource.setTransactionTimeout(1)).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			boolean value = this.resource.setTransactionTimeout(1);
			
			this.control.verify();
			
			assertEquals(true, value);
		}
		catch (XAException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link XAResource#isSameRM(XAResource)}
	 */
	public void testIsSameRM()
	{
		javax.transaction.xa.XAResource resource = EasyMock.createMock(javax.transaction.xa.XAResource.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResource.isSameRM(resource)).andReturn(true);
			
			this.control.replay();
			
			boolean same = this.resource.isSameRM(resource);
			
			this.control.verify();
			
			assertEquals(true, same);
		}
		catch (XAException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link XAResource#recover(int)}
	 */
	public void testRecover()
	{
		Xid id = EasyMock.createMock(Xid.class);
		
		Xid[] ids = new Xid[] { id };
		
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlResource.recover(1)).andReturn(ids);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			Xid[] value = this.resource.recover(1);
			
			this.control.verify();
			
			assertSame(ids, value);
		}
		catch (XAException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link XAResource#prepare(Xid)}
	 */
	public void testPrepare()
	{
		Xid id = EasyMock.createMock(Xid.class);
		
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
			
			this.control.replay();
			
			int value = this.resource.prepare(id);
			
			this.control.verify();
			
			assertEquals(1, value);
		}
		catch (XAException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link XAResource#forget(Xid)}
	 */
	public void testForget()
	{
		Xid id = EasyMock.createMock(Xid.class);
		
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
			
			this.control.replay();
			
			this.resource.forget(id);
			
			this.control.verify();
		}
		catch (XAException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link XAResource#rollback(Xid)}
	 */
	public void testRollback()
	{
		Xid id = EasyMock.createMock(Xid.class);
		
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
			
			this.control.replay();
			
			this.resource.rollback(id);
			
			this.control.verify();
		}
		catch (XAException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link XAResource#end(Xid, int)}
	 */
	public void testEnd()
	{
		Xid id = EasyMock.createMock(Xid.class);
		
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
			
			this.control.replay();
			
			this.resource.end(id, 1);
			
			this.control.verify();
		}
		catch (XAException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link XAResource#start(Xid, int)}
	 */
	public void testStart()
	{
		Xid id = EasyMock.createMock(Xid.class);
		
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
			
			this.control.replay();
			
			this.resource.start(id, 1);
			
			this.control.verify();
		}
		catch (XAException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link XAResource#commit(Xid, boolean)}
	 */
	public void testCommit()
	{
		Xid id = EasyMock.createMock(Xid.class);
		
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
			
			this.control.replay();
			
			this.resource.commit(id, true);
			
			this.control.verify();
		}
		catch (XAException e)
		{
			fail(e);
		}
	}
}
