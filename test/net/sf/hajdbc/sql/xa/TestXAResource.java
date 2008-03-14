/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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
package net.sf.hajdbc.sql.xa;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.MockDatabase;
import net.sf.hajdbc.sql.Invoker;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.util.reflect.ProxyFactory;

import org.easymock.EasyMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("unchecked")
public class TestXAResource implements XAResource
{
	private Balancer balancer = EasyMock.createStrictMock(Balancer.class);
	private DatabaseCluster cluster = EasyMock.createStrictMock(DatabaseCluster.class);
	private XAResource resource1 = EasyMock.createStrictMock(XAResource.class);
	private XAResource resource2 = EasyMock.createStrictMock(XAResource.class);
	private SQLProxy parent = EasyMock.createStrictMock(SQLProxy.class);
	private SQLProxy root = EasyMock.createStrictMock(SQLProxy.class);
	private LockManager lockManager = EasyMock.createStrictMock(LockManager.class);
	private Lock lock = EasyMock.createStrictMock(Lock.class);
	
	private Database database1 = new MockDatabase("1");
	private Database database2 = new MockDatabase("2");
	private Set<Database> databaseSet;
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	private XAResourceInvocationHandler handler;
	private XAResource resource;
	
	@BeforeClass
	protected void init() throws Exception
	{
		Map map = new TreeMap();
		map.put(this.database1, this.resource1);
		map.put(this.database2, this.resource2);
		
		this.databaseSet = map.keySet();
		
		EasyMock.expect(this.parent.getDatabaseCluster()).andReturn(this.cluster);

		this.parent.addChild(EasyMock.isA(XAResourceInvocationHandler.class));

		this.replay();
		
		this.handler = new XAResourceInvocationHandler(EasyMock.createStrictMock(XAConnection.class), this.parent, EasyMock.createMock(Invoker.class), map);
		this.resource = ProxyFactory.createProxy(XAResource.class, this.handler);
		
		this.verify();
		this.reset();
	}
	
	private Object[] objects()
	{
		return new Object[] { this.cluster, this.balancer, this.resource1, this.resource2, this.parent, this.root, this.lock, this.lockManager };
	}
	
	protected void replay()
	{
		EasyMock.replay(this.objects());
	}
	
	protected void verify()
	{
		EasyMock.verify(this.objects());
	}
	
	@AfterMethod
	protected void reset()
	{
		EasyMock.reset(this.objects());
	}
	
	@DataProvider(name = "xid-boolean")
	Object[][] xidBooleanProvider()
	{
		return new Object[][] { new Object[] { EasyMock.createMock(Xid.class), false } };
	}
	
	/**
	 * @see javax.transaction.xa.XAResource#commit(javax.transaction.xa.Xid, boolean)
	 */
	@Override
	@Test(dataProvider = "xid-boolean")
	public void commit(Xid xid, boolean onePhase) throws XAException
	{
		// Simulate start transaction
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		this.resource1.start(xid, XAResource.TMNOFLAGS);
		this.resource2.start(xid, XAResource.TMNOFLAGS);
		
		this.replay();
		
		this.resource.start(xid, XAResource.TMNOFLAGS);
		
		this.verify();
		this.reset();

		// Begin test
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		this.resource1.commit(xid, onePhase);
		this.resource2.commit(xid, onePhase);
		
		this.lock.unlock();
		
		this.replay();
		
		this.resource.commit(xid, onePhase);
		
		this.verify();
	}
	
	@DataProvider(name = "xid-flags")
	Object[][] xidIntProvider()
	{
		return new Object[][] { new Object[] { EasyMock.createMock(Xid.class), XAResource.TMNOFLAGS } };
	}

	/**
	 * @see javax.transaction.xa.XAResource#end(javax.transaction.xa.Xid, int)
	 */
	@Override
	@Test(dataProvider = "xid-flags")
	public void end(Xid xid, int flags) throws XAException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		this.resource1.end(xid, flags);
		this.resource2.end(xid, flags);
		
		this.replay();
		
		this.resource.end(xid, flags);
		
		this.verify();
	}

	@DataProvider(name = "xid")
	Object[][] xidProvider()
	{
		return new Object[][] { new Object[] { EasyMock.createMock(Xid.class) } };
	}
	
	/**
	 * @see javax.transaction.xa.XAResource#forget(javax.transaction.xa.Xid)
	 */
	@Override
	@Test(dataProvider = "xid")
	public void forget(Xid xid) throws XAException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		this.resource1.forget(xid);
		this.resource2.forget(xid);
		
		this.replay();
		
		this.resource.forget(xid);
		
		this.verify();
	}

	/**
	 * @see javax.transaction.xa.XAResource#getTransactionTimeout()
	 */
	@Override
	@Test
	public int getTransactionTimeout() throws XAException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.resource1.getTransactionTimeout()).andReturn(1);
		
		this.replay();
		
		int timeout = this.resource.getTransactionTimeout();
		
		this.verify();
		
		assert timeout == 1 : timeout;
		
		return timeout;
	}

	@DataProvider(name = "resource")
	Object[][] resourceProvider()
	{
		return new Object[][] { new Object[] { EasyMock.createMock(XAResource.class) } };
	}
	
	/**
	 * @see javax.transaction.xa.XAResource#isSameRM(javax.transaction.xa.XAResource)
	 */
	@Override
	@Test(dataProvider = "resource")
	public boolean isSameRM(XAResource resource) throws XAException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.resource1.isSameRM(resource)).andReturn(true);
		
		this.replay();
		
		boolean same = this.resource.isSameRM(resource);
		
		this.verify();

		assert same;
		
		return same;
	}

	/**
	 * @see javax.transaction.xa.XAResource#prepare(javax.transaction.xa.Xid)
	 */
	@Override
	@Test(dataProvider = "xid")
	public int prepare(Xid xid) throws XAException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);

		EasyMock.expect(this.resource1.prepare(xid)).andReturn(XAResource.XA_OK);
		EasyMock.expect(this.resource2.prepare(xid)).andReturn(XAResource.XA_OK);
		
		this.replay();
		
		int result = this.resource.prepare(xid);
		
		this.verify();
		
		assert result == XAResource.XA_OK :  result;
		
		return result;
	}

	@DataProvider(name = "flags")
	Object[][] flagsProvider()
	{
		return new Object[][] { new Object[] { XAResource.TMNOFLAGS } };
	}
	
	/**
	 * @see javax.transaction.xa.XAResource#recover(int)
	 */
	@Override
	@Test(dataProvider = "flags")
	public Xid[] recover(int flags) throws XAException
	{
		Xid[] xids = new Xid[0];
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);

		EasyMock.expect(this.resource1.recover(flags)).andReturn(xids);
		EasyMock.expect(this.resource2.recover(flags)).andReturn(xids);
		
		this.replay();
		
		Xid[] result = this.resource.recover(flags);
		
		this.verify();
		
		assert result == xids;
		
		return result;
	}

	/**
	 * @see javax.transaction.xa.XAResource#rollback(javax.transaction.xa.Xid)
	 */
	@Override
	@Test(dataProvider = "xid")
	public void rollback(Xid xid) throws XAException
	{
		// Simulate start transaction
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		this.resource1.start(xid, XAResource.TMNOFLAGS);
		this.resource2.start(xid, XAResource.TMNOFLAGS);
		
		this.replay();
		
		this.resource.start(xid, XAResource.TMNOFLAGS);
		
		this.verify();
		this.reset();
				
		// Start test
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		this.resource1.rollback(xid);
		this.resource2.rollback(xid);
		
		this.lock.unlock();
		
		this.replay();
		
		this.resource.rollback(xid);
		
		this.verify();
	}

	/**
	 * @see javax.transaction.xa.XAResource#setTransactionTimeout(int)
	 */
	@Override
	@Test(dataProvider = "flags")
	public boolean setTransactionTimeout(int timeout) throws XAException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.resource1.setTransactionTimeout(timeout)).andReturn(true);
		EasyMock.expect(this.resource2.setTransactionTimeout(timeout)).andReturn(true);
		
		this.replay();
		
		boolean result = this.resource.setTransactionTimeout(timeout);
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see javax.transaction.xa.XAResource#start(javax.transaction.xa.Xid, int)
	 */
	@Override
	@Test(dataProvider = "xid-flags")
	public void start(Xid xid, int flags) throws XAException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		this.resource1.start(xid, flags);
		this.resource2.start(xid, flags);
		
		this.replay();
		
		this.resource.start(xid, flags);
		
		this.verify();
		this.reset();
		
		// Test resume
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		this.resource1.start(xid, flags);
		this.resource2.start(xid, flags);
		
		this.replay();
		
		this.resource.start(xid, flags);
		
		this.verify();
		this.reset();
		
		// Simulate transaction rollback
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		this.resource1.rollback(xid);
		this.resource2.rollback(xid);
		
		this.lock.unlock();
		
		this.replay();
		
		this.resource.rollback(xid);
		
		this.verify();
	}
}
