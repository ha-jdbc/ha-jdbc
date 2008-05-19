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
package net.sf.hajdbc.sql.pool;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;

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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@Test
@SuppressWarnings({ "unchecked", "nls" })
public class TestPooledConnection implements PooledConnection
{
	protected Balancer balancer = EasyMock.createStrictMock(Balancer.class);
	protected DatabaseCluster cluster = EasyMock.createStrictMock(DatabaseCluster.class);
	protected PooledConnection connection1 = EasyMock.createStrictMock(PooledConnection.class);
	protected PooledConnection connection2 = EasyMock.createStrictMock(PooledConnection.class);
	protected SQLProxy parent = EasyMock.createStrictMock(SQLProxy.class);
	private SQLProxy root = EasyMock.createStrictMock(SQLProxy.class);
	private LockManager lockManager = EasyMock.createStrictMock(LockManager.class);
	private Lock lock = EasyMock.createStrictMock(Lock.class);
	
	protected Database database1 = new MockDatabase("1");
	protected Database database2 = new MockDatabase("2");
	protected Set<Database> databaseSet;
	protected ExecutorService executor = Executors.newSingleThreadExecutor();
	private AbstractPooledConnectionInvocationHandler handler;
	protected PooledConnection connection;
	
	protected Class<? extends PooledConnection> getConnectionClass()
	{
		return PooledConnection.class;
	}
	
	protected AbstractPooledConnectionInvocationHandler getInvocationHandler(Map map) throws Exception
	{
		return new PooledConnectionInvocationHandler(EasyMock.createStrictMock(ConnectionPoolDataSource.class), this.parent, EasyMock.createMock(Invoker.class), map);
	}
	
	@BeforeClass
	protected void init() throws Exception
	{
		Map map = new TreeMap();
		map.put(this.database1, this.connection1);
		map.put(this.database2, this.connection2);
		
		this.databaseSet = map.keySet();
		
		EasyMock.expect(this.parent.getDatabaseCluster()).andReturn(this.cluster);

		this.parent.addChild(EasyMock.isA(AbstractPooledConnectionInvocationHandler.class));

		this.replay();
		
		this.handler = this.getInvocationHandler(map);
		this.connection = ProxyFactory.createProxy(this.getConnectionClass(), this.handler);
		
		this.verify();
		this.reset();
	}
	
	private Object[] objects()
	{
		return new Object[] { this.cluster, this.balancer, this.connection1, this.connection2, this.parent, this.root, this.lock, this.lockManager };
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
	
	@DataProvider(name = "connection-listener")
	protected Object[][] connectionListenerParameters()
	{
		return new Object[][] { new Object[] { EasyMock.createMock(ConnectionEventListener.class) } };
	}
	
	/**
	 * @see javax.sql.PooledConnection#addConnectionEventListener(javax.sql.ConnectionEventListener)
	 */
	@Override
	@Test(dataProvider = "connection-listener")
	public void addConnectionEventListener(ConnectionEventListener listener)
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.connection1.addConnectionEventListener(listener);
		this.connection2.addConnectionEventListener(listener);

		this.replay();
		
		this.connection.addConnectionEventListener(listener);
		
		this.verify();
	}

	@DataProvider(name = "statement-listener")
	protected Object[][] statementListenerParameters()
	{
		return new Object[][] { new Object[] { EasyMock.createMock(StatementEventListener.class) } };
	}
	
	/**
	 * @see javax.sql.PooledConnection#addStatementEventListener(javax.sql.StatementEventListener)
	 */
	@Override
	@Test(dataProvider = "statement-listener")
	public void addStatementEventListener(StatementEventListener listener)
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.connection1.addStatementEventListener(listener);
		this.connection2.addStatementEventListener(listener);
		
		this.replay();
		
		this.connection.addStatementEventListener(listener);
		
		this.verify();
	}

	/**
	 * @see javax.sql.PooledConnection#close()
	 */
	@Override
	public void close() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		this.connection1.close();
		this.connection2.close();

		this.parent.removeChild(this.handler);
		
		this.replay();
		
		this.connection.close();
		
		this.verify();
	}

	public void testGetConnection() throws SQLException
	{
		Connection connection1 = EasyMock.createStrictMock(Connection.class);
		Connection connection2 = EasyMock.createStrictMock(Connection.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.connection1.getConnection()).andReturn(connection1);
		EasyMock.expect(this.connection2.getConnection()).andReturn(connection2);
		
		this.replay();
		
		Connection result = this.getConnection();
		
		this.verify();
		
		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == connection1;
		assert proxy.getObject(this.database2) == connection2;
	}
	
	/**
	 * @see javax.sql.PooledConnection#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException
	{
		return this.connection.getConnection();
	}

	/**
	 * @see javax.sql.PooledConnection#removeConnectionEventListener(javax.sql.ConnectionEventListener)
	 */
	@Override
	@Test(dataProvider = "connection-listener")
	public void removeConnectionEventListener(ConnectionEventListener listener)
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.connection1.removeConnectionEventListener(listener);
		this.connection2.removeConnectionEventListener(listener);
		
		this.replay();
		
		this.connection.removeConnectionEventListener(listener);
		
		this.verify();
	}

	/**
	 * @see javax.sql.PooledConnection#removeStatementEventListener(javax.sql.StatementEventListener)
	 */
	@Override
	@Test(dataProvider = "statement-listener")
	public void removeStatementEventListener(StatementEventListener listener)
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.connection1.removeStatementEventListener(listener);
		this.connection2.removeStatementEventListener(listener);
		
		this.replay();
		
		this.connection.removeStatementEventListener(listener);
		
		this.verify();
	}
}
