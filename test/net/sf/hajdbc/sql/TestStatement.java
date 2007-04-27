/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.MockDatabase;
import net.sf.hajdbc.TableProperties;
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
@SuppressWarnings("unchecked")
public class TestStatement implements java.sql.Statement
{
	protected Balancer balancer = EasyMock.createStrictMock(Balancer.class);
	protected DatabaseCluster cluster = EasyMock.createStrictMock(DatabaseCluster.class);
	protected FileSupport fileSupport = EasyMock.createStrictMock(FileSupport.class);
	protected Lock readLock = EasyMock.createStrictMock(Lock.class);
	protected Lock sequenceLock = EasyMock.createStrictMock(Lock.class);
	protected Lock tableLock = EasyMock.createStrictMock(Lock.class);
	protected LockManager lockManager = EasyMock.createStrictMock(LockManager.class);
	protected Dialect dialect = EasyMock.createStrictMock(Dialect.class);
	protected DatabaseMetaDataCache metaData = EasyMock.createStrictMock(DatabaseMetaDataCache.class);
	protected DatabaseProperties databaseProperties = EasyMock.createStrictMock(DatabaseProperties.class);
	protected TableProperties tableProperties = EasyMock.createStrictMock(TableProperties.class);
	protected ColumnProperties columnProperties = EasyMock.createStrictMock(ColumnProperties.class);
	protected Connection connection = EasyMock.createStrictMock(Connection.class);
	protected Statement statement1 = EasyMock.createStrictMock(this.getStatementClass());
	protected Statement statement2 = EasyMock.createStrictMock(this.getStatementClass());
	protected SQLProxy parent = EasyMock.createStrictMock(SQLProxy.class);
	protected SQLProxy root = EasyMock.createStrictMock(SQLProxy.class);
	
	protected Database database1 = new MockDatabase("1");
	protected Database database2 = new MockDatabase("2");
	protected Set<Database> databaseSet;
	protected ExecutorService executor = Executors.newSingleThreadExecutor();
	protected Statement statement;
	protected AbstractStatementInvocationHandler handler;
	
	protected Class<? extends java.sql.Statement> getStatementClass()
	{
		return java.sql.Statement.class;
	}
	
	protected AbstractStatementInvocationHandler getInvocationHandler(Map map) throws Exception
	{
		return new StatementInvocationHandler(this.connection, this.parent, EasyMock.createMock(Invoker.class), map, this.fileSupport);
	}
	
	@BeforeClass
	void init() throws Exception
	{
		Map<Database, Statement> map = new TreeMap<Database, Statement>();
		map.put(this.database1, this.statement1);
		map.put(this.database2, this.statement2);
		
		this.databaseSet = map.keySet();
		
		EasyMock.expect(this.parent.getDatabaseCluster()).andReturn(this.cluster);

		this.recordConstructor();
		
		this.replay();

		this.handler = this.getInvocationHandler(map);
		this.statement = ProxyFactory.createProxy(this.getStatementClass(), this.handler);

		this.verify();
		this.reset();
	}
	
	@SuppressWarnings("unused")
	protected void recordConstructor() throws SQLException
	{
		this.parent.addChild(EasyMock.isA(StatementInvocationHandler.class));
	}
	
	private Object[] objects()
	{
		return new Object[] { this.cluster, this.balancer, this.connection, this.statement1, this.statement2, this.fileSupport, this.readLock, this.sequenceLock, this.tableLock, this.lockManager, this.parent, this.root, this.dialect, this.metaData, this.databaseProperties, this.tableProperties, this.columnProperties };
	}
	
	void replay()
	{
		EasyMock.replay(this.objects());
	}
	
	void verify()
	{
		EasyMock.verify(this.objects());
	}
	
	@AfterMethod
	void reset()
	{
		EasyMock.reset(this.objects());
	}

	@DataProvider(name = "string")
	Object[][] stringProvider()
	{
		return new Object[][] { new Object[] { "sql" } };
	}
	
	/**
	 * @see java.sql.Statement#addBatch(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void addBatch(String sql) throws SQLException
	{
		this.statement1.addBatch(sql);
		this.statement2.addBatch(sql);
		
		this.replay();
		
		this.statement.addBatch(sql);
		
		this.verify();
	}

	/**
	 * @see java.sql.Statement#cancel()
	 */
	@Test
	public void cancel() throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		this.statement1.cancel();
		this.statement2.cancel();
		
		this.replay();
		
		this.statement.cancel();
		
		this.verify();
	}

	/**
	 * @see java.sql.Statement#clearBatch()
	 */
	@Test
	public void clearBatch() throws SQLException
	{
		this.statement1.clearBatch();
		this.statement2.clearBatch();
		
		this.replay();
		
		this.statement.clearBatch();
		
		this.verify();
	}

	/**
	 * @see java.sql.Statement#clearWarnings()
	 */
	@Test
	public void clearWarnings() throws SQLException
	{
		this.statement1.clearWarnings();
		this.statement2.clearWarnings();
		
		this.replay();
		
		this.statement.clearWarnings();
		
		this.verify();
	}

	/**
	 * @see java.sql.Statement#close()
	 */
	@Test
	public void close() throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		this.statement1.close();
		this.statement2.close();

		this.parent.removeChild(this.handler);
		
		this.replay();
		
		this.statement.close();
		
		this.verify();
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public boolean execute(String sql) throws SQLException
	{
		this.expectLocks(sql, null, null);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.execute(sql)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql)).andReturn(true);

		this.readLock.unlock();
		
		this.replay();
		
		boolean result = this.statement.execute(sql);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", null);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.execute(sql)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql)).andReturn(true);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.statement.execute(sql);
		
		this.verify();
		
		assert result;

		this.reset();
		
		this.expectLocks(sql, null, "table");
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.execute(sql)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql)).andReturn(true);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.execute(sql);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", "table");
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.execute(sql)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql)).andReturn(true);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.execute(sql);
		
		this.verify();
		
		assert result;
		
		return result;
	}

	@DataProvider(name = "string-int")
	Object[][] stringIntProvider()
	{
		return new Object[][] { new Object[] { "sql", java.sql.Statement.NO_GENERATED_KEYS } };
	}
	
	/**
	 * @see java.sql.Statement#execute(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
	{
		this.expectLocks(sql, null, null);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.execute(sql, autoGeneratedKeys)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, autoGeneratedKeys)).andReturn(true);
		
		this.readLock.unlock();
		
		this.replay();
		
		boolean result = this.statement.execute(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", null);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.execute(sql, autoGeneratedKeys)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, autoGeneratedKeys)).andReturn(true);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.statement.execute(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result;
		
		this.reset();
		this.reset();
		
		this.expectLocks(sql, null, "table");
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.execute(sql, autoGeneratedKeys)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, autoGeneratedKeys)).andReturn(true);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.execute(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", "table");
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.execute(sql, autoGeneratedKeys)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, autoGeneratedKeys)).andReturn(true);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.execute(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result;
		
		return result;
	}

	@DataProvider(name = "string-ints")
	Object[][] stringIntsProvider()
	{
		return new Object[][] { new Object[] { "sql", new int[] { 1 } } };
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String, int[])
	 */
	@Test(dataProvider = "string-ints")
	public boolean execute(String sql, int[] columnIndexes) throws SQLException
	{
		this.expectLocks(sql, null, null);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.execute(sql, columnIndexes)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnIndexes)).andReturn(true);
		
		this.readLock.unlock();
		
		this.replay();
		
		boolean result = this.statement.execute(sql, columnIndexes);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", null);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.execute(sql, columnIndexes)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnIndexes)).andReturn(true);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.statement.execute(sql, columnIndexes);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		this.expectLocks(sql, null, "table");
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.execute(sql, columnIndexes)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnIndexes)).andReturn(true);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.execute(sql, columnIndexes);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", "table");
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.execute(sql, columnIndexes)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnIndexes)).andReturn(true);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.execute(sql, columnIndexes);
		
		this.verify();
		
		assert result;
		
		return result;
	}

	@DataProvider(name = "string-strings")
	Object[][] stringStringsProvider()
	{
		return new Object[][] { new Object[] { "sql", new String[] { "name" } } };
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
	 */
	@Test(dataProvider = "string-strings")
	public boolean execute(String sql, String[] columnNames) throws SQLException
	{
		this.expectLocks(sql, null, null);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.execute(sql, columnNames)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnNames)).andReturn(true);
		
		this.readLock.unlock();
		
		this.replay();
		
		boolean result = this.statement.execute(sql, columnNames);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", null);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.execute(sql, columnNames)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnNames)).andReturn(true);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.statement.execute(sql, columnNames);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		this.expectLocks(sql, null, "table");
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.execute(sql, columnNames)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnNames)).andReturn(true);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.execute(sql, columnNames);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", "table");
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.execute(sql, columnNames)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnNames)).andReturn(true);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.execute(sql, columnNames);
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#executeBatch()
	 */
	@Test
	public int[] executeBatch() throws SQLException
	{
		int[] array = new int[0];
		
		this.addBatch("sql");
		
		this.reset();
		
		this.expectLocks("sql", null, null);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);

		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.executeBatch()).andReturn(array);
		EasyMock.expect(this.statement2.executeBatch()).andReturn(array);
		
		this.readLock.unlock();
		
		this.replay();
		
		int[] result = this.statement.executeBatch();
		
		this.verify();
		
		assert result == array;
		
		this.reset();
		
		this.expectLocks("sql", "sequence", null);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.executeBatch()).andReturn(array);
		EasyMock.expect(this.statement2.executeBatch()).andReturn(array);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.statement.executeBatch();
		
		this.verify();
		
		assert result == array;
		
		this.reset();
		
		this.expectLocks("sql", null, "table");
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.executeBatch()).andReturn(array);
		EasyMock.expect(this.statement2.executeBatch()).andReturn(array);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.executeBatch();
		
		this.verify();
		
		assert result == array;
		
		this.reset();
		
		this.expectLocks("sql", "sequence", "table");
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.executeBatch()).andReturn(array);
		EasyMock.expect(this.statement2.executeBatch()).andReturn(array);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.executeBatch();
		
		this.verify();
		
		assert result == array;
		
		this.reset();
		
		this.clearBatch();
		
		return result;
	}

	/**
	 * @see java.sql.Statement#executeQuery(java.lang.String)
	 */
	@SuppressWarnings("unchecked")
	@Test(dataProvider = "string")
	public ResultSet executeQuery(String sql) throws SQLException
	{
		ResultSet resultSet1 = EasyMock.createMock(ResultSet.class);
		ResultSet resultSet2 = EasyMock.createMock(ResultSet.class);
		
		this.expectLocks(sql, null, null);
		
		// Read-only result set
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		this.expectSelectForUpdateCheck(sql, false);
		
		// Locators update directly		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);

		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.statement2.executeQuery(sql)).andReturn(resultSet2);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		ResultSet results = this.statement.executeQuery(sql);
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;
		
		this.reset();
		
		this.expectLocks(sql, null, null);
		
		// Updatable result set
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_UPDATABLE);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.executeQuery(sql)).andReturn(resultSet1);
		EasyMock.expect(this.statement2.executeQuery(sql)).andReturn(resultSet2);

		this.readLock.unlock();
		
		this.replay();
		
		results = this.statement.executeQuery(sql);
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database1) == resultSet1;
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;
		
		this.reset();
		
		this.expectLocks(sql, null, null);
		
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		// Select-for-update
		this.expectSelectForUpdateCheck(sql, true);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.executeQuery(sql)).andReturn(resultSet1);
		EasyMock.expect(this.statement2.executeQuery(sql)).andReturn(resultSet2);

		this.readLock.unlock();
		
		this.replay();
		
		results = this.statement.executeQuery(sql);
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database1) == resultSet1;
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", null);

		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.executeQuery(sql)).andReturn(resultSet1);
		EasyMock.expect(this.statement2.executeQuery(sql)).andReturn(resultSet2);

		this.sequenceLock.unlock();
		
		this.replay();
		
		results = this.statement.executeQuery(sql);
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database1) == resultSet1;
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;
		
		this.reset();
		
		this.expectLocks(sql, null, "table");
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.executeQuery(sql)).andReturn(resultSet1);
		EasyMock.expect(this.statement2.executeQuery(sql)).andReturn(resultSet2);

		this.tableLock.unlock();
		
		this.replay();
		
		results = this.statement.executeQuery(sql);
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database1) == resultSet1;
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", "table");
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.executeQuery(sql)).andReturn(resultSet1);
		EasyMock.expect(this.statement2.executeQuery(sql)).andReturn(resultSet2);

		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		results = this.statement.executeQuery(sql);
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database1) == resultSet1;
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;
		
		return results;
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public int executeUpdate(String sql) throws SQLException
	{
		this.expectLocks(sql, null, null);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql)).andReturn(1);
		
		this.readLock.unlock();
		
		this.replay();
		
		int result = this.statement.executeUpdate(sql);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", null);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql)).andReturn(1);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.statement.executeUpdate(sql);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		this.expectLocks(sql, null, "table");
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql)).andReturn(1);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.executeUpdate(sql);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", "table");
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql)).andReturn(1);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.executeUpdate(sql);
		
		this.verify();
		
		assert result == 1 : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
	{
		this.expectLocks(sql, null, null);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		
		this.readLock.unlock();
		
		this.replay();
		
		int result = this.statement.executeUpdate(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", null);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.statement.executeUpdate(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		this.expectLocks(sql, null, "table");
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.executeUpdate(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", "table");
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.executeUpdate(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result == 1 : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
	 */
	@Test(dataProvider = "string-ints")
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
	{
		this.expectLocks(sql, null, null);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql, columnIndexes)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnIndexes)).andReturn(1);
		
		this.readLock.unlock();
		
		this.replay();
		
		int result = this.statement.executeUpdate(sql, columnIndexes);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", null);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql, columnIndexes)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnIndexes)).andReturn(1);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.statement.executeUpdate(sql, columnIndexes);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		this.expectLocks(sql, null, "table");
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql, columnIndexes)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnIndexes)).andReturn(1);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.executeUpdate(sql, columnIndexes);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", "table");
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql, columnIndexes)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnIndexes)).andReturn(1);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.executeUpdate(sql, columnIndexes);
		
		this.verify();
		
		assert result == 1 : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
	 */
	@Test(dataProvider = "string-strings")
	public int executeUpdate(String sql, String[] columnNames) throws SQLException
	{
		this.expectLocks(sql, null, null);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql, columnNames)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnNames)).andReturn(1);
		
		this.readLock.unlock();
		
		this.replay();
		
		int result = this.statement.executeUpdate(sql, columnNames);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", null);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql, columnNames)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnNames)).andReturn(1);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.statement.executeUpdate(sql, columnNames);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		this.expectLocks(sql, null, "table");
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql, columnNames)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnNames)).andReturn(1);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.executeUpdate(sql, columnNames);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		this.expectLocks(sql, "sequence", "table");
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.statement1.executeUpdate(sql, columnNames)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnNames)).andReturn(1);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.statement.executeUpdate(sql, columnNames);
		
		this.verify();
		
		assert result == 1 : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getConnection()
	 */
	@Test
	public java.sql.Connection getConnection() throws SQLException
	{
		java.sql.Connection connection = this.statement.getConnection();
		
		assert connection == this.connection;
		
		return connection;
	}

	/**
	 * @see java.sql.Statement#getFetchDirection()
	 */
	@Test
	public int getFetchDirection() throws SQLException
	{
		EasyMock.expect(this.statement1.getFetchDirection()).andReturn(ResultSet.FETCH_FORWARD);
		
		this.replay();
		
		int result = this.statement.getFetchDirection();
		
		this.verify();
		
		assert result == ResultSet.FETCH_FORWARD : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getFetchSize()
	 */
	@Test
	public int getFetchSize() throws SQLException
	{
		EasyMock.expect(this.statement1.getFetchDirection()).andReturn(1);
		
		this.replay();
		
		int result = this.statement.getFetchDirection();
		
		this.verify();
		
		assert result == 1 : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getGeneratedKeys()
	 */
	@Test
	public ResultSet getGeneratedKeys() throws SQLException
	{
		ResultSet resultSet = EasyMock.createMock(ResultSet.class);
		
		EasyMock.expect(this.statement1.getGeneratedKeys()).andReturn(resultSet);
		
		this.replay();
		
		ResultSet results = this.statement.getGeneratedKeys();
		
		this.verify();
		
		assert results == resultSet : results;
		
		return results;
	}

	/**
	 * @see java.sql.Statement#getMaxFieldSize()
	 */
	@Test
	public int getMaxFieldSize() throws SQLException
	{
		EasyMock.expect(this.statement1.getMaxFieldSize()).andReturn(1);
		
		this.replay();
		
		int result = this.statement.getMaxFieldSize();
		
		this.verify();
		
		assert result == 1 : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getMaxRows()
	 */
	@Test
	public int getMaxRows() throws SQLException
	{
		EasyMock.expect(this.statement1.getMaxRows()).andReturn(1);
		
		this.replay();
		
		int result = this.statement.getMaxRows();
		
		this.verify();
		
		assert result == 1 : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getMoreResults()
	 */
	@Test
	public boolean getMoreResults() throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.getMoreResults()).andReturn(true);
		EasyMock.expect(this.statement2.getMoreResults()).andReturn(true);
		
		this.replay();
		
		boolean more = this.statement.getMoreResults();
		
		this.verify();
		
		assert more;
		
		return more;
	}

	@DataProvider(name = "int")
	Object[][] intProvider()
	{
		return new Object[][] { new Object[] { Statement.KEEP_CURRENT_RESULT }, new Object[] { Statement.CLOSE_ALL_RESULTS } };
	}
	
	/**
	 * @see java.sql.Statement#getMoreResults(int)
	 */
	@Test(dataProvider = "int")
	public boolean getMoreResults(int current) throws SQLException
	{
		if (current == Statement.KEEP_CURRENT_RESULT)
		{
			EasyMock.expect(this.statement1.getMoreResults(current)).andReturn(true);
			EasyMock.expect(this.statement2.getMoreResults(current)).andReturn(true);
		}
		else
		{
			EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

			EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
			
			this.root.retain(this.databaseSet);
			
			EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
			
			EasyMock.expect(this.statement1.getMoreResults(current)).andReturn(true);
			EasyMock.expect(this.statement2.getMoreResults(current)).andReturn(true);
		}
		
		this.replay();
		
		boolean more = this.statement.getMoreResults(current);
		
		this.verify();
		
		assert more;
		
		return more;
	}

	/**
	 * @see java.sql.Statement#getQueryTimeout()
	 */
	@Test
	public int getQueryTimeout() throws SQLException
	{
		EasyMock.expect(this.statement1.getQueryTimeout()).andReturn(1);
		
		this.replay();
		
		int result = this.statement.getQueryTimeout();
		
		this.verify();
		
		assert result == 1 : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getResultSet()
	 */
	@SuppressWarnings("unchecked")
	@Test
	public ResultSet getResultSet() throws SQLException
	{
		ResultSet resultSet1 = EasyMock.createMock(ResultSet.class);
		ResultSet resultSet2 = EasyMock.createMock(ResultSet.class);
		
		// Read-only
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);

		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.statement2.getResultSet()).andReturn(resultSet2);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		ResultSet results = this.statement.getResultSet();
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;

		this.reset();
		
		// Updatable
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_UPDATABLE);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.statement1.getResultSet()).andReturn(resultSet1);
		EasyMock.expect(this.statement2.getResultSet()).andReturn(resultSet2);

		this.readLock.unlock();
		
		this.replay();
		
		results = this.statement.getResultSet();
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database1) == resultSet1;
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;

		return results;
	}

	/**
	 * @see java.sql.Statement#getResultSetConcurrency()
	 */
	@Test
	public int getResultSetConcurrency() throws SQLException
	{
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		this.replay();
		
		int result = this.statement.getResultSetConcurrency();
		
		this.verify();
		
		assert result == ResultSet.CONCUR_READ_ONLY : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getResultSetHoldability()
	 */
	@Test
	public int getResultSetHoldability() throws SQLException
	{
		EasyMock.expect(this.statement1.getResultSetHoldability()).andReturn(ResultSet.CLOSE_CURSORS_AT_COMMIT);
		
		this.replay();
		
		int result = this.statement.getResultSetHoldability();
		
		this.verify();
		
		assert result == ResultSet.CLOSE_CURSORS_AT_COMMIT : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getResultSetType()
	 */
	@Test
	public int getResultSetType() throws SQLException
	{
		EasyMock.expect(this.statement1.getResultSetType()).andReturn(ResultSet.TYPE_FORWARD_ONLY);
		
		this.replay();
		
		int result = this.statement.getResultSetType();
		
		this.verify();
		
		assert result == ResultSet.TYPE_FORWARD_ONLY : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getUpdateCount()
	 */
	@Test
	public int getUpdateCount() throws SQLException
	{
		EasyMock.expect(this.statement1.getUpdateCount()).andReturn(1);
		
		this.replay();
		
		int result = this.statement.getUpdateCount();
		
		this.verify();
		
		assert result == 1 : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getWarnings()
	 */
	@Test
	public SQLWarning getWarnings() throws SQLException
	{
		SQLWarning warning = new SQLWarning();
		
		EasyMock.expect(this.statement1.getWarnings()).andReturn(warning);
		
		this.replay();
		
		SQLWarning result = this.statement.getWarnings();
		
		this.verify();
		
		assert result == warning : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#setCursorName(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void setCursorName(String name) throws SQLException
	{
		this.statement1.setCursorName(name);
		this.statement2.setCursorName(name);
		
		this.replay();
		
		this.statement.setCursorName(name);
		
		this.verify();
	}

	@DataProvider(name = "boolean")
	Object[][] booleanProvider()
	{
		return new Object[][] { new Object[] { true } };
	}
	
	/**
	 * @see java.sql.Statement#setEscapeProcessing(boolean)
	 */
	@Test(dataProvider = "boolean")
	public void setEscapeProcessing(boolean enable) throws SQLException
	{
		this.statement1.setEscapeProcessing(enable);
		this.statement2.setEscapeProcessing(enable);
		
		this.replay();
		
		this.statement.setEscapeProcessing(enable);
		
		this.verify();
	}

	@DataProvider(name = "direction")
	Object[][] directionProvider()
	{
		return new Object[][] { new Object[] { ResultSet.FETCH_FORWARD } };
	}

	/**
	 * @see java.sql.Statement#setFetchDirection(int)
	 */
	@Test(dataProvider = "direction")
	public void setFetchDirection(int direction) throws SQLException
	{
		this.statement1.setFetchDirection(direction);
		this.statement2.setFetchDirection(direction);
		
		this.replay();
		
		this.statement.setFetchDirection(direction);
		
		this.verify();
	}

	/**
	 * @see java.sql.Statement#setFetchSize(int)
	 */
	@Test(dataProvider = "int")
	public void setFetchSize(int rows) throws SQLException
	{
		this.statement1.setFetchSize(rows);
		this.statement2.setFetchSize(rows);
		
		this.replay();
		
		this.statement.setFetchSize(rows);
		
		this.verify();
	}

	/**
	 * @see java.sql.Statement#setMaxFieldSize(int)
	 */
	@Test(dataProvider = "int")
	public void setMaxFieldSize(int max) throws SQLException
	{
		this.statement1.setMaxFieldSize(max);
		this.statement2.setMaxFieldSize(max);
		
		this.replay();
		
		this.statement.setMaxFieldSize(max);
		
		this.verify();
	}

	/**
	 * @see java.sql.Statement#setMaxRows(int)
	 */
	@Test(dataProvider = "int")
	public void setMaxRows(int max) throws SQLException
	{
		this.statement1.setMaxRows(max);
		this.statement2.setMaxRows(max);
		
		this.replay();
		
		this.statement.setMaxRows(max);
		
		this.verify();
	}

	/**
	 * @see java.sql.Statement#setQueryTimeout(int)
	 */
	@Test(dataProvider = "int")
	public void setQueryTimeout(int seconds) throws SQLException
	{
		this.statement1.setQueryTimeout(seconds);
		this.statement2.setQueryTimeout(seconds);
		
		this.replay();
		
		this.statement.setQueryTimeout(seconds);
		
		this.verify();
	}

	/**
	 * @see java.sql.Statement#isClosed()
	 */
	@Test
	public boolean isClosed() throws SQLException
	{
		EasyMock.expect(this.statement1.isClosed()).andReturn(true);

		this.replay();
		
		boolean result = this.statement.isClosed();
		
		return result;
	}

	/**
	 * @see java.sql.Statement#isPoolable()
	 */
	@Test
	public boolean isPoolable() throws SQLException
	{
		EasyMock.expect(this.statement1.isPoolable()).andReturn(true);

		this.replay();
		
		boolean result = this.statement.isPoolable();
		
		return result;
	}

	/**
	 * @see java.sql.Statement#setPoolable(boolean)
	 */
	@Test(dataProvider = "boolean")
	public void setPoolable(boolean poolable) throws SQLException
	{
		this.statement1.setPoolable(poolable);
		this.statement2.setPoolable(poolable);

		this.replay();
		
		this.statement.setPoolable(poolable);
	}

	@DataProvider(name = "class")
	Object[][] classProvider()
	{
		return new Object[][] { new Object[] { Object.class } };
	}
	
	/**
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	@Test(dataProvider = "class")
	public boolean isWrapperFor(Class<?> targetClass) throws SQLException
	{
		EasyMock.expect(this.statement1.isWrapperFor(targetClass)).andReturn(true);

		this.replay();
		
		boolean result = this.statement.isWrapperFor(targetClass);
		
		return result;
	}

	/**
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	@Test(dataProvider = "class")
	public <T> T unwrap(Class<T> targetClass) throws SQLException
	{
		try
		{
			EasyMock.expect(this.statement1.unwrap(targetClass)).andReturn(targetClass.newInstance());
	
			this.replay();
			
			T result = this.statement.unwrap(targetClass);
			
			return result;
		}
		catch (InstantiationException e)
		{
			assert false : e;
			return null;
		}
		catch (IllegalAccessException e)
		{
			assert false : e;
			return null;
		}
	}
	
	protected void expectLocks(String sql, String sequence, String table) throws SQLException
	{	
		EasyMock.expect(this.cluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.supportsSequences()).andReturn(true);
		EasyMock.expect(this.cluster.isSequenceDetectionEnabled()).andReturn(true);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(sequence);
		
		EasyMock.expect(this.dialect.supportsIdentityColumns()).andReturn(true);
		EasyMock.expect(this.cluster.isIdentityColumnDetectionEnabled()).andReturn(true);
		EasyMock.expect(this.dialect.parseInsertTable(sql)).andReturn(table);
		
		if (table != null)
		{
			EasyMock.expect(this.cluster.getDatabaseMetaDataCache()).andReturn(this.metaData);
			EasyMock.expect(this.metaData.getDatabaseProperties(this.connection)).andReturn(this.databaseProperties);
			EasyMock.expect(this.databaseProperties.findTable(table)).andReturn(this.tableProperties);
			EasyMock.expect(this.tableProperties.getColumns()).andReturn(Collections.singleton("column"));
			EasyMock.expect(this.tableProperties.getColumnProperties("column")).andReturn(this.columnProperties);
			EasyMock.expect(this.columnProperties.isAutoIncrement()).andReturn(null);
			EasyMock.expect(this.dialect.isIdentity(this.columnProperties)).andReturn(true);
			EasyMock.expect(this.tableProperties.getName()).andReturn(table);
		}
		
		if ((sequence != null) || (table != null))
		{
			EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
			
			if (sequence != null)
			{
				EasyMock.expect(this.lockManager.writeLock("sequence")).andReturn(this.sequenceLock);
			}
			
			if (table != null)
			{
				EasyMock.expect(this.lockManager.writeLock("table")).andReturn(this.tableLock);
			}
		}
	}
	
	protected void expectSelectForUpdateCheck(String sql, boolean isSelectForUpdate) throws SQLException
	{
		EasyMock.expect(this.cluster.getDatabaseMetaDataCache()).andReturn(this.metaData);
		EasyMock.expect(this.metaData.getDatabaseProperties(this.connection)).andReturn(this.databaseProperties);
		EasyMock.expect(this.databaseProperties.supportsSelectForUpdate()).andReturn(true);
		
		EasyMock.expect(this.cluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.isSelectForUpdate(sql)).andReturn(isSelectForUpdate);
	}
}
