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
import org.easymock.IAnswer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings({ "unchecked", "nls" })
public abstract class AbstractTestStatement<S extends Statement> implements java.sql.Statement
{
	protected TransactionContext transactionContext = EasyMock.createStrictMock(TransactionContext.class);
	protected Balancer balancer = EasyMock.createStrictMock(Balancer.class);
	protected DatabaseCluster cluster = EasyMock.createStrictMock(DatabaseCluster.class);
	protected FileSupport fileSupport = EasyMock.createStrictMock(FileSupport.class);
	protected Lock sequenceLock = EasyMock.createStrictMock(Lock.class);
	protected Lock tableLock = EasyMock.createStrictMock(Lock.class);
	protected LockManager lockManager = EasyMock.createStrictMock(LockManager.class);
	protected Dialect dialect = EasyMock.createStrictMock(Dialect.class);
	protected DatabaseMetaDataCache metaData = EasyMock.createStrictMock(DatabaseMetaDataCache.class);
	protected DatabaseProperties databaseProperties = EasyMock.createStrictMock(DatabaseProperties.class);
	protected TableProperties tableProperties = EasyMock.createStrictMock(TableProperties.class);
	protected ColumnProperties columnProperties = EasyMock.createStrictMock(ColumnProperties.class);
	protected Connection connection = EasyMock.createStrictMock(Connection.class);
	protected S statement1 = EasyMock.createStrictMock(this.getStatementClass());
	protected S statement2 = EasyMock.createStrictMock(this.getStatementClass());
	protected SQLProxy parent = EasyMock.createStrictMock(SQLProxy.class);
	protected SQLProxy root = EasyMock.createStrictMock(SQLProxy.class);
	
	protected Database database1 = new MockDatabase("1");
	protected Database database2 = new MockDatabase("2");
	protected Set<Database> databaseSet;
	protected ExecutorService executor = Executors.newSingleThreadExecutor();
	protected S statement;
	protected AbstractStatementInvocationHandler handler;
	protected IAnswer<InvocationStrategy> anwser = new IAnswer<InvocationStrategy>()
	{
		@Override
		public InvocationStrategy answer() throws Throwable
		{
			return (InvocationStrategy) EasyMock.getCurrentArguments()[0];
		}		
	};
	
	protected abstract Class<S> getStatementClass();
	
	protected abstract AbstractStatementInvocationHandler<?, S> getInvocationHandler(Map<Database, S> map) throws Exception;
	
	@BeforeClass
	void init() throws Exception
	{
		Map<Database, S> map = new TreeMap<Database, S>();
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
	
	protected void recordConstructor() throws SQLException
	{
		this.parent.addChild(EasyMock.isA(StatementInvocationHandler.class));
	}
	
	private Object[] objects()
	{
		return new Object[] { this.cluster, this.balancer, this.connection, this.statement1, this.statement2, this.fileSupport, this.sequenceLock, this.tableLock, this.lockManager, this.parent, this.root, this.dialect, this.metaData, this.databaseProperties, this.tableProperties, this.columnProperties, this.transactionContext };
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.close();
		this.statement2.close();

		this.fileSupport.close();
		this.parent.removeChild(this.handler);
		
		this.replay();
		
		this.statement.close();
		
		this.verify();
	}

	@Test(dataProvider = "string")
	public void testExecute(String sql) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, null);

		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
				
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql)).andReturn(true);

		this.replay();
		
		boolean result = this.execute(sql);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql)).andReturn(true);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.execute(sql);
		
		this.verify();
		
		assert result;

		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql)).andReturn(true);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.execute(sql);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql)).andReturn(true);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.execute(sql);
		
		this.verify();
		
		assert result;
	}
	
	/**
	 * @see java.sql.Statement#execute(java.lang.String)
	 */
	@Override
	public boolean execute(String sql) throws SQLException
	{
		return this.statement.execute(sql);
	}

	@DataProvider(name = "string-int")
	Object[][] stringIntProvider()
	{
		return new Object[][] {
			new Object[] { "sql", java.sql.Statement.NO_GENERATED_KEYS },
			new Object[] { "sql", java.sql.Statement.RETURN_GENERATED_KEYS },
		};
	}

	@Test(dataProvider = "string-int")
	public void testExecute(String sql, int autoGeneratedKeys) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql, autoGeneratedKeys)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, autoGeneratedKeys)).andReturn(true);
		
		this.replay();
		
		boolean result = this.execute(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql, autoGeneratedKeys)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, autoGeneratedKeys)).andReturn(true);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.execute(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql, autoGeneratedKeys)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, autoGeneratedKeys)).andReturn(true);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.execute(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql, autoGeneratedKeys)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, autoGeneratedKeys)).andReturn(true);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.execute(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result;
	}
	
	/**
	 * @see java.sql.Statement#execute(java.lang.String, int)
	 */
	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
	{
		return this.statement.execute(sql, autoGeneratedKeys);
	}

	@DataProvider(name = "string-ints")
	Object[][] stringIntsProvider()
	{
		return new Object[][] { new Object[] { "sql", new int[] { 1 } } };
	}

	@Test(dataProvider = "string-ints")
	public void testExecute(String sql, int[] columnIndexes) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql, columnIndexes)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnIndexes)).andReturn(true);
		
		this.replay();
		
		boolean result = this.execute(sql, columnIndexes);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql, columnIndexes)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnIndexes)).andReturn(true);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.execute(sql, columnIndexes);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql, columnIndexes)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnIndexes)).andReturn(true);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.execute(sql, columnIndexes);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql, columnIndexes)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnIndexes)).andReturn(true);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.execute(sql, columnIndexes);
		
		this.verify();
		
		assert result;
	}
	
	/**
	 * @see java.sql.Statement#execute(java.lang.String, int[])
	 */
	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException
	{
		return this.statement.execute(sql, columnIndexes);
	}

	@DataProvider(name = "string-strings")
	Object[][] stringStringsProvider()
	{
		return new Object[][] { new Object[] { "sql", new String[] { "name" } } };
	}

	@Test(dataProvider = "string-strings")
	public void testExecute(String sql, String[] columnNames) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql, columnNames)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnNames)).andReturn(true);
		
		this.replay();
		
		boolean result = this.execute(sql, columnNames);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql, columnNames)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnNames)).andReturn(true);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.execute(sql, columnNames);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql, columnNames)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnNames)).andReturn(true);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.execute(sql, columnNames);
		
		this.verify();
		
		assert result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.execute(sql, columnNames)).andReturn(true);
		EasyMock.expect(this.statement2.execute(sql, columnNames)).andReturn(true);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.execute(sql, columnNames);
		
		this.verify();
		
		assert result;
	}
	
	/**
	 * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
	 */
	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException
	{
		return this.statement.execute(sql, columnNames);
	}

	public void testExecuteBatch() throws SQLException
	{
		int[] array = new int[0];
		
		this.addBatch("sql");
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers("sql", null, null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeBatch()).andReturn(array);
		EasyMock.expect(this.statement2.executeBatch()).andReturn(array);
		
		this.replay();
		
		int[] result = this.executeBatch();
		
		this.verify();
		
		assert result == array;
		
		this.reset();
		
		this.addBatch("sql");
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers("sql", "sequence", null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeBatch()).andReturn(array);
		EasyMock.expect(this.statement2.executeBatch()).andReturn(array);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.executeBatch();
		
		this.verify();
		
		assert result == array;
		
		this.reset();
		
		this.addBatch("sql");
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers("sql", null, "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeBatch()).andReturn(array);
		EasyMock.expect(this.statement2.executeBatch()).andReturn(array);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.executeBatch();
		
		this.verify();
		
		assert result == array;
		
		this.reset();
		
		this.addBatch("sql");
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers("sql", "sequence", "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeBatch()).andReturn(array);
		EasyMock.expect(this.statement2.executeBatch()).andReturn(array);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.executeBatch();
		
		this.verify();
		
		assert result == array;
		
		this.reset();
	}
	
	/**
	 * @see java.sql.Statement#executeBatch()
	 */
	@Override
	public int[] executeBatch() throws SQLException
	{
		return this.statement.executeBatch();
	}

	@Test(dataProvider = "string")
	public void testExecuteQuery(String sql) throws SQLException
	{
		ResultSet resultSet1 = EasyMock.createMock(ResultSet.class);
		ResultSet resultSet2 = EasyMock.createMock(ResultSet.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, null);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		// Read-only result set
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		this.expectSelectForUpdateCheck(sql, false);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);

		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.statement2.executeQuery(sql)).andReturn(resultSet2);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		ResultSet results = this.executeQuery(sql);
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;
		
		this.reset();
		
		EasyMock.makeThreadSafe(this.statement1, true);
		EasyMock.makeThreadSafe(this.statement2, true);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, null);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		// Updatable result set
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_UPDATABLE);
		
		this.expectSelectForUpdateCheck(sql, false);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);

		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeQuery(sql)).andReturn(resultSet1);
		EasyMock.expect(this.statement2.executeQuery(sql)).andReturn(resultSet2);
		
		this.replay();
		
		results = this.executeQuery(sql);
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database1) == resultSet1;
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;
		
		this.reset();
		
		EasyMock.makeThreadSafe(this.statement1, true);
		EasyMock.makeThreadSafe(this.statement2, true);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, null);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		// Select-for-update
		this.expectSelectForUpdateCheck(sql, true);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeQuery(sql)).andReturn(resultSet1);
		EasyMock.expect(this.statement2.executeQuery(sql)).andReturn(resultSet2);

		this.replay();
		
		results = this.executeQuery(sql);
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database1) == resultSet1;
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;
		
		this.reset();
		
		EasyMock.makeThreadSafe(this.statement1, true);
		EasyMock.makeThreadSafe(this.statement2, true);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", null);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		this.expectSelectForUpdateCheck(sql, false);

		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeQuery(sql)).andReturn(resultSet1);
		EasyMock.expect(this.statement2.executeQuery(sql)).andReturn(resultSet2);

		this.sequenceLock.unlock();
		
		this.replay();
		
		results = this.executeQuery(sql);
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database1) == resultSet1;
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;
		
		this.reset();
		
		EasyMock.makeThreadSafe(this.statement1, true);
		EasyMock.makeThreadSafe(this.statement2, true);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, "table");
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		this.expectSelectForUpdateCheck(sql, false);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeQuery(sql)).andReturn(resultSet1);
		EasyMock.expect(this.statement2.executeQuery(sql)).andReturn(resultSet2);

		this.tableLock.unlock();
		
		this.replay();
		
		results = this.executeQuery(sql);
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database1) == resultSet1;
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;
		
		this.reset();
		
		EasyMock.makeThreadSafe(this.statement1, true);
		EasyMock.makeThreadSafe(this.statement2, true);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", "table");
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		this.expectSelectForUpdateCheck(sql, false);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeQuery(sql)).andReturn(resultSet1);
		EasyMock.expect(this.statement2.executeQuery(sql)).andReturn(resultSet2);

		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		results = this.executeQuery(sql);
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database1) == resultSet1;
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;
	}
	
	/**
	 * @see java.sql.Statement#executeQuery(java.lang.String)
	 */
	@Override
	public ResultSet executeQuery(String sql) throws SQLException
	{
		return this.statement.executeQuery(sql);
	}
	
	@Test(dataProvider = "string")
	public void testExecuteUpdate(String sql) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);

		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeUpdate(sql)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql)).andReturn(1);
		
		this.replay();
		
		int result = this.executeUpdate(sql);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);

		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeUpdate(sql)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql)).andReturn(1);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.executeUpdate(sql);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);

		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeUpdate(sql)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql)).andReturn(1);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.executeUpdate(sql);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);

		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeUpdate(sql)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql)).andReturn(1);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.executeUpdate(sql);
		
		this.verify();
		
		assert result == 1 : result;
	}
	
	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String)
	 */
	@Override
	public int executeUpdate(String sql) throws SQLException
	{
		return this.statement.executeUpdate(sql);
	}

	@Test(dataProvider = "string-int")
	public void testExecuteUpdate(String sql, int autoGeneratedKeys) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		
		this.replay();
		
		int result = this.executeUpdate(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.executeUpdate(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.executeUpdate(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, autoGeneratedKeys)).andReturn(1);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.executeUpdate(sql, autoGeneratedKeys);
		
		this.verify();
		
		assert result == 1 : result;
	}
	
	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int)
	 */
	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
	{
		return this.statement.executeUpdate(sql, autoGeneratedKeys);
	}

	@Test(dataProvider = "string-ints")
	public void testExecuteUpdate(String sql, int[] columnIndexes) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeUpdate(sql, columnIndexes)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnIndexes)).andReturn(1);
		
		this.replay();
		
		int result = this.executeUpdate(sql, columnIndexes);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeUpdate(sql, columnIndexes)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnIndexes)).andReturn(1);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.executeUpdate(sql, columnIndexes);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);

		EasyMock.expect(this.statement1.executeUpdate(sql, columnIndexes)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnIndexes)).andReturn(1);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.executeUpdate(sql, columnIndexes);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);

		EasyMock.expect(this.statement1.executeUpdate(sql, columnIndexes)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnIndexes)).andReturn(1);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.executeUpdate(sql, columnIndexes);
		
		this.verify();
		
		assert result == 1 : result;
	}
	
	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
	 */
	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
	{
		return this.statement.executeUpdate(sql, columnIndexes);
	}

	@Test(dataProvider = "string-strings")
	public void testExecuteUpdate(String sql, String[] columnNames) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeUpdate(sql, columnNames)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnNames)).andReturn(1);
		
		this.replay();
		
		int result = this.executeUpdate(sql, columnNames);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", null);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeUpdate(sql, columnNames)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnNames)).andReturn(1);
		
		this.sequenceLock.unlock();
		
		this.replay();
		
		result = this.executeUpdate(sql, columnNames);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, null, "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);

		EasyMock.expect(this.statement1.executeUpdate(sql, columnNames)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnNames)).andReturn(1);
		
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.executeUpdate(sql, columnNames);
		
		this.verify();
		
		assert result == 1 : result;
		
		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.expectIdentifiers(sql, "sequence", "table");
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.transactionContext.start(EasyMock.isA(InvocationStrategy.class), EasyMock.same(this.connection))).andAnswer(this.anwser);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandEvaluationEnabled()).andReturn(false);
		
		this.sequenceLock.lock();
		this.tableLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.executeUpdate(sql, columnNames)).andReturn(1);
		EasyMock.expect(this.statement2.executeUpdate(sql, columnNames)).andReturn(1);
		
		this.sequenceLock.unlock();
		this.tableLock.unlock();
		
		this.replay();
		
		result = this.executeUpdate(sql, columnNames);
		
		this.verify();
		
		assert result == 1 : result;
	}
	
	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
	 */
	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException
	{
		return this.statement.executeUpdate(sql, columnNames);
	}

	public void testGetConnection() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.replay();
		
		java.sql.Connection connection = this.getConnection();

		this.verify();
		
		assert connection == this.connection;
	}
	
	/**
	 * @see java.sql.Statement#getConnection()
	 */
	@Override
	public java.sql.Connection getConnection() throws SQLException
	{
		return this.statement.getConnection();
	}

	public void testGetFetchDirection() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getFetchDirection()).andReturn(ResultSet.FETCH_FORWARD);
		
		this.replay();
		
		int result = this.getFetchDirection();
		
		this.verify();
		
		assert result == ResultSet.FETCH_FORWARD : result;
	}
	
	/**
	 * @see java.sql.Statement#getFetchDirection()
	 */
	@Override
	public int getFetchDirection() throws SQLException
	{
		return this.statement.getFetchDirection();
	}

	public void testGetFetchSize() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getFetchDirection()).andReturn(1);
		
		this.replay();
		
		int result = this.getFetchDirection();
		
		this.verify();
		
		assert result == 1 : result;
	}
	
	/**
	 * @see java.sql.Statement#getFetchSize()
	 */
	@Override
	public int getFetchSize() throws SQLException
	{
		return this.statement.getFetchSize();
	}

	public void testGetGeneratedKeys() throws SQLException
	{
		ResultSet resultSet = EasyMock.createMock(ResultSet.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getGeneratedKeys()).andReturn(resultSet);
		
		this.replay();
		
		ResultSet results = this.getGeneratedKeys();
		
		this.verify();
		
		assert results == resultSet : results;
	}
	
	/**
	 * @see java.sql.Statement#getGeneratedKeys()
	 */
	@Override
	public ResultSet getGeneratedKeys() throws SQLException
	{
		return this.statement.getGeneratedKeys();
	}

	public void testGetMaxFieldSize() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getMaxFieldSize()).andReturn(1);
		
		this.replay();
		
		int result = this.getMaxFieldSize();
		
		this.verify();
		
		assert result == 1 : result;
	}
	
	/**
	 * @see java.sql.Statement#getMaxFieldSize()
	 */
	@Override
	public int getMaxFieldSize() throws SQLException
	{
		return this.statement.getMaxFieldSize();
	}

	public void testGetMaxRows() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getMaxRows()).andReturn(1);
		
		this.replay();
		
		int result = this.getMaxRows();
		
		this.verify();
		
		assert result == 1 : result;
	}
	
	/**
	 * @see java.sql.Statement#getMaxRows()
	 */
	@Override
	public int getMaxRows() throws SQLException
	{
		return this.statement.getMaxRows();
	}

	public void testGetMoreResults() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.getMoreResults()).andReturn(true);
		EasyMock.expect(this.statement2.getMoreResults()).andReturn(true);
		
		this.replay();
		
		boolean more = this.getMoreResults();
		
		this.verify();
		
		assert more;
	}
	
	/**
	 * @see java.sql.Statement#getMoreResults()
	 */
	@Override
	public boolean getMoreResults() throws SQLException
	{
		return this.statement.getMoreResults();
	}

	@DataProvider(name = "current")
	Object[][] currentProvider()
	{
		return new Object[][] {
			new Object[] { Statement.KEEP_CURRENT_RESULT },
			new Object[] { Statement.CLOSE_ALL_RESULTS }
		};
	}
	
	@Test(dataProvider = "current")
	public void testGetMoreResults(int current) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		if (current == Statement.KEEP_CURRENT_RESULT)
		{
			EasyMock.expect(this.statement1.getMoreResults(current)).andReturn(true);
			EasyMock.expect(this.statement2.getMoreResults(current)).andReturn(true);
		}
		else
		{
			EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
			
			EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

			EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
			
			this.root.retain(this.databaseSet);
			
			EasyMock.expect(this.statement1.getMoreResults(current)).andReturn(true);
			EasyMock.expect(this.statement2.getMoreResults(current)).andReturn(true);
		}
		
		this.replay();
		
		boolean more = this.getMoreResults(current);
		
		this.verify();
		
		assert more;
	}
	
	/**
	 * @see java.sql.Statement#getMoreResults(int)
	 */
	@Override
	public boolean getMoreResults(int current) throws SQLException
	{
		return this.statement.getMoreResults(current);
	}

	public void testGetQueryTimeout() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getQueryTimeout()).andReturn(1);
		
		this.replay();
		
		int result = this.getQueryTimeout();
		
		this.verify();
		
		assert result == 1 : result;
	}
	
	/**
	 * @see java.sql.Statement#getQueryTimeout()
	 */
	@Override
	public int getQueryTimeout() throws SQLException
	{
		return this.statement.getQueryTimeout();
	}

	public void testGetResultSet() throws SQLException
	{
		ResultSet resultSet1 = EasyMock.createMock(ResultSet.class);
		ResultSet resultSet2 = EasyMock.createMock(ResultSet.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true).times(2);
		
		// Read-only
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);

		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.statement2.getResultSet()).andReturn(resultSet2);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		ResultSet results = this.getResultSet();
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;

		this.reset();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true).times(2);
		
		// Updatable
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_UPDATABLE);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);

		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.statement1.getResultSet()).andReturn(resultSet1);
		EasyMock.expect(this.statement2.getResultSet()).andReturn(resultSet2);
		
		this.replay();
		
		results = this.getResultSet();
		
		this.verify();
		
		assert Proxy.isProxyClass(results.getClass());
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database1) == resultSet1;
		assert SQLProxy.class.cast(Proxy.getInvocationHandler(results)).getObject(this.database2) == resultSet2;
	}
	
	/**
	 * @see java.sql.Statement#getResultSet()
	 */
	@Override
	public ResultSet getResultSet() throws SQLException
	{
		return this.statement.getResultSet();
	}

	public void testGetResultSetConcurrency() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		this.replay();
		
		int result = this.statement.getResultSetConcurrency();
		
		this.verify();
		
		assert result == ResultSet.CONCUR_READ_ONLY : result;
	}
	
	/**
	 * @see java.sql.Statement#getResultSetConcurrency()
	 */
	@Override
	public int getResultSetConcurrency() throws SQLException
	{
		return this.statement.getResultSetConcurrency();
	}

	public void testGetResultSetHoldability() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getResultSetHoldability()).andReturn(ResultSet.CLOSE_CURSORS_AT_COMMIT);
		
		this.replay();
		
		int result = this.getResultSetHoldability();
		
		this.verify();
		
		assert result == ResultSet.CLOSE_CURSORS_AT_COMMIT : result;
	}
	
	/**
	 * @see java.sql.Statement#getResultSetHoldability()
	 */
	@Override
	public int getResultSetHoldability() throws SQLException
	{
		return this.statement.getResultSetHoldability();
	}

	public void testGetResultType() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getResultSetType()).andReturn(ResultSet.TYPE_FORWARD_ONLY);
		
		this.replay();
		
		int result = this.getResultSetType();
		
		this.verify();
		
		assert result == ResultSet.TYPE_FORWARD_ONLY : result;
	}
	
	/**
	 * @see java.sql.Statement#getResultSetType()
	 */
	@Override
	public int getResultSetType() throws SQLException
	{
		return this.statement.getResultSetType();
	}

	public void testGetUpdateCount() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getUpdateCount()).andReturn(1);
		
		this.replay();
		
		int result = this.getUpdateCount();
		
		this.verify();
		
		assert result == 1 : result;
	}
	
	/**
	 * @see java.sql.Statement#getUpdateCount()
	 */
	@Override
	public int getUpdateCount() throws SQLException
	{
		return this.statement.getUpdateCount();
	}

	public void testGetWarnings() throws SQLException
	{
		SQLWarning warning = new SQLWarning();
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.getWarnings()).andReturn(warning);
		
		this.replay();
		
		SQLWarning result = this.getWarnings();
		
		this.verify();
		
		assert result == warning : result;
	}
	
	/**
	 * @see java.sql.Statement#getWarnings()
	 */
	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		return this.statement.getWarnings();
	}

	/**
	 * @see java.sql.Statement#setCursorName(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void setCursorName(String name) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setFetchDirection(direction);
		this.statement2.setFetchDirection(direction);
		
		this.replay();
		
		this.statement.setFetchDirection(direction);
		
		this.verify();
	}

	@DataProvider(name = "int")
	Object[][] intProvider()
	{
		return new Object[][] { new Object[] { 1 } };
	}

	/**
	 * @see java.sql.Statement#setFetchSize(int)
	 */
	@Test(dataProvider = "int")
	public void setFetchSize(int rows) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
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
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		this.statement1.setQueryTimeout(seconds);
		this.statement2.setQueryTimeout(seconds);
		
		this.replay();
		
		this.statement.setQueryTimeout(seconds);
		
		this.verify();
	}

	public void testIsClosed() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.isClosed()).andReturn(true);

		this.replay();
		
		boolean result = this.isClosed();
		
		assert result;
	}
	
	/**
	 * @see java.sql.Statement#isClosed()
	 */
	@Override
	public boolean isClosed() throws SQLException
	{
		return this.statement.isClosed();
	}

	public void testIsPoolable() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.isPoolable()).andReturn(true);

		this.replay();
		
		boolean result = this.isPoolable();
		
		assert result;
	}
	
	/**
	 * @see java.sql.Statement#isPoolable()
	 */
	@Override
	public boolean isPoolable() throws SQLException
	{
		return this.statement.isPoolable();
	}

	/**
	 * @see java.sql.Statement#setPoolable(boolean)
	 */
	@Test(dataProvider = "boolean")
	public void setPoolable(boolean poolable) throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
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
	
	public void testIsWrapperFor() throws SQLException
	{
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.isWrapperFor(Statement.class)).andReturn(true);

		this.replay();
		
		boolean result = this.isWrapperFor(Statement.class);
		
		assert result;
	}
	
	/**
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	@Override
	public boolean isWrapperFor(Class<?> targetClass) throws SQLException
	{
		return this.statement.isWrapperFor(targetClass);
	}

	public void testUnwrap() throws SQLException
	{
		Statement statement = EasyMock.createMock(Statement.class);
		
		EasyMock.expect(this.cluster.isActive()).andReturn(true);
		
		EasyMock.expect(this.statement1.unwrap(Statement.class)).andReturn(statement);
		
		this.replay();
		
		Statement result = this.statement.unwrap(Statement.class);
		
		assert result == statement;
	}
	
	/**
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	@Override
	public <T> T unwrap(Class<T> targetClass) throws SQLException
	{
		return this.statement.unwrap(targetClass);
	}

	protected void expectIdentifiers(String sql, String sequence, String table) throws SQLException
	{
		EasyMock.expect(this.cluster.isSequenceDetectionEnabled()).andReturn(true);
		EasyMock.expect(this.cluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(sequence);
		
		EasyMock.expect(this.cluster.isIdentityColumnDetectionEnabled()).andReturn(true);
		EasyMock.expect(this.cluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseInsertTable(sql)).andReturn(table);
		
		if (table != null)
		{
			EasyMock.expect(this.cluster.getDatabaseMetaDataCache()).andReturn(this.metaData);
			EasyMock.expect(this.metaData.getDatabaseProperties(EasyMock.same(this.connection))).andReturn(this.databaseProperties);
			EasyMock.expect(this.databaseProperties.findTable(table)).andReturn(this.tableProperties);
			EasyMock.expect(this.tableProperties.getIdentityColumns()).andReturn(Collections.singleton("column"));
			EasyMock.expect(this.tableProperties.getName()).andReturn(table);
		}
		
		if ((sequence != null) || (table != null))
		{
			EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
			
			if (sequence != null)
			{
				EasyMock.expect(this.lockManager.writeLock(sequence)).andReturn(this.sequenceLock);			
			}
			
			if (table != null)
			{
				EasyMock.expect(this.lockManager.writeLock(table)).andReturn(this.tableLock);			
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
