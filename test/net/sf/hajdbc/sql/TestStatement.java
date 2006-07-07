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
package net.sf.hajdbc.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.MockDatabase;
import net.sf.hajdbc.Operation;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.Configuration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public class TestStatement implements java.sql.Statement
{
	protected IMocksControl control = EasyMock.createStrictControl();
	
	protected DatabaseCluster databaseCluster = this.control.createMock(DatabaseCluster.class);
	protected LockManager lockManager = this.control.createMock(LockManager.class);
	protected DatabaseMetaDataCache metaData = this.control.createMock(DatabaseMetaDataCache.class);
	protected DatabaseProperties databaseProperties = this.control.createMock(DatabaseProperties.class);
	protected java.sql.Connection sqlConnection = this.control.createMock(java.sql.Connection.class);
	protected java.sql.Statement sqlStatement = this.control.createMock(this.getStatementClass());
	protected Balancer balancer = this.control.createMock(Balancer.class);
	protected FileSupport fileSupport = this.control.createMock(FileSupport.class);
	protected Lock lock = this.control.createMock(Lock.class);	
	protected Dialect dialect = this.control.createMock(Dialect.class);
	
	protected Connection connection;
	protected Statement statement;
	protected Database database = new MockDatabase();
	protected List<Database> databaseList = Collections.singletonList(this.database);
	protected ExecutorService executor = Executors.newSingleThreadExecutor();
	
	protected Class<? extends java.sql.Statement> getStatementClass()
	{
		return java.sql.Statement.class;
	}
	
	protected Statement createStatement(Connection connection) throws SQLException
	{
		Operation<java.sql.Connection, java.sql.Statement> operation = new Operation<java.sql.Connection, java.sql.Statement>()
		{
			public java.sql.Statement execute(Database database, java.sql.Connection connection)
			{
				return TestStatement.this.sqlStatement;
			}
		};
		
		return new Statement<java.sql.Statement>(connection, operation);
	}
	
	@SuppressWarnings("unchecked")
	@Configuration(beforeTestMethod = true)
	protected void setUp() throws Exception
	{
		Map map = Collections.singletonMap(this.database, new Object());
		
		EasyMock.expect(this.databaseCluster.getConnectionFactoryMap()).andReturn(map);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		this.lock.lock();
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		this.lock.unlock();

		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		this.lock.lock();
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		this.lock.unlock();
		
		this.control.replay();
		
		ConnectionFactory<Object> connectionFactory = new ConnectionFactory<Object>(this.databaseCluster, Object.class);
		
		Operation<Object, java.sql.Connection> operation = new Operation<Object, java.sql.Connection>()
		{
			public java.sql.Connection execute(Database database, Object object)
			{
				return TestStatement.this.sqlConnection;
			}
		};
		
		this.connection = new Connection<Object>(connectionFactory, operation, this.fileSupport);
		
		this.statement = this.createStatement(this.connection);
		
		this.control.verify();
		this.control.reset();
	}
	
	@Configuration(afterTestMethod = true)
	public void reset()
	{
		this.control.reset();
	}

	@DataProvider(name = "string")
	protected Object[][] stringProvider()
	{
		return new Object[][] { new Object[] { "sql" } };
	}
	
	/**
	 * @see java.sql.Statement#addBatch(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void addBatch(String sql) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlStatement.addBatch(sql);
		
		this.control.replay();
		
		this.statement.addBatch(sql);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.Statement#cancel()
	 */
	@Test
	public void cancel() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlStatement.cancel();
		
		this.lock.unlock();
		
		this.control.replay();
		
		this.statement.cancel();
		
		this.control.verify();
	}

	/**
	 * @see java.sql.Statement#clearBatch()
	 */
	@Test
	public void clearBatch() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlStatement.clearBatch();
		
		this.control.replay();
		
		this.statement.clearBatch();
		
		this.control.verify();
	}

	/**
	 * @see java.sql.Statement#clearWarnings()
	 */
	@Test
	public void clearWarnings() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlStatement.clearWarnings();
		
		this.control.replay();
		
		this.statement.clearWarnings();
		
		this.control.verify();
	}

	/**
	 * @see java.sql.Statement#close()
	 */
	@Test
	public void close() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlStatement.close();
		
		this.lock.unlock();
		
		this.control.replay();
		
		this.statement.close();
		
		this.control.verify();
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public boolean execute(String sql) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.execute(sql)).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();
		
		boolean result = this.statement.execute(sql);
		
		this.control.verify();
		
		assert result;
		
		this.control.reset();
		
		String sequence = "sequence";
		
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(sequence);
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.writeLock(sequence)).andReturn(this.lock);
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.execute(sql)).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();
		
		result = this.statement.execute(sql);
		
		this.control.verify();
		
		assert result;
		
		return result;
	}

	@DataProvider(name = "string-int")
	protected Object[][] stringIntProvider()
	{
		return new Object[][] { new Object[] { "sql", java.sql.Statement.NO_GENERATED_KEYS } };
	}
	
	/**
	 * @see java.sql.Statement#execute(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.execute(sql, autoGeneratedKeys)).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();
		
		boolean result = this.statement.execute(sql, autoGeneratedKeys);
		
		this.control.verify();
		
		assert result;
		
		this.control.reset();
		
		String sequence = "sequence";
		
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(sequence);
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.writeLock(sequence)).andReturn(this.lock);
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.execute(sql, autoGeneratedKeys)).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();
		
		result = this.statement.execute(sql, autoGeneratedKeys);
		
		this.control.verify();
		
		assert result;
		
		return result;
	}

	@DataProvider(name = "string-ints")
	protected Object[][] stringIntsProvider()
	{
		return new Object[][] { new Object[] { "sql", new int[] { 1 } } };
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String, int[])
	 */
	@Test(dataProvider = "string-ints")
	public boolean execute(String sql, int[] columnIndexes) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.execute(sql, columnIndexes)).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();
		
		boolean result = this.statement.execute(sql, columnIndexes);
		
		this.control.verify();
		
		assert result;
		
		this.control.reset();
		
		String sequence = "sequence";
		
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(sequence);
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.writeLock(sequence)).andReturn(this.lock);
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.execute(sql, columnIndexes)).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();
		
		result = this.statement.execute(sql, columnIndexes);
		
		this.control.verify();
		
		assert result;
		
		return result;
	}

	@DataProvider(name = "string-strings")
	protected Object[][] stringStringsProvider()
	{
		return new Object[][] { new Object[] { "sql", new String[] { "name" } } };
	}

	/**
	 * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
	 */
	@Test(dataProvider = "string-strings")
	public boolean execute(String sql, String[] columnNames) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.execute(sql, columnNames)).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();
		
		boolean result = this.statement.execute(sql, columnNames);
		
		this.control.verify();
		
		assert result;
		
		this.control.reset();
		
		String sequence = "sequence";
		
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(sequence);
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.writeLock(sequence)).andReturn(this.lock);
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.execute(sql, columnNames)).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();
		
		result = this.statement.execute(sql, columnNames);
		
		this.control.verify();
		
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
		
		this.addBatch("sql1");
		this.control.reset();
		this.addBatch("sql2");
		this.control.reset();
		
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence("sql1")).andReturn(null);
		EasyMock.expect(this.dialect.parseSequence("sql2")).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.executeBatch()).andReturn(array);
		
		this.lock.unlock();
		
		this.control.replay();
		
		int[] results = this.statement.executeBatch();
		
		this.control.verify();
		
		assert results == array;
		
		this.control.reset();
		
		String sequence = "sequence";
		
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence("sql1")).andReturn(sequence);
		EasyMock.expect(this.dialect.parseSequence("sql2")).andReturn(sequence);

		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.writeLock(sequence)).andReturn(this.lock);
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.executeBatch()).andReturn(array);
		
		this.lock.unlock();
		
		this.control.replay();
		
		results = this.statement.executeBatch();
		
		this.control.verify();
		
		assert results == array;
		
		return results;
	}

	/**
	 * @see java.sql.Statement#executeQuery(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public ResultSet executeQuery(String sql) throws SQLException
	{
		ResultSet resultSet = EasyMock.createMock(ResultSet.class);
		
		// Read-only result set
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlStatement.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		EasyMock.expect(this.databaseCluster.getDatabaseMetaDataCache()).andReturn(this.metaData);
		EasyMock.expect(this.metaData.getDatabaseProperties(this.connection)).andReturn(this.databaseProperties);
		EasyMock.expect(this.databaseProperties.isSelectForUpdateSupported()).andReturn(true);
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.isSelectForUpdate(sql)).andReturn(false);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);

		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlStatement.executeQuery(sql)).andReturn(resultSet);

		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		ResultSet results = this.statement.executeQuery(sql);
		
		this.control.verify();
		
		assert results == resultSet;
		
		this.control.reset();
		
		// Sequence reference
		String sequence = "sequence";
		
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(sequence);
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.writeLock(sequence)).andReturn(this.lock);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.executeQuery(sql)).andReturn(resultSet);
		
		this.lock.unlock();
		
		this.control.replay();
		
		results = this.statement.executeQuery(sql);
		
		this.control.verify();
		
		assert net.sf.hajdbc.sql.ResultSet.class.isInstance(results) : results.getClass().getName();
		assert net.sf.hajdbc.sql.ResultSet.class.cast(results).getObject(this.database) == resultSet;
		
		this.control.reset();
		
		// Updatable result set
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlStatement.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_UPDATABLE);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.executeQuery(sql)).andReturn(resultSet);
		
		this.lock.unlock();
		
		this.control.replay();
		
		results = this.statement.executeQuery(sql);
		
		this.control.verify();
		
		assert net.sf.hajdbc.sql.ResultSet.class.isInstance(results) : results.getClass().getName();
		assert net.sf.hajdbc.sql.ResultSet.class.cast(results).getObject(this.database) == resultSet;
		
		this.control.reset();
		
		// SELECT...FOR UPDATE
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlStatement.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		EasyMock.expect(this.databaseCluster.getDatabaseMetaDataCache()).andReturn(this.metaData);
		EasyMock.expect(this.metaData.getDatabaseProperties(this.connection)).andReturn(this.databaseProperties);
		EasyMock.expect(this.databaseProperties.isSelectForUpdateSupported()).andReturn(true);
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.isSelectForUpdate(sql)).andReturn(true);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.executeQuery(sql)).andReturn(resultSet);
		
		this.lock.unlock();
		
		this.control.replay();
		
		results = this.statement.executeQuery(sql);
		
		this.control.verify();
		
		assert net.sf.hajdbc.sql.ResultSet.class.isInstance(results) : results.getClass().getName();
		assert net.sf.hajdbc.sql.ResultSet.class.cast(results).getObject(this.database) == resultSet;
		
		return results;
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public int executeUpdate(String sql) throws SQLException
	{
		int expected = 0;
		
		// Read-only result set
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		EasyMock.expect(this.sqlStatement.executeUpdate(sql)).andReturn(expected);

		this.lock.unlock();
		
		this.control.replay();
		
		int results = this.statement.executeUpdate(sql);
		
		this.control.verify();
		
		assert results == expected : results;
		
		this.control.reset();
		
		// Sequence reference
		String sequence = "sequence";
		
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(sequence);
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.writeLock(sequence)).andReturn(this.lock);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.executeUpdate(sql)).andReturn(expected);
		
		this.lock.unlock();
		
		this.control.replay();
		
		results = this.statement.executeUpdate(sql);
		
		this.control.verify();
		
		assert results == expected : results;
		
		return results;
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
	{
		int expected = 0;
		
		// Read-only result set
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		EasyMock.expect(this.sqlStatement.executeUpdate(sql, autoGeneratedKeys)).andReturn(expected);

		this.lock.unlock();
		
		this.control.replay();
		
		int results = this.statement.executeUpdate(sql, autoGeneratedKeys);
		
		this.control.verify();
		
		assert results == expected : results;
		
		this.control.reset();
		
		// Sequence reference
		String sequence = "sequence";
		
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(sequence);
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.writeLock(sequence)).andReturn(this.lock);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.executeUpdate(sql, autoGeneratedKeys)).andReturn(expected);
		
		this.lock.unlock();
		
		this.control.replay();
		
		results = this.statement.executeUpdate(sql, autoGeneratedKeys);
		
		this.control.verify();
		
		assert results == expected : results;
		
		return results;
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
	 */
	@Test(dataProvider = "string-ints")
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
	{
		int expected = 0;
		
		// Read-only result set
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		EasyMock.expect(this.sqlStatement.executeUpdate(sql, columnIndexes)).andReturn(expected);

		this.lock.unlock();
		
		this.control.replay();
		
		int results = this.statement.executeUpdate(sql, columnIndexes);
		
		this.control.verify();
		
		assert results == expected : results;
		
		this.control.reset();
		
		// Sequence reference
		String sequence = "sequence";
		
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(sequence);
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.writeLock(sequence)).andReturn(this.lock);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.executeUpdate(sql, columnIndexes)).andReturn(expected);
		
		this.lock.unlock();
		
		this.control.replay();
		
		results = this.statement.executeUpdate(sql, columnIndexes);
		
		this.control.verify();
		
		assert results == expected : results;
		
		return results;
	}

	/**
	 * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
	 */
	@Test(dataProvider = "string-strings")
	public int executeUpdate(String sql, String[] columnNames) throws SQLException
	{
		int expected = 0;
		
		// Read-only result set
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(null);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		EasyMock.expect(this.sqlStatement.executeUpdate(sql, columnNames)).andReturn(expected);

		this.lock.unlock();
		
		this.control.replay();
		
		int results = this.statement.executeUpdate(sql, columnNames);
		
		this.control.verify();
		
		assert results == expected : results;
		
		this.control.reset();
		
		// Sequence reference
		String sequence = "sequence";
		
		EasyMock.expect(this.databaseCluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn(sequence);
		
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.writeLock(sequence)).andReturn(this.lock);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.executeUpdate(sql, columnNames)).andReturn(expected);
		
		this.lock.unlock();
		
		this.control.replay();
		
		results = this.statement.executeUpdate(sql, columnNames);
		
		this.control.verify();
		
		assert results == expected : results;
		
		return results;
	}

	/**
	 * @see java.sql.Statement#getConnection()
	 */
	@Test
	public java.sql.Connection getConnection()
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
		int direction = ResultSet.FETCH_FORWARD;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlStatement.getFetchDirection()).andReturn(direction);
		
		this.control.replay();
		
		int result = this.statement.getFetchDirection();
		
		this.control.verify();
		
		assert result == direction : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getFetchSize()
	 */
	@Test
	public int getFetchSize() throws SQLException
	{
		int size = 10;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlStatement.getFetchDirection()).andReturn(size);
		
		this.control.replay();
		
		int result = this.statement.getFetchDirection();
		
		this.control.verify();
		
		assert result == size : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getGeneratedKeys()
	 */
	@Test
	public ResultSet getGeneratedKeys() throws SQLException
	{
		ResultSet resultSet = EasyMock.createMock(ResultSet.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlStatement.getGeneratedKeys()).andReturn(resultSet);
		
		this.control.replay();
		
		ResultSet results = this.statement.getGeneratedKeys();
		
		this.control.verify();
		
		assert results == resultSet : results;
		
		return results;
	}

	/**
	 * @see java.sql.Statement#getMaxFieldSize()
	 */
	@Test
	public int getMaxFieldSize() throws SQLException
	{
		int size = 10;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlStatement.getMaxFieldSize()).andReturn(size);
		
		this.control.replay();
		
		int result = this.statement.getMaxFieldSize();
		
		this.control.verify();
		
		assert result == size : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getMaxRows()
	 */
	@Test
	public int getMaxRows() throws SQLException
	{
		int rows = 10;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlStatement.getMaxRows()).andReturn(rows);
		
		this.control.replay();
		
		int result = this.statement.getMaxRows();
		
		this.control.verify();
		
		assert result == rows : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getMoreResults()
	 */
	@Test
	public boolean getMoreResults() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.getMoreResults()).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();
		
		boolean more = this.statement.getMoreResults();
		
		this.control.verify();
		
		assert more;
		
		return more;
	}

	@DataProvider(name = "int")
	protected Object[][] intProvider()
	{
		return new Object[][] { new Object[] { 10 } };
	}
	
	/**
	 * @see java.sql.Statement#getMoreResults(int)
	 */
	@Test(dataProvider = "int")
	public boolean getMoreResults(int current) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.getMoreResults(current)).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();
		
		boolean more = this.statement.getMoreResults(current);
		
		this.control.verify();
		
		assert more;
		
		return more;
	}

	/**
	 * @see java.sql.Statement#getQueryTimeout()
	 */
	@Test
	public int getQueryTimeout() throws SQLException
	{
		int seconds = 10;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlStatement.getQueryTimeout()).andReturn(seconds);
		
		this.control.replay();
		
		int result = this.statement.getQueryTimeout();
		
		this.control.verify();
		
		assert result == seconds : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getResultSet()
	 */
	@Test
	public ResultSet getResultSet() throws SQLException
	{
		ResultSet resultSet = EasyMock.createMock(ResultSet.class);
		
		// Read-only
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlStatement.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_READ_ONLY);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlStatement.getResultSet()).andReturn(resultSet);
		
		this.control.replay();
		
		ResultSet results = this.statement.getResultSet();
		
		this.control.verify();
		
		assert results == resultSet;

		this.control.reset();
		
		// Updatable
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlStatement.getResultSetConcurrency()).andReturn(ResultSet.CONCUR_UPDATABLE);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlStatement.getResultSet()).andReturn(resultSet);

		this.lock.unlock();
		
		this.control.replay();
		
		results = this.statement.getResultSet();
		
		this.control.verify();
		
		assert net.sf.hajdbc.sql.ResultSet.class.isInstance(results) : results.getClass().getName();
		assert net.sf.hajdbc.sql.ResultSet.class.cast(results).getObject(this.database) == resultSet;

		return results;
	}

	/**
	 * @see java.sql.Statement#getResultSetConcurrency()
	 */
	@Test
	public int getResultSetConcurrency() throws SQLException
	{
		int concurrency = ResultSet.CONCUR_READ_ONLY;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlStatement.getResultSetConcurrency()).andReturn(concurrency);
		
		this.control.replay();
		
		int result = this.statement.getResultSetConcurrency();
		
		this.control.verify();
		
		assert result == concurrency : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getResultSetHoldability()
	 */
	@Test
	public int getResultSetHoldability() throws SQLException
	{
		int holdability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlStatement.getResultSetHoldability()).andReturn(holdability);
		
		this.control.replay();
		
		int result = this.statement.getResultSetHoldability();
		
		this.control.verify();
		
		assert result == holdability : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getResultSetType()
	 */
	@Test
	public int getResultSetType() throws SQLException
	{
		int type = ResultSet.TYPE_FORWARD_ONLY;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlStatement.getResultSetType()).andReturn(type);
		
		this.control.replay();
		
		int result = this.statement.getResultSetType();
		
		this.control.verify();
		
		assert result == type : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getUpdateCount()
	 */
	@Test
	public int getUpdateCount() throws SQLException
	{
		int count = 10;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlStatement.getUpdateCount()).andReturn(count);
		
		this.control.replay();
		
		int result = this.statement.getUpdateCount();
		
		this.control.verify();
		
		assert result == count : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#getWarnings()
	 */
	@Test
	public SQLWarning getWarnings() throws SQLException
	{
		SQLWarning warning = new SQLWarning();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlStatement.getWarnings()).andReturn(warning);
		
		this.control.replay();
		
		SQLWarning result = this.statement.getWarnings();
		
		this.control.verify();
		
		assert result == warning : result;
		
		return result;
	}

	/**
	 * @see java.sql.Statement#setCursorName(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void setCursorName(String name) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);

		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.sqlStatement.setCursorName(name);

		this.lock.unlock();
		
		this.control.replay();
		
		this.statement.setCursorName(name);
		
		this.control.verify();
	}

	@DataProvider(name = "boolean")
	protected Object[][] booleanProvider()
	{
		return new Object[][] { new Object[] { true } };
	}
	
	/**
	 * @see java.sql.Statement#setEscapeProcessing(boolean)
	 */
	@Test(dataProvider = "boolean")
	public void setEscapeProcessing(boolean enable) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlStatement.setEscapeProcessing(enable);
		
		this.control.replay();
		
		this.statement.setEscapeProcessing(enable);
		
		this.control.verify();
	}

	@DataProvider(name = "direction")
	protected Object[][] directionProvider()
	{
		return new Object[][] { new Object[] { ResultSet.FETCH_FORWARD } };
	}

	/**
	 * @see java.sql.Statement#setFetchDirection(int)
	 */
	@Test(dataProvider = "direction")
	public void setFetchDirection(int direction) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlStatement.setFetchDirection(direction);
		
		this.control.replay();
		
		this.statement.setFetchDirection(direction);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.Statement#setFetchSize(int)
	 */
	@Test(dataProvider = "int")
	public void setFetchSize(int rows) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlStatement.setFetchSize(rows);
		
		this.control.replay();
		
		this.statement.setFetchSize(rows);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.Statement#setMaxFieldSize(int)
	 */
	@Test(dataProvider = "int")
	public void setMaxFieldSize(int max) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlStatement.setMaxFieldSize(max);
		
		this.control.replay();
		
		this.statement.setMaxFieldSize(max);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.Statement#setMaxRows(int)
	 */
	@Test(dataProvider = "int")
	public void setMaxRows(int max) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlStatement.setMaxRows(max);

		this.lock.unlock();
		
		this.control.replay();
		
		this.statement.setMaxRows(max);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.Statement#setQueryTimeout(int)
	 */
	@Test(dataProvider = "int")
	public void setQueryTimeout(int seconds) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlStatement.setQueryTimeout(seconds);
		
		this.control.replay();
		
		this.statement.setQueryTimeout(seconds);
		
		this.control.verify();
	}
}
