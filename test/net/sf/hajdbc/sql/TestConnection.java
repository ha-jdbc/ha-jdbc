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

import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.MockDatabase;
import net.sf.hajdbc.Operation;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public class TestConnection implements java.sql.Connection
{
	private IMocksControl control = EasyMock.createStrictControl();
	private Balancer balancer = this.control.createMock(Balancer.class);
	private DatabaseCluster databaseCluster = this.control.createMock(DatabaseCluster.class);
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	private FileSupport fileSupport = this.control.createMock(FileSupport.class);
	private Lock lock = this.control.createMock(Lock.class);
	private LockManager lockManager = this.control.createMock(LockManager.class);
	private java.sql.Connection sqlConnection = this.control.createMock(java.sql.Connection.class);
	private Database database = new MockDatabase();
	private List<Database> databaseList = Collections.singletonList(this.database);
	private Connection<java.sql.Connection> connection;
	
	@SuppressWarnings("unchecked")
	@BeforeClass
	void init() throws Exception
	{
		Map map = Collections.singletonMap(this.database, this.sqlConnection);
		
		EasyMock.expect(this.databaseCluster.getConnectionFactoryMap()).andReturn(map);
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.control.replay();
		
		ConnectionFactory<java.sql.Connection> connectionFactory = new ConnectionFactory<java.sql.Connection>(this.databaseCluster, java.sql.Connection.class);
		
		Operation<java.sql.Connection, java.sql.Connection> operation = new Operation<java.sql.Connection, java.sql.Connection>()
		{
			public java.sql.Connection execute(Database database, java.sql.Connection connection)
			{
				return connection;
			}
		};
		
		this.connection = new Connection<java.sql.Connection>(connectionFactory, operation, this.fileSupport);

		this.control.verify();
		this.control.reset();
	}
	
	@AfterMethod
	void reset()
	{
		this.control.reset();
	}
	
	/**
	 * @see java.sql.Connection#clearWarnings()
	 */
	@Test
	public void clearWarnings() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlConnection.clearWarnings();
			
		this.control.replay();
		
		this.connection.clearWarnings();
		
		this.control.verify();
	}
	
	/**
	 * @see java.sql.Connection#close()
	 */
	@Test
	public void close() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlConnection.close();
		
		this.fileSupport.close();
		
		this.control.replay();
		
		this.connection.close();
		
		this.control.verify();
	}
	
	/**
	 * @see java.sql.Connection#commit()
	 */
	@Test
	public void commit() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlConnection.commit();
		
		this.lock.unlock();
		
		this.control.replay();
		
		this.connection.commit();
		
		this.control.verify();
	}
	
	/**
	 * @see java.sql.Connection#createStatement()
	 */
	@Test
	public Statement createStatement() throws SQLException
	{
		// Read/write connection
		Statement statement = EasyMock.createMock(Statement.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlConnection.createStatement()).andReturn(statement);
		
		this.control.replay();
		
		Statement result = this.connection.createStatement();

		this.control.verify();
		
		assert net.sf.hajdbc.sql.Statement.class.isInstance(result) : result.getClass().getName();
		assert net.sf.hajdbc.sql.Statement.class.cast(result).getObject(this.database) == statement;

		this.control.reset();

		// Read-only connection
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlConnection.createStatement()).andReturn(statement);
		
		this.control.replay();
		
		result = this.connection.createStatement();

		this.control.verify();
		
		assert result == statement;
		
		return result;
	}
	
	@DataProvider(name = "int-int")
	Object[][] intIntProvider()
	{
		return new Object[][] { new Object[] { ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE } };
	}
	
	/**
	 * @see java.sql.Connection#createStatement(int, int)
	 */
	@Test(dataProvider = "int-int")
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
	{
		// Read/write connection
		Statement statement = EasyMock.createMock(Statement.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlConnection.createStatement(resultSetType, resultSetConcurrency)).andReturn(statement);
		
		this.control.replay();
		
		Statement result = this.connection.createStatement(resultSetType, resultSetConcurrency);

		this.control.verify();
		
		assert net.sf.hajdbc.sql.Statement.class.isInstance(result) : result.getClass().getName();
		assert net.sf.hajdbc.sql.Statement.class.cast(result).getObject(this.database) == statement;

		this.control.reset();

		// Read-only connection
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlConnection.createStatement(resultSetType, resultSetConcurrency)).andReturn(statement);
		
		this.control.replay();
		
		result = this.connection.createStatement(resultSetType, resultSetConcurrency);

		this.control.verify();
		
		assert result == statement;
		
		return result;
	}
	
	@DataProvider(name = "int-int-int")
	Object[][] intIntIntProvider()
	{
		return new Object[][] { new Object[] { ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.HOLD_CURSORS_OVER_COMMIT } };
	}
	
	/**
	 * @see java.sql.Connection#createStatement(int, int, int)
	 */
	@Test(dataProvider = "int-int-int")
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
	{
		// Read/write connection
		Statement statement = EasyMock.createMock(Statement.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement);
		
		this.control.replay();
		
		Statement result = this.connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);

		this.control.verify();
		
		assert net.sf.hajdbc.sql.Statement.class.isInstance(result) : result.getClass().getName();
		assert net.sf.hajdbc.sql.Statement.class.cast(result).getObject(this.database) == statement;

		this.control.reset();

		// Read-only connection
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlConnection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement);
		
		this.control.replay();
		
		result = this.connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);

		this.control.verify();
		
		assert result == statement;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#getAutoCommit()
	 */
	@Test
	public boolean getAutoCommit() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlConnection.getAutoCommit()).andReturn(true);
		
		this.control.replay();
		
		boolean autoCommit = this.connection.getAutoCommit();
		
		this.control.verify();
		
		assert autoCommit;
		
		return autoCommit;
	}
	
	/**
	 * @see java.sql.Connection#getCatalog()
	 */
	@Test
	public String getCatalog() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlConnection.getCatalog()).andReturn("catalog");
		
		this.control.replay();
		
		String catalog = this.connection.getCatalog();
		
		this.control.verify();
		
		assert catalog.equals("catalog") : catalog;
		
		return catalog;
	}
	
	/**
	 * @see java.sql.Connection#getHoldability()
	 */
	@Test
	public int getHoldability() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlConnection.getHoldability()).andReturn(ResultSet.HOLD_CURSORS_OVER_COMMIT);
		
		this.control.replay();
		
		int holdability = this.connection.getHoldability();
		
		this.control.verify();
		
		assert holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT : holdability;
		
		return holdability;
	}
	
	/**
	 * @see java.sql.Connection#getMetaData()
	 */
	@Test
	public DatabaseMetaData getMetaData() throws SQLException
	{
		DatabaseMetaData metaData = EasyMock.createMock(DatabaseMetaData.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);

		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlConnection.getMetaData()).andReturn(metaData);
		
		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		DatabaseMetaData result = this.connection.getMetaData();
		
		this.control.verify();
		
		assert result == metaData;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#getTransactionIsolation()
	 */
	@Test
	public int getTransactionIsolation() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlConnection.getTransactionIsolation()).andReturn(java.sql.Connection.TRANSACTION_NONE);

		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		int isolation = this.connection.getTransactionIsolation();
		
		this.control.verify();
		
		assert isolation == java.sql.Connection.TRANSACTION_NONE : isolation;
		
		return isolation;
	}
	
	/**
	 * @see java.sql.Connection#getTypeMap()
	 */
	@Test
	public Map<String, Class<?>> getTypeMap() throws SQLException
	{
		Map<String, Class<?>> map = Collections.emptyMap();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlConnection.getTypeMap()).andReturn(map);
		
		this.control.replay();
		
		Map<String, Class<?>> result = this.connection.getTypeMap();
		
		this.control.verify();
		
		assert result == map;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#getWarnings()
	 */
	@Test
	public SQLWarning getWarnings() throws SQLException
	{
		SQLWarning warning = new SQLWarning();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlConnection.getWarnings()).andReturn(warning);
		
		this.control.replay();
		
		SQLWarning result = this.connection.getWarnings();
		
		this.control.verify();
		
		assert result == warning;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#isClosed()
	 */
	@Test
	public boolean isClosed() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlConnection.isClosed()).andReturn(true);
		
		this.control.replay();
		
		boolean closed = this.connection.isClosed();
		
		this.control.verify();
		
		assert closed;
		
		return closed;
	}
	
	/**
	 * @see java.sql.Connection#isReadOnly()
	 */
	@Test
	public boolean isReadOnly() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
		
		this.control.replay();
		
		boolean readOnly = this.connection.isReadOnly();
		
		this.control.verify();
		
		assert readOnly;
		
		return readOnly;
	}
	
	@DataProvider(name = "string")
	Object[][] stringProvider()
	{
		return new Object[][] { new Object[] { "sql" } };
	}
	
	/**
	 * @see java.sql.Connection#nativeSQL(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public String nativeSQL(String sql) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlConnection.nativeSQL(sql)).andReturn("native-sql");
		
		this.control.replay();
		
		String nativeSQL = this.connection.nativeSQL(sql);
		
		this.control.verify();
		
		assert nativeSQL.equals("native-sql") : nativeSQL;
		
		return nativeSQL;
	}
	
	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public CallableStatement prepareCall(String sql) throws SQLException
	{
		CallableStatement statement = EasyMock.createMock(CallableStatement.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlConnection.prepareCall(sql)).andReturn(statement);
		
		this.control.replay();
		
		CallableStatement result = this.connection.prepareCall(sql);

		this.control.verify();
		
		assert net.sf.hajdbc.sql.CallableStatement.class.isInstance(result) : result.getClass().getName();
		assert net.sf.hajdbc.sql.CallableStatement.class.cast(result).getObject(this.database) == statement;

		this.control.reset();

		// Read-only connection
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlConnection.prepareCall(sql)).andReturn(statement);

		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		result = this.connection.prepareCall(sql);

		this.control.verify();
		
		assert result == statement;
		
		return result;
	}
	
	@DataProvider(name = "string-int-int")
	Object[][] stringIntIntProvider()
	{
		return new Object[][] { new Object[] { "sql", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY } };
	}
	
	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
	 */
	@Test(dataProvider = "string-int-int")
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
	{
		CallableStatement statement = EasyMock.createMock(CallableStatement.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlConnection.prepareCall(sql, resultSetType, resultSetConcurrency)).andReturn(statement);
		
		this.control.replay();
		
		CallableStatement result = this.connection.prepareCall(sql, resultSetType, resultSetConcurrency);

		this.control.verify();
		
		assert net.sf.hajdbc.sql.CallableStatement.class.isInstance(result) : result.getClass().getName();
		assert net.sf.hajdbc.sql.CallableStatement.class.cast(result).getObject(this.database) == statement;

		this.control.reset();

		// Read-only connection
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlConnection.prepareCall(sql, resultSetType, resultSetConcurrency)).andReturn(statement);

		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		result = this.connection.prepareCall(sql, resultSetType, resultSetConcurrency);

		this.control.verify();
		
		assert result == statement;
		
		return result;
	}
	
	@DataProvider(name = "string-int-int-int")
	Object[][] stringIntIntIntProvider()
	{
		return new Object[][] { new Object[] { "sql", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT } };
	}
	
	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
	 */
	@Test(dataProvider = "string-int-int-int")
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
	{
		CallableStatement statement = EasyMock.createMock(CallableStatement.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement);
		
		this.control.replay();
		
		CallableStatement result = this.connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);

		this.control.verify();
		
		assert net.sf.hajdbc.sql.CallableStatement.class.isInstance(result) : result.getClass().getName();
		assert net.sf.hajdbc.sql.CallableStatement.class.cast(result).getObject(this.database) == statement;

		this.control.reset();

		// Read-only connection
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlConnection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement);

		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		result = this.connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);

		this.control.verify();
		
		assert result == statement;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public PreparedStatement prepareStatement(String sql) throws SQLException
	{
		PreparedStatement statement = EasyMock.createMock(PreparedStatement.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlConnection.prepareStatement(sql)).andReturn(statement);
		
		this.control.replay();
		
		PreparedStatement result = this.connection.prepareStatement(sql);

		this.control.verify();
		
		assert net.sf.hajdbc.sql.PreparedStatement.class.isInstance(result) : result.getClass().getName();
		assert net.sf.hajdbc.sql.PreparedStatement.class.cast(result).getObject(this.database) == statement;

		this.control.reset();

		// Read-only connection
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlConnection.prepareStatement(sql)).andReturn(statement);

		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		result = this.connection.prepareStatement(sql);

		this.control.verify();
		
		assert result == statement;
		
		return result;
	}
	
	@DataProvider(name = "string-int")
	Object[][] stringIntProvider()
	{
		return new Object[][] { new Object[] { "sql", Statement.NO_GENERATED_KEYS } };
	}
	
	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
	{
		PreparedStatement statement = EasyMock.createMock(PreparedStatement.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlConnection.prepareStatement(sql, autoGeneratedKeys)).andReturn(statement);
		
		this.control.replay();
		
		PreparedStatement result = this.connection.prepareStatement(sql, autoGeneratedKeys);

		this.control.verify();
		
		assert net.sf.hajdbc.sql.PreparedStatement.class.isInstance(result) : result.getClass().getName();
		assert net.sf.hajdbc.sql.PreparedStatement.class.cast(result).getObject(this.database) == statement;

		this.control.reset();

		// Read-only connection
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlConnection.prepareStatement(sql, autoGeneratedKeys)).andReturn(statement);

		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		result = this.connection.prepareStatement(sql, autoGeneratedKeys);

		this.control.verify();
		
		assert result == statement;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
	 */
	@Test(dataProvider = "string-int-int")
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
	{
		PreparedStatement statement = EasyMock.createMock(PreparedStatement.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlConnection.prepareStatement(sql, resultSetType, resultSetConcurrency)).andReturn(statement);
		
		this.control.replay();
		
		PreparedStatement result = this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency);

		this.control.verify();
		
		assert net.sf.hajdbc.sql.PreparedStatement.class.isInstance(result) : result.getClass().getName();
		assert net.sf.hajdbc.sql.PreparedStatement.class.cast(result).getObject(this.database) == statement;

		this.control.reset();

		// Read-only connection
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlConnection.prepareStatement(sql, resultSetType, resultSetConcurrency)).andReturn(statement);

		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		result = this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency);

		this.control.verify();
		
		assert result == statement;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
	 */
	@Test(dataProvider = "string-int-int-int")
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
	{
		PreparedStatement statement = EasyMock.createMock(PreparedStatement.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement);
		
		this.control.replay();
		
		PreparedStatement result = this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);

		this.control.verify();
		
		assert net.sf.hajdbc.sql.PreparedStatement.class.isInstance(result) : result.getClass().getName();
		assert net.sf.hajdbc.sql.PreparedStatement.class.cast(result).getObject(this.database) == statement;

		this.control.reset();

		// Read-only connection
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlConnection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement);

		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		result = this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);

		this.control.verify();
		
		assert result == statement;
		
		return result;
	}
	
	@DataProvider(name = "string-ints")
	Object[][] stringIntsProvider()
	{
		return new Object[][] { new Object[] { "sql", new int[] { 1 } } };
	}
	
	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
	 */
	@Test(dataProvider = "string-ints")
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
	{
		PreparedStatement statement = EasyMock.createMock(PreparedStatement.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlConnection.prepareStatement(sql, columnIndexes)).andReturn(statement);

		this.control.replay();
		
		PreparedStatement result = this.connection.prepareStatement(sql, columnIndexes);

		this.control.verify();
		
		assert net.sf.hajdbc.sql.PreparedStatement.class.isInstance(result) : result.getClass().getName();
		assert net.sf.hajdbc.sql.PreparedStatement.class.cast(result).getObject(this.database) == statement;

		this.control.reset();

		// Read-only connection
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlConnection.prepareStatement(sql, columnIndexes)).andReturn(statement);

		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		result = this.connection.prepareStatement(sql, columnIndexes);

		this.control.verify();
		
		assert result == statement;
		
		return result;
	}
	
	@DataProvider(name = "string-strings")
	Object[][] stringStringsProvider()
	{
		return new Object[][] { new Object[] { "sql", new String[] { "col1" } } };
	}
		
	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, java.lang.String[])
	 */
	@Test(dataProvider = "string-strings")
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
	{
		PreparedStatement statement = EasyMock.createMock(PreparedStatement.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlConnection.prepareStatement(sql, columnNames)).andReturn(statement);
		
		this.control.replay();
		
		PreparedStatement result = this.connection.prepareStatement(sql, columnNames);

		this.control.verify();
		
		assert net.sf.hajdbc.sql.PreparedStatement.class.isInstance(result) : result.getClass().getName();
		assert net.sf.hajdbc.sql.PreparedStatement.class.cast(result).getObject(this.database) == statement;

		this.control.reset();

		// Read-only connection
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlConnection.prepareStatement(sql, columnNames)).andReturn(statement);

		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		result = this.connection.prepareStatement(sql, columnNames);

		this.control.verify();
		
		assert result == statement;
		
		return result;
	}
	
	@DataProvider(name = "savepoint")
	Object[][] savepointProvider() throws SQLException
	{
		final java.sql.Savepoint sqlSavepoint = EasyMock.createMock(java.sql.Savepoint.class);
		
		Operation<java.sql.Connection, java.sql.Savepoint> operation = new Operation<java.sql.Connection, java.sql.Savepoint>()
		{
			public java.sql.Savepoint execute(Database database, java.sql.Connection connection)
			{
				return sqlSavepoint;
			}
		};
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.lock.unlock();
		
		this.control.replay();
		
		Savepoint savepoint = new Savepoint(this.connection, operation);
			
		this.control.verify();
		this.control.reset();
		
		return new Object[][] { new Object[] { savepoint } };
	}
	
	/**
	 * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
	 */
	@Test(dataProvider = "savepoint")
	public void releaseSavepoint(java.sql.Savepoint savepoint) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlConnection.releaseSavepoint(savepoint);
		
		this.lock.unlock();
		
		this.control.replay();
		
		this.connection.releaseSavepoint(savepoint);
		
		this.control.verify();
	}
	
	/**
	 * @see java.sql.Connection#rollback()
	 */
	@Test
	public void rollback() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlConnection.rollback();
		
		this.lock.unlock();
		
		this.control.replay();
		
		this.connection.rollback();
		
		this.control.verify();
	}
	
	/**
	 * @see java.sql.Connection#rollback(java.sql.Savepoint)
	 */
	@Test(dataProvider = "savepoint")
	public void rollback(java.sql.Savepoint savepoint) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlConnection.rollback(savepoint);
		
		this.lock.unlock();
		
		this.control.replay();
		
		this.connection.rollback(savepoint);
		
		this.control.verify();
	}
	
	@DataProvider(name = "boolean")
	Object[][] booleanProvider()
	{
		return new Object[][] { new Object[] { true } };
	}
	
	/**
	 * @see java.sql.Connection#setAutoCommit(boolean)
	 */
	@Test(dataProvider = "boolean")
	public void setAutoCommit(boolean autoCommit) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlConnection.setAutoCommit(autoCommit);
		
		this.control.replay();
		
		this.connection.setAutoCommit(autoCommit);
		
		this.control.verify();
	}
	
	/**
	 * @see java.sql.Connection#setCatalog(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void setCatalog(String catalog) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlConnection.setCatalog(catalog);
		
		this.control.replay();
		
		this.connection.setCatalog(catalog);
		
		this.control.verify();
	}
	
	@DataProvider(name = "holdability")
	Object[][] holdabilityProvider()
	{
		return new Object[][] { new Object[] { ResultSet.HOLD_CURSORS_OVER_COMMIT } };
	}
	
	/**
	 * @see java.sql.Connection#setHoldability(int)
	 */
	@Test(dataProvider = "holdability")
	public void setHoldability(int holdability) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlConnection.setHoldability(holdability);
		
		this.control.replay();
		
		this.connection.setHoldability(holdability);
		
		this.control.verify();
	}
	
	/**
	 * @see java.sql.Connection#setReadOnly(boolean)
	 */
	@Test(dataProvider = "boolean")
	public void setReadOnly(boolean readOnly) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlConnection.setReadOnly(readOnly);
		
		this.control.replay();
		
		this.connection.setReadOnly(readOnly);
		
		this.control.verify();
	}
	
	/**
	 * @see java.sql.Connection#setSavepoint()
	 */
	@Test
	public java.sql.Savepoint setSavepoint() throws SQLException
	{
		java.sql.Savepoint savepoint = EasyMock.createMock(java.sql.Savepoint.class);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlConnection.setSavepoint()).andReturn(savepoint);
		
		this.lock.unlock();
		
		this.control.replay();
		
		java.sql.Savepoint result = this.connection.setSavepoint();
		
		this.control.verify();

		assert Savepoint.class.isInstance(result) : result.getClass().getName();
		assert Savepoint.class.cast(result).getObject(this.database) == savepoint;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#setSavepoint(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public java.sql.Savepoint setSavepoint(String name) throws SQLException
	{
		java.sql.Savepoint savepoint = EasyMock.createMock(java.sql.Savepoint.class);
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlConnection.setSavepoint(name)).andReturn(savepoint);
		
		this.lock.unlock();
		
		this.control.replay();
		
		java.sql.Savepoint result = this.connection.setSavepoint(name);
		
		this.control.verify();

		assert Savepoint.class.isInstance(result) : result.getClass().getName();
		assert Savepoint.class.cast(result).getObject(this.database) == savepoint;
		
		return result;
	}
	
	@DataProvider(name = "isolation")
	Object[][] isolationProvider()
	{
		return new Object[][] { new Object[] { java.sql.Connection.TRANSACTION_NONE } };
	}
	
	/**
	 * @see java.sql.Connection#setTransactionIsolation(int)
	 */
	@Test(dataProvider = "isolation")
	public void setTransactionIsolation(int level) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlConnection.setTransactionIsolation(level);
		
		this.control.replay();
		
		this.connection.setTransactionIsolation(level);
		
		this.control.verify();
	}
	
	@DataProvider(name = "map")
	Object[][] mapProvider()
	{
		return new Object[][] { new Object[] { Collections.EMPTY_MAP } };
	}
	
	/**
	 * @see java.sql.Connection#setTypeMap(java.util.Map)
	 */
	@Test(dataProvider = "map")
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlConnection.setTypeMap(map);
		
		this.control.replay();
		
		this.connection.setTypeMap(map);
		
		this.control.verify();
	}
}
