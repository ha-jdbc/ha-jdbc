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
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
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

@SuppressWarnings({ "unchecked", "nls" })
public class TestConnection implements Connection
{
	private Balancer balancer = EasyMock.createStrictMock(Balancer.class);
	private DatabaseCluster cluster = EasyMock.createStrictMock(DatabaseCluster.class);
	private FileSupport fileSupport = EasyMock.createStrictMock(FileSupport.class);
	private Lock readLock = EasyMock.createStrictMock(Lock.class);
	private Lock writeLock1 = EasyMock.createStrictMock(Lock.class);
	private Lock writeLock2 = EasyMock.createStrictMock(Lock.class);
	private LockManager lockManager = EasyMock.createStrictMock(LockManager.class);
	private Dialect dialect = EasyMock.createStrictMock(Dialect.class);
	private DatabaseMetaDataCache metaData = EasyMock.createStrictMock(DatabaseMetaDataCache.class);
	private DatabaseProperties databaseProperties = EasyMock.createStrictMock(DatabaseProperties.class);
	private TableProperties tableProperties = EasyMock.createStrictMock(TableProperties.class);
	private ColumnProperties columnProperties = EasyMock.createStrictMock(ColumnProperties.class);
	private Connection connection1 = EasyMock.createStrictMock(java.sql.Connection.class);
	private Connection connection2 = EasyMock.createStrictMock(java.sql.Connection.class);
	private SQLProxy parent = EasyMock.createStrictMock(SQLProxy.class);
	private SQLProxy root = EasyMock.createStrictMock(SQLProxy.class);
	private Savepoint savepoint1 = EasyMock.createStrictMock(Savepoint.class);
	private Savepoint savepoint2 = EasyMock.createStrictMock(Savepoint.class);
	
	private Database database1 = new MockDatabase("1");
	private Database database2 = new MockDatabase("2");
	private Set<Database> databaseSet;
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	private Connection connection;
	private ConnectionInvocationHandler handler;
	
	@BeforeClass
	void init() throws Exception
	{
		Map<Database, Connection> map = new TreeMap<Database, Connection>();
		map.put(this.database1, this.connection1);
		map.put(this.database2, this.connection2);
		
		this.databaseSet = map.keySet();
		
		EasyMock.expect(this.parent.getDatabaseCluster()).andReturn(this.cluster);

		this.parent.addChild(EasyMock.isA(ConnectionInvocationHandler.class));

		this.replay();
		
		this.handler = new ConnectionInvocationHandler(new Object(), this.parent, EasyMock.createMock(Invoker.class), map, this.fileSupport);
		this.connection = ProxyFactory.createProxy(Connection.class, this.handler);
		
		this.verify();
		this.reset();
	}
	
	private Object[] objects()
	{
		return new Object[] { this.cluster, this.balancer, this.connection1, this.connection2, this.fileSupport, this.readLock, this.writeLock1, this.writeLock2, this.lockManager, this.parent, this.root, this.savepoint1, this.savepoint2, this.dialect, this.metaData, this.databaseProperties, this.tableProperties, this.columnProperties };
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
	
	/**
	 * @see java.sql.Connection#clearWarnings()
	 */
	@Test
	public void clearWarnings() throws SQLException
	{
		this.connection1.clearWarnings();
		this.connection2.clearWarnings();
		
		this.replay();
		
		this.connection.clearWarnings();
		
		this.verify();
	}
	
	/**
	 * @see java.sql.Connection#close()
	 */
	@Test
	public void close() throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		this.connection1.close();
		this.connection2.close();
		
		this.fileSupport.close();

		this.parent.removeChild(this.handler);
		
		this.replay();
		
		this.connection.close();
		
		this.verify();
	}
	
	/**
	 * @see java.sql.Connection#commit()
	 */
	@Test
	public void commit() throws SQLException
	{
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.connection1.commit();
		this.connection2.commit();
		
		this.readLock.unlock();
		
		this.replay();
		
		this.connection.commit();
		
		this.verify();
	}
	
	/**
	 * @see java.sql.Connection#createStatement()
	 */
	@SuppressWarnings("unchecked")
	@Test
	public Statement createStatement() throws SQLException
	{
		// Read/write connection
		Statement statement1 = EasyMock.createMock(Statement.class);
		Statement statement2 = EasyMock.createMock(Statement.class);
		
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.connection1.createStatement()).andReturn(statement1);
		EasyMock.expect(this.connection2.createStatement()).andReturn(statement2);
		
		this.replay();
		
		Statement result = this.connection.createStatement();

		this.verify();
		
		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == statement1;
		assert proxy.getObject(this.database2) == statement2;

		this.reset();
		
		// Read-only connection
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.connection1.createStatement()).andReturn(statement1);
		
		this.replay();
		
		result = this.connection.createStatement();

		this.verify();
		
		assert result == statement1;
		
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
	@SuppressWarnings("unchecked")
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
	{
		// Read/write connection
		Statement statement1 = EasyMock.createMock(Statement.class);
		Statement statement2 = EasyMock.createMock(Statement.class);
		
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.connection1.createStatement(resultSetType, resultSetConcurrency)).andReturn(statement1);
		EasyMock.expect(this.connection2.createStatement(resultSetType, resultSetConcurrency)).andReturn(statement2);
		
		this.replay();
		
		Statement result = this.connection.createStatement(resultSetType, resultSetConcurrency);

		this.verify();
		
		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == statement1;
		assert proxy.getObject(this.database2) == statement2;

		this.reset();
		
		// Read-only connection
		EasyMock.expect(this.connection.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.connection1.createStatement(resultSetType, resultSetConcurrency)).andReturn(statement1);
		
		this.replay();
		
		result = this.connection.createStatement(resultSetType, resultSetConcurrency);

		this.verify();
		
		assert result == statement1;
		
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
	@SuppressWarnings("unchecked")
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
	{
		// Read/write connection
		Statement statement1 = EasyMock.createMock(Statement.class);
		Statement statement2 = EasyMock.createMock(Statement.class);
		
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.connection1.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement1);
		EasyMock.expect(this.connection2.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement2);
		
		this.replay();
		
		Statement result = this.connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);

		this.verify();
		
		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == statement1;
		assert proxy.getObject(this.database2) == statement2;

		this.reset();
		
		// Read-only connection
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.connection1.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement1);
		
		this.replay();
		
		result = this.connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);

		this.verify();
		
		assert result == statement1;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#getAutoCommit()
	 */
	@Test
	public boolean getAutoCommit() throws SQLException
	{
		EasyMock.expect(this.connection1.getAutoCommit()).andReturn(true);
		
		this.replay();
		
		boolean autoCommit = this.connection.getAutoCommit();
		
		this.verify();
		
		assert autoCommit;
		
		return autoCommit;
	}
	
	/**
	 * @see java.sql.Connection#getCatalog()
	 */
	@Test
	public String getCatalog() throws SQLException
	{
		EasyMock.expect(this.connection1.getCatalog()).andReturn("catalog");
		
		this.replay();
		
		String catalog = this.connection.getCatalog();
		
		this.verify();
		
		assert catalog.equals("catalog") : catalog;
		
		return catalog;
	}
	
	/**
	 * @see java.sql.Connection#getHoldability()
	 */
	@Test
	public int getHoldability() throws SQLException
	{
		EasyMock.expect(this.connection1.getHoldability()).andReturn(ResultSet.HOLD_CURSORS_OVER_COMMIT);
		
		this.replay();
		
		int holdability = this.connection.getHoldability();
		
		this.verify();
		
		assert holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT : holdability;
		
		return holdability;
	}
	
	/**
	 * @see java.sql.Connection#getMetaData()
	 */
	@Test
	@SuppressWarnings("unchecked")
	public DatabaseMetaData getMetaData() throws SQLException
	{
		DatabaseMetaData metaData = EasyMock.createMock(DatabaseMetaData.class);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);

		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.connection2.getMetaData()).andReturn(metaData);
		
		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		DatabaseMetaData result = this.connection.getMetaData();
		
		this.verify();
		
		assert result == metaData;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#getTransactionIsolation()
	 */
	@Test
	@SuppressWarnings("unchecked")
	public int getTransactionIsolation() throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.connection2.getTransactionIsolation()).andReturn(java.sql.Connection.TRANSACTION_NONE);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		int isolation = this.connection.getTransactionIsolation();
		
		this.verify();
		
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
		
		EasyMock.expect(this.connection1.getTypeMap()).andReturn(map);
		
		this.replay();
		
		Map<String, Class<?>> result = this.connection.getTypeMap();
		
		this.verify();
		
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
		
		EasyMock.expect(this.connection1.getWarnings()).andReturn(warning);
		
		this.replay();
		
		SQLWarning result = this.connection.getWarnings();
		
		this.verify();
		
		assert result == warning;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#isClosed()
	 */
	@Test
	public boolean isClosed() throws SQLException
	{
		EasyMock.expect(this.connection1.isClosed()).andReturn(true);
		
		this.replay();
		
		boolean closed = this.connection.isClosed();
		
		this.verify();
		
		assert closed;
		
		return closed;
	}
	
	/**
	 * @see java.sql.Connection#isReadOnly()
	 */
	@Test
	public boolean isReadOnly() throws SQLException
	{
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(true);
		
		this.replay();
		
		boolean readOnly = this.connection.isReadOnly();
		
		this.verify();
		
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
		EasyMock.expect(this.connection1.nativeSQL(sql)).andReturn("native-sql");
		
		this.replay();
		
		String nativeSQL = this.connection.nativeSQL(sql);
		
		this.verify();
		
		assert nativeSQL.equals("native-sql") : nativeSQL;
		
		return nativeSQL;
	}
	
	/**
	 * @see java.sql.Connection#prepareCall(java.lang.String)
	 */
	@Test(dataProvider = "string")
	@SuppressWarnings("unchecked")
	public CallableStatement prepareCall(String sql) throws SQLException
	{
		CallableStatement statement1 = EasyMock.createMock(CallableStatement.class);
		CallableStatement statement2 = EasyMock.createMock(CallableStatement.class);
		
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.connection1.prepareCall(sql)).andReturn(statement1);
		EasyMock.expect(this.connection2.prepareCall(sql)).andReturn(statement2);

		this.getLockList(sql);
		
		this.replay();
		
		CallableStatement result = this.connection.prepareCall(sql);

		this.verify();
		
		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == statement1;
		assert proxy.getObject(this.database2) == statement2;

		this.reset();
		
		// Read-only connection
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.connection2.prepareCall(sql)).andReturn(statement2);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		result = this.connection.prepareCall(sql);

		this.verify();
		
		assert result == statement2;
		
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
	@SuppressWarnings("unchecked")
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
	{
		CallableStatement statement1 = EasyMock.createMock(CallableStatement.class);
		CallableStatement statement2 = EasyMock.createMock(CallableStatement.class);
		
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.connection1.prepareCall(sql, resultSetType, resultSetConcurrency)).andReturn(statement1);
		EasyMock.expect(this.connection2.prepareCall(sql, resultSetType, resultSetConcurrency)).andReturn(statement2);
		
		this.getLockList(sql);
		
		this.replay();
		
		CallableStatement result = this.connection.prepareCall(sql, resultSetType, resultSetConcurrency);

		this.verify();
		
		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == statement1;
		assert proxy.getObject(this.database2) == statement2;

		this.reset();
		
		// Read-only connection
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.connection2.prepareCall(sql, resultSetType, resultSetConcurrency)).andReturn(statement2);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		result = this.connection.prepareCall(sql, resultSetType, resultSetConcurrency);

		this.verify();
		
		assert result == statement2;
		
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
	@SuppressWarnings("unchecked")
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
	{
		CallableStatement statement1 = EasyMock.createMock(CallableStatement.class);
		CallableStatement statement2 = EasyMock.createMock(CallableStatement.class);
		
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.connection1.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement1);
		EasyMock.expect(this.connection2.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement2);

		this.getLockList(sql);
		
		this.replay();
		
		CallableStatement result = this.connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);

		this.verify();
		
		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == statement1;
		assert proxy.getObject(this.database2) == statement2;

		this.reset();
		
		// Read-only connection
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.connection2.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement2);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		result = this.connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);

		this.verify();
		
		assert result == statement2;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String)
	 */
	@Test(dataProvider = "string")
	@SuppressWarnings("unchecked")
	public PreparedStatement prepareStatement(String sql) throws SQLException
	{
		PreparedStatement statement1 = EasyMock.createMock(PreparedStatement.class);
		PreparedStatement statement2 = EasyMock.createMock(PreparedStatement.class);
		
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);		
		
		EasyMock.expect(this.connection1.prepareStatement(sql)).andReturn(statement1);
		EasyMock.expect(this.connection2.prepareStatement(sql)).andReturn(statement2);
		
		this.getLockList(sql);
		
		this.replay();
		
		PreparedStatement result = this.connection.prepareStatement(sql);

		this.verify();
		
		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == statement1;
		assert proxy.getObject(this.database2) == statement2;

		this.reset();
		
		// Read-only connection
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.connection2.prepareStatement(sql)).andReturn(statement2);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		result = this.connection.prepareStatement(sql);

		this.verify();
		
		assert result == statement2;
		
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
	@SuppressWarnings("unchecked")
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
	{
		PreparedStatement statement1 = EasyMock.createMock(PreparedStatement.class);
		PreparedStatement statement2 = EasyMock.createMock(PreparedStatement.class);
		
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.connection1.prepareStatement(sql, autoGeneratedKeys)).andReturn(statement1);
		EasyMock.expect(this.connection2.prepareStatement(sql, autoGeneratedKeys)).andReturn(statement2);

		this.getLockList(sql);
		
		this.replay();
		
		PreparedStatement result = this.connection.prepareStatement(sql, autoGeneratedKeys);

		this.verify();
		
		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == statement1;
		assert proxy.getObject(this.database2) == statement2;

		this.reset();
		
		// Read-only connection
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.connection2.prepareStatement(sql, autoGeneratedKeys)).andReturn(statement2);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		result = this.connection.prepareStatement(sql, autoGeneratedKeys);

		this.verify();
		
		assert result == statement2;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
	 */
	@Test(dataProvider = "string-int-int")
	@SuppressWarnings("unchecked")
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
	{
		PreparedStatement statement1 = EasyMock.createMock(PreparedStatement.class);
		PreparedStatement statement2 = EasyMock.createMock(PreparedStatement.class);
		
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.connection1.prepareStatement(sql, resultSetType, resultSetConcurrency)).andReturn(statement1);
		EasyMock.expect(this.connection2.prepareStatement(sql, resultSetType, resultSetConcurrency)).andReturn(statement2);

		this.getLockList(sql);
		
		this.replay();
		
		PreparedStatement result = this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency);

		this.verify();
		
		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == statement1;
		assert proxy.getObject(this.database2) == statement2;

		this.reset();
		
		// Read-only connection
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.connection2.prepareStatement(sql, resultSetType, resultSetConcurrency)).andReturn(statement2);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		result = this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency);

		this.verify();
		
		assert result == statement2;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
	 */
	@Test(dataProvider = "string-int-int-int")
	@SuppressWarnings("unchecked")
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
	{
		PreparedStatement statement1 = EasyMock.createMock(PreparedStatement.class);
		PreparedStatement statement2 = EasyMock.createMock(PreparedStatement.class);
		
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.connection1.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement1);
		EasyMock.expect(this.connection2.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement2);
		
		this.getLockList(sql);
		
		this.replay();
		
		PreparedStatement result = this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);

		this.verify();
		
		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == statement1;
		assert proxy.getObject(this.database2) == statement2;

		this.reset();
		
		// Read-only connection
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.connection2.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability)).andReturn(statement2);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		result = this.connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);

		this.verify();
		
		assert result == statement2;
		
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
	@SuppressWarnings("unchecked")
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
	{
		PreparedStatement statement1 = EasyMock.createMock(PreparedStatement.class);
		PreparedStatement statement2 = EasyMock.createMock(PreparedStatement.class);
		
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.connection1.prepareStatement(sql, columnIndexes)).andReturn(statement1);
		EasyMock.expect(this.connection2.prepareStatement(sql, columnIndexes)).andReturn(statement2);

		this.getLockList(sql);
		
		this.replay();
		
		PreparedStatement result = this.connection.prepareStatement(sql, columnIndexes);

		this.verify();
		
		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == statement1;
		assert proxy.getObject(this.database2) == statement2;

		this.reset();
		
		// Read-only connection
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.connection2.prepareStatement(sql, columnIndexes)).andReturn(statement2);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		result = this.connection.prepareStatement(sql, columnIndexes);

		this.verify();
		
		assert result == statement2;
		
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
	@SuppressWarnings("unchecked")
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
	{
		PreparedStatement statement1 = EasyMock.createMock(PreparedStatement.class);
		PreparedStatement statement2 = EasyMock.createMock(PreparedStatement.class);
		
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(false);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.connection1.prepareStatement(sql, columnNames)).andReturn(statement1);
		EasyMock.expect(this.connection2.prepareStatement(sql, columnNames)).andReturn(statement2);

		this.getLockList(sql);
		
		this.replay();
		
		PreparedStatement result = this.connection.prepareStatement(sql, columnNames);

		this.verify();
		
		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == statement1;
		assert proxy.getObject(this.database2) == statement2;

		this.reset();
		
		// Read-only connection
		EasyMock.expect(this.connection1.isReadOnly()).andReturn(true);
		
		EasyMock.expect(this.cluster.isCurrentTimestampEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentDateEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isCurrentTimeEvaluationEnabled()).andReturn(false);
		EasyMock.expect(this.cluster.isRandomEvaluationEnabled()).andReturn(false);
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.connection2.prepareStatement(sql, columnNames)).andReturn(statement2);

		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		result = this.connection.prepareStatement(sql, columnNames);

		this.verify();
		
		assert result == statement2;
		
		return result;
	}
	
	@DataProvider(name = "savepoint")
	Object[][] savepointProvider() throws Exception
	{
		Map<Database, Savepoint> map = new TreeMap<Database, Savepoint>();
		map.put(this.database1, this.savepoint1);
		map.put(this.database2, this.savepoint2);
		
		return new Object[][] { new Object[] { ProxyFactory.createProxy(Savepoint.class, new SavepointInvocationHandler(this.connection, this.handler, EasyMock.createMock(Invoker.class), map)) } };
	}
	
	/**
	 * @see java.sql.Connection#releaseSavepoint(Savepoint)
	 */
	@Test(dataProvider = "savepoint")
	public void releaseSavepoint(Savepoint savepoint) throws SQLException
	{
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.connection1.releaseSavepoint(this.savepoint1);
		this.connection2.releaseSavepoint(this.savepoint2);
		
		this.readLock.unlock();
		
		this.replay();
		
		this.connection.releaseSavepoint(savepoint);
		
		this.verify();
	}
	
	/**
	 * @see java.sql.Connection#rollback()
	 */
	@Test
	public void rollback() throws SQLException
	{
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.connection1.rollback();
		this.connection2.rollback();
		
		this.readLock.unlock();
		
		this.replay();
		
		this.connection.rollback();
		
		this.verify();
	}
	
	/**
	 * @see java.sql.Connection#rollback(Savepoint)
	 */
	@Test(dataProvider = "savepoint")
	public void rollback(Savepoint savepoint) throws SQLException
	{
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		this.connection1.rollback(this.savepoint1);
		this.connection2.rollback(this.savepoint2);
		
		this.readLock.unlock();
		
		this.replay();
		
		this.connection.rollback(savepoint);
		
		this.verify();
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
		this.connection1.setAutoCommit(autoCommit);
		this.connection2.setAutoCommit(autoCommit);
		
		this.replay();
		
		this.connection.setAutoCommit(autoCommit);
		
		this.verify();
	}
	
	/**
	 * @see java.sql.Connection#setCatalog(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void setCatalog(String catalog) throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		this.connection1.setCatalog(catalog);
		this.connection2.setCatalog(catalog);
		
		this.replay();
		
		this.connection.setCatalog(catalog);
		
		this.verify();
	}
	
	@DataProvider(name = "holdability")
	Object[][] holdabilityProvider()
	{
		return new Object[][] { new Object[] { ResultSet.HOLD_CURSORS_OVER_COMMIT } };
	}
	
	/**
	 * @see java.sql.Connection#setHoldability(int)
	 */
	@Test(dataProvider = "int")
	public void setHoldability(int holdability) throws SQLException
	{
		this.connection1.setHoldability(holdability);
		this.connection2.setHoldability(holdability);
		
		this.replay();
		
		this.connection.setHoldability(holdability);
		
		this.verify();
	}
	
	/**
	 * @see java.sql.Connection#setReadOnly(boolean)
	 */
	@Test(dataProvider = "boolean")
	public void setReadOnly(boolean readOnly) throws SQLException
	{
		this.connection1.setReadOnly(readOnly);
		this.connection2.setReadOnly(readOnly);
		
		this.replay();
		
		this.connection.setReadOnly(readOnly);
		
		this.verify();
	}
	
	/**
	 * @see java.sql.Connection#setSavepoint()
	 */
	@Test
	@SuppressWarnings("unchecked")
	public Savepoint setSavepoint() throws SQLException
	{
		Savepoint savepoint1 = EasyMock.createMock(Savepoint.class);
		Savepoint savepoint2 = EasyMock.createMock(Savepoint.class);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);

		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.connection1.setSavepoint()).andReturn(savepoint1);
		EasyMock.expect(this.connection2.setSavepoint()).andReturn(savepoint2);
		
		this.readLock.unlock();
		
		this.replay();
		
		Savepoint result = this.connection.setSavepoint();
		
		this.verify();

		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == savepoint1;
		assert proxy.getObject(this.database2) == savepoint2;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#setSavepoint(java.lang.String)
	 */
	@Test(dataProvider = "string")
	@SuppressWarnings("unchecked")
	public Savepoint setSavepoint(String name) throws SQLException
	{
		Savepoint savepoint1 = EasyMock.createMock(Savepoint.class);
		Savepoint savepoint2 = EasyMock.createMock(Savepoint.class);
		
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.readLock);
		
		this.readLock.lock();
		
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getTransactionalExecutor()).andReturn(this.executor);
		
		EasyMock.expect(this.connection1.setSavepoint(name)).andReturn(savepoint1);
		EasyMock.expect(this.connection2.setSavepoint(name)).andReturn(savepoint2);
		
		this.readLock.unlock();
		
		this.replay();
		
		Savepoint result = this.connection.setSavepoint(name);
		
		this.verify();

		assert Proxy.isProxyClass(result.getClass());
		
		SQLProxy proxy = SQLProxy.class.cast(Proxy.getInvocationHandler(result));
		
		assert proxy.getObject(this.database1) == savepoint1;
		assert proxy.getObject(this.database2) == savepoint2;
		
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
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseSet);
		
		EasyMock.expect(this.parent.getRoot()).andReturn(this.root);
		
		this.root.retain(this.databaseSet);
		
		EasyMock.expect(this.cluster.getNonTransactionalExecutor()).andReturn(this.executor);
		
		this.connection1.setTransactionIsolation(level);
		this.connection2.setTransactionIsolation(level);
		
		this.replay();
		
		this.connection.setTransactionIsolation(level);
		
		this.verify();
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
		this.connection1.setTypeMap(map);
		this.connection2.setTypeMap(map);
		
		this.replay();
		
		this.connection.setTypeMap(map);
		
		this.verify();
	}

	@DataProvider(name = "string-objects")
	Object[][] elementsProvider()
	{
		return new Object[][] { new Object[] { "", new Object[0] } };
	}

	/**
	 * @see java.sql.Connection#createArrayOf(java.lang.String, java.lang.Object[])
	 */
	@Test(dataProvider = "string-objects")
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.connection1.createArrayOf(typeName, elements)).andReturn(array);
		
		this.replay();
		
		Array result = this.connection.createArrayOf(typeName, elements);
		
		this.verify();
		
		assert result == array;
		
		return result;
	}

	/**
	 * @see java.sql.Connection#createBlob()
	 */
	@Test
	public Blob createBlob() throws SQLException
	{
		Blob blob = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.connection1.createBlob()).andReturn(blob);
		
		this.replay();
		
		Blob result = this.connection.createBlob();
		
		this.verify();
		
		assert result == blob;
		
		return result;
	}

	/**
	 * @see java.sql.Connection#createClob()
	 */
	@Test
	public Clob createClob() throws SQLException
	{
		Clob clob = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.connection1.createClob()).andReturn(clob);
		
		this.replay();
		
		Clob result = this.connection.createClob();
		
		this.verify();
		
		assert result == clob;
		
		return result;
	}

	/**
	 * @see java.sql.Connection#createNClob()
	 */
	@Test
	public NClob createNClob() throws SQLException
	{
		NClob clob = EasyMock.createMock(NClob.class);
		
		EasyMock.expect(this.connection1.createNClob()).andReturn(clob);
		
		this.replay();
		
		NClob result = this.connection.createNClob();
		
		this.verify();
		
		assert result == clob;
		
		return result;
	}

	/**
	 * @see java.sql.Connection#createSQLXML()
	 */
	@Test
	public SQLXML createSQLXML() throws SQLException
	{
		SQLXML xml = EasyMock.createMock(SQLXML.class);
		
		EasyMock.expect(this.connection1.createSQLXML()).andReturn(xml);
		
		this.replay();
		
		SQLXML result = this.connection.createSQLXML();
		
		this.verify();
		
		assert result == xml;
		
		return result;
	}
	
	/**
	 * @see java.sql.Connection#createStruct(java.lang.String, java.lang.Object[])
	 */
	@Test(dataProvider = "string-objects")
	public Struct createStruct(String typeName, Object[] elements) throws SQLException
	{
		Struct struct = EasyMock.createMock(Struct.class);
		
		EasyMock.expect(this.connection1.createStruct(typeName, elements)).andReturn(struct);
		
		this.replay();
		
		Struct result = this.connection.createStruct(typeName, elements);
		
		this.verify();
		
		assert result == struct;
		
		return result;
	}

	/**
	 * @see java.sql.Connection#getClientInfo()
	 */
	@Test
	public Properties getClientInfo() throws SQLException
	{
		Properties properties = new Properties();
		
		EasyMock.expect(this.connection1.getClientInfo()).andReturn(properties);
		
		this.replay();
		
		Properties result = this.connection.getClientInfo();
		
		this.verify();
		
		assert result == properties;
		
		return result;
	}

	/**
	 * @see java.sql.Connection#getClientInfo(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public String getClientInfo(String property) throws SQLException
	{
		EasyMock.expect(this.connection1.getClientInfo(property)).andReturn("value");
		
		this.replay();
		
		String result = this.connection.getClientInfo(property);
		
		this.verify();
		
		assert result.equals("value");
		
		return result;
	}

	@DataProvider(name = "int")
	Object[][] intProvider()
	{
		return new Object[][] { new Object[] { 1 } };
	}
	
	/**
	 * @see java.sql.Connection#isValid(int)
	 */
	@Test(dataProvider = "int")
	public boolean isValid(int timeout) throws SQLException
	{
		EasyMock.expect(this.cluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database2);
		
		this.balancer.beforeInvocation(this.database2);
		
		EasyMock.expect(this.connection2.isValid(timeout)).andReturn(true);
		
		this.balancer.afterInvocation(this.database2);
		
		this.replay();
		
		boolean result = this.connection.isValid(timeout);
		
		this.verify();
		
		assert result;
		
		return result;
	}

	@DataProvider(name = "properties")
	Object[][] propertiesProvider()
	{
		return new Object[][] { new Object[] { new Properties() } };
	}
	
	/**
	 * @see java.sql.Connection#setClientInfo(java.util.Properties)
	 */
	@Test(dataProvider = "properties")
	public void setClientInfo(Properties properties) throws SQLClientInfoException
	{
		this.connection1.setClientInfo(properties);
		this.connection2.setClientInfo(properties);
		
		this.replay();
		
		this.connection.setClientInfo(properties);
		
		this.verify();
	}

	@DataProvider(name = "string-string")
	Object[][] stringStringProvider()
	{
		return new Object[][] { new Object[] { "name", "value" } };
	}
	
	/**
	 * @see java.sql.Connection#setClientInfo(java.lang.String, java.lang.String)
	 */
	@Test(dataProvider = "string-string")
	public void setClientInfo(String property, String value) throws SQLClientInfoException
	{
		this.connection1.setClientInfo(property, value);
		this.connection2.setClientInfo(property, value);
		
		this.replay();
		
		this.connection.setClientInfo(property, value);
		
		this.verify();
	}

	@DataProvider(name = "class")
	Object[][] classProvider()
	{
		return new Object[][] { new Object[] { Connection.class } };
	}
	
	/**
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	@Test(dataProvider = "class")
	public boolean isWrapperFor(Class<?> targetClass) throws SQLException
	{
		EasyMock.expect(this.connection1.isWrapperFor(targetClass)).andReturn(true);
		
		this.replay();
		
		boolean result = this.connection.isWrapperFor(targetClass);
		
		this.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	@Test(dataProvider = "class")
	public <T> T unwrap(Class<T> targetClass) throws SQLException
	{
		T object = EasyMock.createMock(targetClass);
		
		EasyMock.expect(this.connection1.unwrap(targetClass)).andReturn(object);
		
		this.replay();
		
		T result = this.connection.unwrap(targetClass);
		
		this.verify();
		
		assert result == object;
		
		return result;
	}
	
	protected void getLockList(String sql) throws SQLException
	{	
		EasyMock.expect(this.cluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.cluster.isSequenceDetectionEnabled()).andReturn(true);
		EasyMock.expect(this.dialect.parseSequence(sql)).andReturn("sequence");
		
		EasyMock.expect(this.cluster.isIdentityColumnDetectionEnabled()).andReturn(true);
		EasyMock.expect(this.dialect.parseInsertTable(sql)).andReturn("table");
		EasyMock.expect(this.cluster.getDatabaseMetaDataCache()).andReturn(this.metaData);
		EasyMock.expect(this.metaData.getDatabaseProperties(this.connection)).andReturn(this.databaseProperties);
		EasyMock.expect(this.databaseProperties.findTable("table")).andReturn(this.tableProperties);
		EasyMock.expect(this.tableProperties.getColumns()).andReturn(Collections.singleton("column"));
		EasyMock.expect(this.tableProperties.getColumnProperties("column")).andReturn(this.columnProperties);
		EasyMock.expect(this.columnProperties.isAutoIncrement()).andReturn(null);
		EasyMock.expect(this.dialect.isIdentity(this.columnProperties)).andReturn(true);
		EasyMock.expect(this.tableProperties.getName()).andReturn("table");
		EasyMock.expect(this.cluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.writeLock("sequence")).andReturn(this.writeLock1);
		EasyMock.expect(this.lockManager.writeLock("table")).andReturn(this.writeLock2);
		
		EasyMock.expect(this.cluster.getDatabaseMetaDataCache()).andReturn(this.metaData);
		EasyMock.expect(this.metaData.getDatabaseProperties(this.connection)).andReturn(this.databaseProperties);
		EasyMock.expect(this.databaseProperties.supportsSelectForUpdate()).andReturn(true);
		EasyMock.expect(this.cluster.getDialect()).andReturn(this.dialect);
		EasyMock.expect(this.dialect.isSelectForUpdate(sql)).andReturn(true);
	}
}
