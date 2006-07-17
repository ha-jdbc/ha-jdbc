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

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
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
import org.testng.annotations.Configuration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/***
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestResultSet implements java.sql.ResultSet
{
	private IMocksControl control = EasyMock.createStrictControl();
	
	private DatabaseCluster databaseCluster = this.control.createMock(DatabaseCluster.class);
	
	private LockManager lockManager = this.control.createMock(LockManager.class);
	
	protected java.sql.Connection sqlConnection = this.control.createMock(java.sql.Connection.class);
	
	protected java.sql.Statement sqlStatement = this.control.createMock(java.sql.Statement.class);
	
	protected java.sql.ResultSet sqlResultSet = this.control.createMock(java.sql.ResultSet.class);
	
	private Database database = new MockDatabase();
	
	private Balancer balancer = this.control.createMock(Balancer.class);
	
	private FileSupport fileSupport = this.control.createMock(FileSupport.class);
	
	private Lock lock = this.control.createMock(Lock.class);
	
	private Statement<java.sql.Statement> statement;
	private ResultSet<java.sql.Statement> resultSet;
	private List<Database> databaseList = Collections.singletonList(this.database);
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	
	@SuppressWarnings("unchecked")
	@Configuration(beforeTestClass = true)
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
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		this.lock.lock();
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		this.lock.unlock();
				
		this.control.replay();
		
		ConnectionFactory<Object> connectionFactory = new ConnectionFactory<Object>(this.databaseCluster, Object.class);
		
		Operation<Object, java.sql.Connection> operation = new Operation<Object, java.sql.Connection>()
		{
			public java.sql.Connection execute(Database database, Object sqlObject)
			{
				return TestResultSet.this.sqlConnection;
			}
		};
		
		Connection<Object> connection = new Connection<Object>(connectionFactory, operation, this.fileSupport);
		
		Operation<java.sql.Connection, java.sql.Statement> connectionOperation = new Operation<java.sql.Connection, java.sql.Statement>()
		{
			public java.sql.Statement execute(Database database, java.sql.Connection connection)
			{
				return TestResultSet.this.sqlStatement;
			}
		};
		
		this.statement = new Statement<java.sql.Statement>(connection, connectionOperation);
		
		Operation<java.sql.Statement, java.sql.ResultSet> statementOperation = new Operation<java.sql.Statement, java.sql.ResultSet>()
		{
			public java.sql.ResultSet execute(Database database, java.sql.Statement statement)
			{
				return TestResultSet.this.sqlResultSet;
			}
		};
		
		this.resultSet = new ResultSet<java.sql.Statement>(this.statement, statementOperation, this.lock);
		
		this.control.verify();
		this.control.reset();
	}
	
	@Configuration(afterTestMethod = true)
	public void reset()
	{
		this.control.reset();
	}

	@DataProvider(name = "int")
	Object[][] intProvider()
	{
		return new Object[][] { new Object[] { 1 } };
	}
	
	/**
	 * @see java.sql.ResultSet#absolute(int)
	 */
	@Test(dataProvider = "int")
	public boolean absolute(int row) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlResultSet.absolute(row)).andReturn(true);
		
		this.control.replay();
		
		boolean valid = this.resultSet.absolute(row);
		
		this.control.verify();
		
		assert valid;
		
		return valid;
	}

	/**
	 * @see java.sql.ResultSet#afterLast()
	 */
	@Test
	public void afterLast() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.afterLast();
		
		this.control.replay();
		
		this.resultSet.afterLast();
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#beforeFirst()
	 */
	@Test
	public void beforeFirst() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.beforeFirst();
		
		this.control.replay();
		
		this.resultSet.beforeFirst();
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#cancelRowUpdates()
	 */
	@Test
	public void cancelRowUpdates() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.cancelRowUpdates();
		
		this.control.replay();
		
		this.resultSet.cancelRowUpdates();
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#clearWarnings()
	 */
	@Test
	public void clearWarnings() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.clearWarnings();
		
		this.control.replay();
		
		this.resultSet.clearWarnings();
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#close()
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
		
		this.sqlResultSet.close();
		
		this.lock.unlock();
		
		this.control.replay();
		
		this.resultSet.close();
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#deleteRow()
	 */
	@Test
	public void deleteRow() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.deleteRow();
		
		this.lock.unlock();
		
		this.control.replay();
		
		this.resultSet.deleteRow();
		
		this.control.verify();
	}

	@DataProvider(name = "string")
	Object[][] stringProvider()
	{
		return new Object[][] { new Object[] { "" } };
	}
	
	/**
	 * @see java.sql.ResultSet#findColumn(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public int findColumn(String name) throws SQLException
	{
		int index = 1;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.findColumn(name)).andReturn(index);
		
		this.control.replay();
		
		int result = this.resultSet.findColumn(name);
		
		this.control.verify();
		
		assert result == index : result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#first()
	 */
	@Test
	public boolean first() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.getType()).andReturn(java.sql.ResultSet.TYPE_FORWARD_ONLY);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlResultSet.first()).andReturn(true);
		
		this.control.replay();
		
		boolean result = this.resultSet.first();
		
		this.control.verify();
		
		assert result;
		
		this.control.reset();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.getType()).andReturn(java.sql.ResultSet.TYPE_SCROLL_SENSITIVE);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlResultSet.first()).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();
		
		result = this.resultSet.first();
		
		this.control.verify();
		
		assert result;
				
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getArray(int)
	 */
	@Test(dataProvider = "int")
	public Array getArray(int index) throws SQLException
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getArray(index)).andReturn(array);
		
		this.control.replay();
		
		Array result = this.resultSet.getArray(index);
		
		this.control.verify();
		
		assert array == result;
		
		return result;
	}
	
	/**
	 * @see java.sql.ResultSet#getArray(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Array getArray(String name) throws SQLException
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getArray(name)).andReturn(array);
		
		this.control.replay();
		
		Array result = this.resultSet.getArray(name);
		
		this.control.verify();
		
		assert array == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getAsciiStream(int)
	 */
	@Test(dataProvider = "int")
	public InputStream getAsciiStream(int index) throws SQLException
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getAsciiStream(index)).andReturn(inputStream);
		
		this.control.replay();
		
		InputStream result = this.resultSet.getAsciiStream(index);
		
		this.control.verify();
		
		assert inputStream == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getAsciiStream(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public InputStream getAsciiStream(String name) throws SQLException
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getAsciiStream(name)).andReturn(inputStream);
		
		this.control.replay();
		
		InputStream result = this.resultSet.getAsciiStream(name);
		
		this.control.verify();
		
		assert inputStream == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(int)
	 */
	@Test(dataProvider = "int")
	public BigDecimal getBigDecimal(int index) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getBigDecimal(index)).andReturn(decimal);
		
		this.control.replay();
		
		BigDecimal result = this.resultSet.getBigDecimal(index);
		
		this.control.verify();
		
		assert decimal == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public BigDecimal getBigDecimal(String name) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getBigDecimal(name)).andReturn(decimal);
		
		this.control.replay();
		
		BigDecimal result = this.resultSet.getBigDecimal(name);
		
		this.control.verify();
		
		assert decimal == result;
		
		return result;
	}

	@DataProvider(name = "int-int")
	Object[][] intIntProvider()
	{
		return new Object[][] { new Object[] { 1, 1 } };
	}
	
	/**
	 * @see java.sql.ResultSet#getBigDecimal(int, int)
	 */
	@Test(dataProvider = "int-int")
	@Deprecated
	public BigDecimal getBigDecimal(int index, int scale) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getBigDecimal(index, scale)).andReturn(decimal);
		
		this.control.replay();
		
		BigDecimal result = this.resultSet.getBigDecimal(index, scale);
		
		this.control.verify();
		
		assert decimal == result;
		
		return result;
	}

	@DataProvider(name = "string-int")
	Object[][] stringIntProvider()
	{
		return new Object[][] { new Object[] { "", 1 } };
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	@Deprecated
	public BigDecimal getBigDecimal(String name, int scale) throws SQLException
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getBigDecimal(name, scale)).andReturn(decimal);
		
		this.control.replay();
		
		BigDecimal result = this.resultSet.getBigDecimal(name, scale);
		
		this.control.verify();
		
		assert decimal == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBinaryStream(int)
	 */
	@Test(dataProvider = "int")
	public InputStream getBinaryStream(int index) throws SQLException
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getBinaryStream(index)).andReturn(inputStream);
		
		this.control.replay();
		
		InputStream result = this.resultSet.getBinaryStream(index);
		
		this.control.verify();
		
		assert inputStream == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBinaryStream(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public InputStream getBinaryStream(String name) throws SQLException
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getBinaryStream(name)).andReturn(inputStream);
		
		this.control.replay();
		
		InputStream result = this.resultSet.getBinaryStream(name);
		
		this.control.verify();
		
		assert inputStream == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBlob(int)
	 */
	@Test(dataProvider = "int")
	public Blob getBlob(int index) throws SQLException
	{
		Blob blob = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getBlob(index)).andReturn(blob);
		
		this.control.replay();
		
		Blob result = this.resultSet.getBlob(index);
		
		this.control.verify();
		
		assert blob == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBlob(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Blob getBlob(String name) throws SQLException
	{
		Blob blob = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getBlob(name)).andReturn(blob);
		
		this.control.replay();
		
		Blob result = this.resultSet.getBlob(name);
		
		this.control.verify();
		
		assert blob == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBoolean(int)
	 */
	@Test(dataProvider = "int")
	public boolean getBoolean(int index) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getBoolean(index)).andReturn(true);
		
		this.control.replay();
		
		boolean result = this.resultSet.getBoolean(index);
		
		this.control.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBoolean(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public boolean getBoolean(String name) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getBoolean(name)).andReturn(true);
		
		this.control.replay();
		
		boolean result = this.resultSet.getBoolean(name);
		
		this.control.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getByte(int)
	 */
	@Test(dataProvider = "int")
	public byte getByte(int index) throws SQLException
	{
		byte b = Integer.valueOf(1).byteValue();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getByte(index)).andReturn(b);
		
		this.control.replay();
		
		byte result = this.resultSet.getByte(index);
		
		this.control.verify();
		
		assert b == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getByte(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public byte getByte(String name) throws SQLException
	{
		byte b = Integer.valueOf(1).byteValue();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getByte(name)).andReturn(b);
		
		this.control.replay();
		
		byte result = this.resultSet.getByte(name);
		
		this.control.verify();
		
		assert b == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBytes(int)
	 */
	@Test(dataProvider = "int")
	public byte[] getBytes(int index) throws SQLException
	{
		byte[] bytes = new byte[0];
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getBytes(index)).andReturn(bytes);
		
		this.control.replay();
		
		byte[] result = this.resultSet.getBytes(index);
		
		this.control.verify();
		
		assert bytes == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getBytes(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public byte[] getBytes(String name) throws SQLException
	{
		byte[] bytes = new byte[0];
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getBytes(name)).andReturn(bytes);
		
		this.control.replay();
		
		byte[] result = this.resultSet.getBytes(name);
		
		this.control.verify();
		
		assert bytes == result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getCharacterStream(int)
	 */
	@Test(dataProvider = "int")
	public Reader getCharacterStream(int index) throws SQLException
	{
		Reader reader = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getCharacterStream(index)).andReturn(reader);
		
		this.control.replay();
		
		Reader result = this.resultSet.getCharacterStream(index);
		
		this.control.verify();
		
		assert result == reader;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getCharacterStream(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Reader getCharacterStream(String name) throws SQLException
	{
		Reader reader = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getCharacterStream(name)).andReturn(reader);
		
		this.control.replay();
		
		Reader result = this.resultSet.getCharacterStream(name);
		
		this.control.verify();
		
		assert result == reader;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getClob(int)
	 */
	@Test(dataProvider = "int")
	public Clob getClob(int index) throws SQLException
	{
		Clob clob = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getClob(index)).andReturn(clob);
		
		this.control.replay();
		
		Clob result = this.resultSet.getClob(index);
		
		this.control.verify();
		
		assert result == clob;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getClob(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Clob getClob(String name) throws SQLException
	{
		Clob clob = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getClob(name)).andReturn(clob);
		
		this.control.replay();
		
		Clob result = this.resultSet.getClob(name);
		
		this.control.verify();
		
		assert result == clob;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getConcurrency()
	 */
	@Test
	public int getConcurrency() throws SQLException
	{
		int concurrency = java.sql.ResultSet.CONCUR_READ_ONLY;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getConcurrency()).andReturn(concurrency);
		
		this.control.replay();
		
		int result = this.resultSet.getConcurrency();
		
		this.control.verify();
		
		assert result == concurrency;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getCursorName()
	 */
	@Test
	public String getCursorName() throws SQLException
	{
		String cursor = "cursor";
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getCursorName()).andReturn(cursor);
		
		this.control.replay();
		
		String result = this.resultSet.getCursorName();
		
		this.control.verify();
		
		assert result == cursor;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getDate(int)
	 */
	@Test(dataProvider = "int")
	public Date getDate(int index) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getDate(index)).andReturn(date);
		
		this.control.replay();
		
		Date result = this.resultSet.getDate(index);
		
		this.control.verify();
		
		assert result == date;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getDate(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Date getDate(String name) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getDate(name)).andReturn(date);
		
		this.control.replay();
		
		Date result = this.resultSet.getDate(name);
		
		this.control.verify();
		
		assert result == date;
		
		return result;
	}

	@DataProvider(name = "int-calendar")
	Object[][] intCalendarProvider()
	{
		return new Object[][] { new Object[] { 1, Calendar.getInstance() } };
	}
	
	/**
	 * @see java.sql.ResultSet#getDate(int, java.util.Calendar)
	 */
	@Test(dataProvider = "int-calendar")
	public Date getDate(int index, Calendar calendar) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getDate(index, calendar)).andReturn(date);
		
		this.control.replay();
		
		Date result = this.resultSet.getDate(index, calendar);
		
		this.control.verify();
		
		assert result == date;
		
		return result;
	}

	@DataProvider(name = "string-calendar")
	Object[][] stringCalendarProvider()
	{
		return new Object[][] { new Object[] { "", Calendar.getInstance() } };
	}

	/**
	 * @see java.sql.ResultSet#getDate(java.lang.String, java.util.Calendar)
	 */
	@Test(dataProvider = "string-calendar")
	public Date getDate(String name, Calendar calendar) throws SQLException
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getDate(name, calendar)).andReturn(date);
		
		this.control.replay();
		
		Date result = this.resultSet.getDate(name, calendar);
		
		this.control.verify();
		
		assert result == date;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getDouble(int)
	 */
	@Test(dataProvider = "int")
	public double getDouble(int index) throws SQLException
	{
		double d = 1.0;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getDouble(index)).andReturn(d);
		
		this.control.replay();
		
		double result = this.resultSet.getDouble(index);
		
		this.control.verify();
		
		assert result == d;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getDouble(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public double getDouble(String name) throws SQLException
	{
		double d = 1.0;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getDouble(name)).andReturn(d);
		
		this.control.replay();
		
		double result = this.resultSet.getDouble(name);
		
		this.control.verify();
		
		assert result == d;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getFetchDirection()
	 */
	@Test
	public int getFetchDirection() throws SQLException
	{
		int direction = java.sql.ResultSet.FETCH_FORWARD;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getFetchDirection()).andReturn(direction);
		
		this.control.replay();
		
		int result = this.resultSet.getFetchDirection();
		
		this.control.verify();
		
		assert result == direction;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getFetchSize()
	 */
	@Test
	public int getFetchSize() throws SQLException
	{
		int size = 10;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getFetchSize()).andReturn(size);
		
		this.control.replay();
		
		int result = this.resultSet.getFetchSize();
		
		this.control.verify();
		
		assert result == size;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getFloat(int)
	 */
	@Test(dataProvider = "int")
	public float getFloat(int index) throws SQLException
	{
		float f = 1.0F;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getFloat(index)).andReturn(f);
		
		this.control.replay();
		
		float result = this.resultSet.getFloat(index);
		
		this.control.verify();
		
		assert result == f;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getFloat(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public float getFloat(String name) throws SQLException
	{
		float f = 1.0F;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getFloat(name)).andReturn(f);
		
		this.control.replay();
		
		float result = this.resultSet.getFloat(name);
		
		this.control.verify();
		
		assert result == f;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getInt(int)
	 */
	@Test(dataProvider = "int")
	public int getInt(int index) throws SQLException
	{
		int i = 1;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getInt(index)).andReturn(i);
		
		this.control.replay();
		
		int result = this.resultSet.getInt(index);
		
		this.control.verify();
		
		assert result == i;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getInt(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public int getInt(String name) throws SQLException
	{
		int i = 1;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getInt(name)).andReturn(i);
		
		this.control.replay();
		
		int result = this.resultSet.getInt(name);
		
		this.control.verify();
		
		assert result == i;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getLong(int)
	 */
	@Test(dataProvider = "int")
	public long getLong(int index) throws SQLException
	{
		long l = 1;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getLong(index)).andReturn(l);
		
		this.control.replay();
		
		long result = this.resultSet.getLong(index);
		
		this.control.verify();
		
		assert result == l;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getLong(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public long getLong(String name) throws SQLException
	{
		long l = 1;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getLong(name)).andReturn(l);
		
		this.control.replay();
		
		long result = this.resultSet.getLong(name);
		
		this.control.verify();
		
		assert result == l;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getMetaData()
	 */
	@Test
	public ResultSetMetaData getMetaData() throws SQLException
	{
		ResultSetMetaData metaData = EasyMock.createMock(ResultSetMetaData.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getMetaData()).andReturn(metaData);
		
		this.control.replay();
		
		ResultSetMetaData result = this.resultSet.getMetaData();
		
		this.control.verify();
		
		assert result == metaData;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getObject(int)
	 */
	@Test(dataProvider = "int")
	public Object getObject(int index) throws SQLException
	{
		Object object = new Object();
				
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getObject(index)).andReturn(object);
		
		this.control.replay();
		
		Object result = this.resultSet.getObject(index);
		
		this.control.verify();
		
		assert result == object;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getObject(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Object getObject(String name) throws SQLException
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getObject(name)).andReturn(object);
		
		this.control.replay();
		
		Object result = this.resultSet.getObject(name);
		
		this.control.verify();
		
		assert result == object;
		
		return result;
	}

	@DataProvider(name = "int-map")
	Object[][] intMapProvider()
	{
		return new Object[][] { new Object[] { 1, Collections.EMPTY_MAP } };
	}
	
	/**
	 * @see java.sql.ResultSet#getObject(int, java.util.Map)
	 */
	@Test(dataProvider = "int-map")
	public Object getObject(int index, Map<String, Class<?>> map) throws SQLException
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getObject(index, map)).andReturn(object);
		
		this.control.replay();
		
		Object result = this.resultSet.getObject(index, map);
		
		this.control.verify();
		
		assert result == object;
		
		return result;
	}

	@DataProvider(name = "string-map")
	Object[][] stringMapProvider()
	{
		return new Object[][] { new Object[] { "", Collections.EMPTY_MAP } };
	}

	/**
	 * @see java.sql.ResultSet#getObject(java.lang.String, java.util.Map)
	 */
	@Test(dataProvider = "string-map")
	public Object getObject(String name, Map<String, Class<?>> map) throws SQLException
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getObject(name, map)).andReturn(object);
		
		this.control.replay();
		
		Object result = this.resultSet.getObject(name, map);
		
		this.control.verify();
		
		assert result == object;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getRef(int)
	 */
	@Test(dataProvider = "int")
	public Ref getRef(int index) throws SQLException
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getRef(index)).andReturn(ref);
		
		this.control.replay();
		
		Ref result = this.resultSet.getRef(index);
		
		this.control.verify();
		
		assert result == ref;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getRef(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Ref getRef(String name) throws SQLException
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getRef(name)).andReturn(ref);
		
		this.control.replay();
		
		Ref result = this.resultSet.getRef(name);
		
		this.control.verify();
		
		assert result == ref;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getRow()
	 */
	@Test
	public int getRow() throws SQLException
	{
		int row = 1;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getRow()).andReturn(row);
		
		this.control.replay();
		
		int result = this.resultSet.getRow();
		
		this.control.verify();
		
		assert result == row;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getShort(int)
	 */
	@Test(dataProvider = "int")
	public short getShort(int index) throws SQLException
	{
		short s = Integer.valueOf(1).shortValue();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getShort(index)).andReturn(s);
		
		this.control.replay();
		
		short result = this.resultSet.getShort(index);
		
		this.control.verify();
		
		assert result == s;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getShort(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public short getShort(String name) throws SQLException
	{
		short s = Integer.valueOf(1).shortValue();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getShort(name)).andReturn(s);
		
		this.control.replay();
		
		short result = this.resultSet.getShort(name);
		
		this.control.verify();
		
		assert result == s;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getStatement()
	 */
	@Test
	public java.sql.Statement getStatement()
	{
		this.control.replay();
		
		java.sql.Statement result = this.resultSet.getStatement();
		
		this.control.verify();
		
		assert result == this.statement;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getString(int)
	 */
	@Test(dataProvider = "int")
	public String getString(int index) throws SQLException
	{
		String string = "";
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getString(index)).andReturn(string);
		
		this.control.replay();
		
		String result = this.resultSet.getString(index);
		
		this.control.verify();
		
		assert result == string;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getString(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public String getString(String name) throws SQLException
	{
		String string = "";
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getString(name)).andReturn(string);
		
		this.control.replay();
		
		String result = this.resultSet.getString(name);
		
		this.control.verify();
		
		assert result == string;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTime(int)
	 */
	@Test(dataProvider = "int")
	public Time getTime(int index) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getTime(index)).andReturn(time);
		
		this.control.replay();
		
		Time result = this.resultSet.getTime(index);
		
		this.control.verify();
		
		assert result == time;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTime(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Time getTime(String name) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getTime(name)).andReturn(time);
		
		this.control.replay();
		
		Time result = this.resultSet.getTime(name);
		
		this.control.verify();
		
		assert result == time;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTime(int, java.util.Calendar)
	 */
	@Test(dataProvider = "int-calendar")
	public Time getTime(int index, Calendar calendar) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getTime(index, calendar)).andReturn(time);
		
		this.control.replay();
		
		Time result = this.resultSet.getTime(index, calendar);
		
		this.control.verify();
		
		assert result == time;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTime(java.lang.String, java.util.Calendar)
	 */
	@Test(dataProvider = "string-calendar")
	public Time getTime(String name, Calendar calendar) throws SQLException
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getTime(name, calendar)).andReturn(time);
		
		this.control.replay();
		
		Time result = this.resultSet.getTime(name, calendar);
		
		this.control.verify();
		
		assert result == time;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(int)
	 */
	@Test(dataProvider = "int")
	public Timestamp getTimestamp(int index) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getTimestamp(index)).andReturn(timestamp);
		
		this.control.replay();
		
		Timestamp result = this.resultSet.getTimestamp(index);
		
		this.control.verify();
		
		assert result == timestamp;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public Timestamp getTimestamp(String name) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getTimestamp(name)).andReturn(timestamp);
		
		this.control.replay();
		
		Timestamp result = this.resultSet.getTimestamp(name);
		
		this.control.verify();
		
		assert result == timestamp;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(int, java.util.Calendar)
	 */
	@Test(dataProvider = "int-calendar")
	public Timestamp getTimestamp(int index, Calendar calendar) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getTimestamp(index, calendar)).andReturn(timestamp);
		
		this.control.replay();
		
		Timestamp result = this.resultSet.getTimestamp(index, calendar);
		
		this.control.verify();
		
		assert result == timestamp;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(java.lang.String, java.util.Calendar)
	 */
	@Test(dataProvider = "string-calendar")
	public Timestamp getTimestamp(String name, Calendar calendar) throws SQLException
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getTimestamp(name, calendar)).andReturn(timestamp);
		
		this.control.replay();
		
		Timestamp result = this.resultSet.getTimestamp(name, calendar);
		
		this.control.verify();
		
		assert result == timestamp;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getType()
	 */
	@Test
	public int getType() throws SQLException
	{
		int type = java.sql.ResultSet.TYPE_FORWARD_ONLY;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getType()).andReturn(type);
		
		this.control.replay();
		
		int result = this.resultSet.getType();
		
		this.control.verify();
		
		assert result == type;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getURL(int)
	 */
	@Test(dataProvider = "int")
	public URL getURL(int index) throws SQLException
	{
		try
		{
			URL url = new URL("http://ha-jdbc.sf.net");
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.first()).andReturn(this.database);
	
			EasyMock.expect(this.sqlResultSet.getURL(index)).andReturn(url);
			
			this.control.replay();
			
			URL result = this.resultSet.getURL(index);
			
			this.control.verify();
			
			assert result == url;
			
			return result;
		}
		catch (MalformedURLException e)
		{
			assert false;
			return null;
		}
	}

	/**
	 * @see java.sql.ResultSet#getURL(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public URL getURL(String name) throws SQLException
	{
		try
		{
			URL url = new URL("http://ha-jdbc.sf.net");
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.first()).andReturn(this.database);
	
			EasyMock.expect(this.sqlResultSet.getURL(name)).andReturn(url);
			
			this.control.replay();
			
			URL result = this.resultSet.getURL(name);
			
			this.control.verify();
			
			assert result == url;
			
			return result;
		}
		catch (MalformedURLException e)
		{
			assert false;
			return null;
		}
	}

	/**
	 * @see java.sql.ResultSet#getUnicodeStream(int)
	 */
	@Test(dataProvider = "int")
	@Deprecated
	public InputStream getUnicodeStream(int index) throws SQLException
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getUnicodeStream(index)).andReturn(inputStream);
		
		this.control.replay();
		
		InputStream result = this.resultSet.getUnicodeStream(index);
		
		this.control.verify();
		
		assert result == inputStream;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getUnicodeStream(java.lang.String)
	 */
	@Test(dataProvider = "string")
	@Deprecated
	public InputStream getUnicodeStream(String name) throws SQLException
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getUnicodeStream(name)).andReturn(inputStream);
		
		this.control.replay();
		
		InputStream result = this.resultSet.getUnicodeStream(name);
		
		this.control.verify();
		
		assert result == inputStream;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#getWarnings()
	 */
	@Test
	public SQLWarning getWarnings() throws SQLException
	{
		SQLWarning warning = new SQLWarning();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.getWarnings()).andReturn(warning);
		
		this.control.replay();
		
		SQLWarning result = this.resultSet.getWarnings();
		
		this.control.verify();
		
		assert result == warning;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#insertRow()
	 */
	@Test
	public void insertRow() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.insertRow();
		
		this.lock.unlock();
		
		this.control.replay();
		
		this.resultSet.insertRow();
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#isAfterLast()
	 */
	@Test
	public boolean isAfterLast() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.isAfterLast()).andReturn(true);
		
		this.control.replay();
		
		boolean result = this.resultSet.isAfterLast();
		
		this.control.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#isBeforeFirst()
	 */
	@Test
	public boolean isBeforeFirst() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		EasyMock.expect(this.sqlResultSet.isBeforeFirst()).andReturn(true);
		
		this.control.replay();
		
		boolean result = this.resultSet.isBeforeFirst();
		
		this.control.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#isFirst()
	 */
	@Test
	public boolean isFirst() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.getType()).andReturn(java.sql.ResultSet.TYPE_FORWARD_ONLY);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.isFirst()).andReturn(true);
		
		this.control.replay();
		
		boolean result = this.resultSet.isFirst();
		
		this.control.verify();
		
		assert result;
		
		this.control.reset();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.getType()).andReturn(java.sql.ResultSet.TYPE_SCROLL_SENSITIVE);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlResultSet.isFirst()).andReturn(true);
		
		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		result = this.resultSet.isFirst();
		
		this.control.verify();
		
		assert result;
				
		return result;
	}

	/**
	 * @see java.sql.ResultSet#isLast()
	 */
	@Test
	public boolean isLast() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.getType()).andReturn(java.sql.ResultSet.TYPE_FORWARD_ONLY);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.isLast()).andReturn(true);
		
		this.control.replay();
		
		boolean result = this.resultSet.isLast();
		
		this.control.verify();
		
		assert result;
		
		this.control.reset();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.getType()).andReturn(java.sql.ResultSet.TYPE_SCROLL_SENSITIVE);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		this.balancer.beforeOperation(this.database);
		
		EasyMock.expect(this.sqlResultSet.isLast()).andReturn(true);

		this.balancer.afterOperation(this.database);
		
		this.control.replay();
		
		result = this.resultSet.isLast();
		
		this.control.verify();
		
		assert result;
				
		return result;
	}

	/**
	 * @see java.sql.ResultSet#last()
	 */
	@Test
	public boolean last() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.getType()).andReturn(java.sql.ResultSet.TYPE_FORWARD_ONLY);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlResultSet.last()).andReturn(true);
		
		this.control.replay();
		
		boolean result = this.resultSet.last();
		
		this.control.verify();
		
		assert result;
		
		this.control.reset();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.getType()).andReturn(java.sql.ResultSet.TYPE_SCROLL_SENSITIVE);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlResultSet.last()).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();
		
		result = this.resultSet.last();
		
		this.control.verify();
		
		assert result;
				
		return result;
	}

	/**
	 * @see java.sql.ResultSet#moveToCurrentRow()
	 */
	@Test
	public void moveToCurrentRow() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.getType()).andReturn(java.sql.ResultSet.TYPE_FORWARD_ONLY);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.moveToCurrentRow();
		
		this.control.replay();
		
		this.resultSet.moveToCurrentRow();
		
		this.control.verify();		
		this.control.reset();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.getType()).andReturn(java.sql.ResultSet.TYPE_SCROLL_SENSITIVE);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		this.sqlResultSet.moveToCurrentRow();
		
		this.lock.unlock();
		
		this.control.replay();

		this.resultSet.moveToCurrentRow();
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#moveToInsertRow()
	 */
	@Test
	public void moveToInsertRow() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.moveToInsertRow();
		
		this.control.replay();
		
		this.resultSet.moveToInsertRow();
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#next()
	 */
	@Test
	public boolean next() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.getType()).andReturn(java.sql.ResultSet.TYPE_FORWARD_ONLY);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlResultSet.next()).andReturn(true);
		
		this.control.replay();
		
		boolean result = this.resultSet.next();
		
		this.control.verify();
		
		assert result;
		
		this.control.reset();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.getType()).andReturn(java.sql.ResultSet.TYPE_SCROLL_SENSITIVE);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		EasyMock.expect(this.sqlResultSet.next()).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();

		result = this.resultSet.next();
		
		this.control.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#previous()
	 */
	@Test
	public boolean previous() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.getType()).andReturn(java.sql.ResultSet.TYPE_FORWARD_ONLY);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlResultSet.previous()).andReturn(true);
		
		this.control.replay();
		
		boolean result = this.resultSet.previous();
		
		this.control.verify();
		
		assert result;
		
		this.control.reset();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.getType()).andReturn(java.sql.ResultSet.TYPE_SCROLL_SENSITIVE);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

		EasyMock.expect(this.sqlResultSet.previous()).andReturn(true);
		
		this.lock.unlock();
		
		this.control.replay();

		result = this.resultSet.previous();
		
		this.control.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#refreshRow()
	 */
	@Test
	public void refreshRow() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.refreshRow();
		
		this.lock.unlock();
		
		this.control.replay();
		
		this.resultSet.refreshRow();
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#relative(int)
	 */
	@Test(dataProvider = "int")
	public boolean relative(int rows) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.sqlResultSet.relative(rows)).andReturn(true);
		
		this.control.replay();

		boolean result = this.resultSet.relative(rows);
		
		this.control.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#rowDeleted()
	 */
	@Test
	public boolean rowDeleted() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.rowDeleted()).andReturn(true);
		
		this.control.replay();

		boolean result = this.resultSet.rowDeleted();
		
		this.control.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#rowInserted()
	 */
	@Test
	public boolean rowInserted() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.rowInserted()).andReturn(true);
		
		this.control.replay();

		boolean result = this.resultSet.rowInserted();
		
		this.control.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#rowUpdated()
	 */
	@Test
	public boolean rowUpdated() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.rowUpdated()).andReturn(true);
		
		this.control.replay();

		boolean result = this.resultSet.rowUpdated();
		
		this.control.verify();
		
		assert result;
		
		return result;
	}

	/**
	 * @see java.sql.ResultSet#setFetchDirection(int)
	 */
	@Test(dataProvider = "int")
	public void setFetchDirection(int direction) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.setFetchDirection(direction);
		
		this.control.replay();

		this.resultSet.setFetchDirection(direction);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#setFetchSize(int)
	 */
	@Test(dataProvider = "int")
	public void setFetchSize(int rows) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.setFetchSize(rows);
		
		this.control.replay();

		this.resultSet.setFetchSize(rows);
		
		this.control.verify();
	}

	@DataProvider(name = "int-array")
	Object[][] intArrayProvider()
	{
		return new Object[][] { new Object[] { 1, EasyMock.createMock(Array.class) } };
	}
		
	/**
	 * @see java.sql.ResultSet#updateArray(int, java.sql.Array)
	 */
	@Test(dataProvider = "int-array")
	public void updateArray(int index, Array value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateArray(index, value);
		
		this.control.replay();

		this.resultSet.updateArray(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-array")
	Object[][] stringArrayProvider()
	{
		return new Object[][] { new Object[] { "", EasyMock.createMock(Array.class) } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateArray(java.lang.String, java.sql.Array)
	 */
	@Test(dataProvider = "string-array")
	public void updateArray(String name, Array value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateArray(name, value);
		
		this.control.replay();

		this.resultSet.updateArray(name, value);
		
		this.control.verify();
	}

	@DataProvider(name = "int-inputStream-int")
	Object[][] intInputStreamIntProvider()
	{
		return new Object[][] { new Object[] { 1, new ByteArrayInputStream(new byte[0]), 1 } };
	}
	
	/**
	 * @see java.sql.ResultSet#updateAsciiStream(int, java.io.InputStream, int)
	 */
	@Test(dataProvider = "int-inputStream-int")
	public void updateAsciiStream(int index, InputStream value, int length) throws SQLException
	{
		File file = new File("");
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
		
		this.sqlResultSet.updateAsciiStream(index, inputStream, length);
		
		this.control.replay();

		this.resultSet.updateAsciiStream(index, value, length);
		
		this.control.verify();
	}

	@DataProvider(name = "string-inputStream-int")
	Object[][] stringInputStreamIntProvider()
	{
		return new Object[][] { new Object[] { "", new ByteArrayInputStream(new byte[0]), 1 } };
	}

	/**
	 * @see java.sql.ResultSet#updateAsciiStream(java.lang.String, java.io.InputStream, int)
	 */
	@Test(dataProvider = "string-inputStream-int")
	public void updateAsciiStream(String name, InputStream value, int length) throws SQLException
	{
		File file = new File("");
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
		
		this.sqlResultSet.updateAsciiStream(name, inputStream, length);
		
		this.control.replay();

		this.resultSet.updateAsciiStream(name, value, length);
		
		this.control.verify();
	}

	@DataProvider(name = "int-bigDecimal")
	Object[][] intBigDecimalProvider()
	{
		return new Object[][] { new Object[] { 1, new BigDecimal(1.0) } };
	}

	/**
	 * @see java.sql.ResultSet#updateBigDecimal(int, java.math.BigDecimal)
	 */
	@Test(dataProvider = "int-bigDecimal")
	public void updateBigDecimal(int index, BigDecimal value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateBigDecimal(index, value);
		
		this.control.replay();

		this.resultSet.updateBigDecimal(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-bigDecimal")
	Object[][] stringBigDecimalProvider()
	{
		return new Object[][] { new Object[] { "", new BigDecimal(1.0) } };
	}

	/**
	 * @see java.sql.ResultSet#updateBigDecimal(java.lang.String, java.math.BigDecimal)
	 */
	@Test(dataProvider = "string-bigDecimal")
	public void updateBigDecimal(String name, BigDecimal value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateBigDecimal(name, value);
		
		this.control.replay();

		this.resultSet.updateBigDecimal(name, value);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateBinaryStream(int, java.io.InputStream, int)
	 */
	@Test(dataProvider = "int-inputStream-int")
	public void updateBinaryStream(int index, InputStream value, int length) throws SQLException
	{
		File file = new File("");
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
		
		this.sqlResultSet.updateBinaryStream(index, inputStream, length);
		
		this.control.replay();

		this.resultSet.updateBinaryStream(index, value, length);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateBinaryStream(java.lang.String, java.io.InputStream, int)
	 */
	@Test(dataProvider = "string-inputStream-int")
	public void updateBinaryStream(String name, InputStream value, int length) throws SQLException
	{
		File file = new File("");
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
		
		this.sqlResultSet.updateBinaryStream(name, inputStream, length);
		
		this.control.replay();

		this.resultSet.updateBinaryStream(name, value, length);
		
		this.control.verify();
	}

	@DataProvider(name = "int-blob")
	Object[][] intBlobProvider()
	{
		return new Object[][] { new Object[] { 1, EasyMock.createMock(Blob.class) } };
	}

	/**
	 * @see java.sql.ResultSet#updateBlob(int, java.sql.Blob)
	 */
	@Test(dataProvider = "int-blob")
	public void updateBlob(int index, Blob value) throws SQLException
	{
		File file = new File("");
		Blob blob = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.fileSupport.getBlob(file)).andReturn(blob);
		
		this.sqlResultSet.updateBlob(index, blob);
		
		this.control.replay();

		this.resultSet.updateBlob(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-blob")
	Object[][] stringBlobProvider()
	{
		return new Object[][] { new Object[] { "", EasyMock.createMock(Blob.class) } };
	}

	/**
	 * @see java.sql.ResultSet#updateBlob(java.lang.String, java.sql.Blob)
	 */
	@Test(dataProvider = "string-blob")
	public void updateBlob(String name, Blob value) throws SQLException
	{
		File file = new File("");
		Blob blob = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.fileSupport.getBlob(file)).andReturn(blob);
		
		this.sqlResultSet.updateBlob(name, blob);
		
		this.control.replay();

		this.resultSet.updateBlob(name, value);
		
		this.control.verify();
	}

	@DataProvider(name = "int-boolean")
	Object[][] intBooleanProvider()
	{
		return new Object[][] { new Object[] { 1, true } };
	}

	/**
	 * @see java.sql.ResultSet#updateBoolean(int, boolean)
	 */
	@Test(dataProvider = "int-boolean")
	public void updateBoolean(int index, boolean value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateBoolean(index, value);
		
		this.control.replay();

		this.resultSet.updateBoolean(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-boolean")
	Object[][] stringBooleanProvider()
	{
		return new Object[][] { new Object[] { "", true } };
	}

	/**
	 * @see java.sql.ResultSet#updateBoolean(java.lang.String, boolean)
	 */
	@Test(dataProvider = "string-boolean")
	public void updateBoolean(String name, boolean value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateBoolean(name, value);
		
		this.control.replay();

		this.resultSet.updateBoolean(name, value);
		
		this.control.verify();
	}

	@DataProvider(name = "int-byte")
	Object[][] intByteProvider()
	{
		return new Object[][] { new Object[] { 1, Integer.valueOf(1).byteValue() } };
	}

	/**
	 * @see java.sql.ResultSet#updateByte(int, byte)
	 */
	@Test(dataProvider = "int-byte")
	public void updateByte(int index, byte value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateByte(index, value);
		
		this.control.replay();

		this.resultSet.updateByte(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-byte")
	Object[][] stringByteProvider()
	{
		return new Object[][] { new Object[] { "", Integer.valueOf(1).byteValue() } };
	}

	/**
	 * @see java.sql.ResultSet#updateByte(java.lang.String, byte)
	 */
	@Test(dataProvider = "string-byte")
	public void updateByte(String name, byte value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateByte(name, value);
		
		this.control.replay();

		this.resultSet.updateByte(name, value);
		
		this.control.verify();
	}

	@DataProvider(name = "int-bytes")
	Object[][] intBytesProvider()
	{
		return new Object[][] { new Object[] { 1, new byte[0] } };
	}

	/**
	 * @see java.sql.ResultSet#updateBytes(int, byte[])
	 */
	@Test(dataProvider = "int-bytes")
	public void updateBytes(int index, byte[] value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateBytes(index, value);
		
		this.control.replay();

		this.resultSet.updateBytes(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-bytes")
	Object[][] stringBytesProvider()
	{
		return new Object[][] { new Object[] { "", new byte[0] } };
	}

	/**
	 * @see java.sql.ResultSet#updateBytes(java.lang.String, byte[])
	 */
	@Test(dataProvider = "string-bytes")
	public void updateBytes(String name, byte[] value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateBytes(name, value);
		
		this.control.replay();

		this.resultSet.updateBytes(name, value);
		
		this.control.verify();
	}

	@DataProvider(name = "int-reader-int")
	Object[][] intReaderIntProvider()
	{
		return new Object[][] { new Object[] { 1, new CharArrayReader(new char[0]), 0 } };
	}

	/**
	 * @see java.sql.ResultSet#updateCharacterStream(int, java.io.Reader, int)
	 */
	@Test(dataProvider = "int-reader-int")
	public void updateCharacterStream(int index, Reader value, int length) throws SQLException
	{
		File file = new File("");
		Reader reader = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader);
		
		this.sqlResultSet.updateCharacterStream(index, reader, length);
		
		this.control.replay();

		this.resultSet.updateCharacterStream(index, value, length);
		
		this.control.verify();
	}

	@DataProvider(name = "string-reader-int")
	Object[][] stringReaderIntProvider()
	{
		return new Object[][] { new Object[] { "", new CharArrayReader(new char[0]), 0 } };
	}

	/**
	 * @see java.sql.ResultSet#updateCharacterStream(java.lang.String, java.io.Reader, int)
	 */
	@Test(dataProvider = "string-reader-int")
	public void updateCharacterStream(String name, Reader value, int length) throws SQLException
	{
		File file = new File("");
		Reader reader = new CharArrayReader(new char[0]);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader);
		
		this.sqlResultSet.updateCharacterStream(name, reader, length);
		
		this.control.replay();

		this.resultSet.updateCharacterStream(name, value, length);
		
		this.control.verify();
	}

	@DataProvider(name = "int-clob")
	Object[][] intClobProvider()
	{
		return new Object[][] { new Object[] { 1, EasyMock.createMock(Clob.class) } };
	}

	/**
	 * @see java.sql.ResultSet#updateClob(int, java.sql.Clob)
	 */
	@Test(dataProvider = "int-clob")
	public void updateClob(int index, Clob value) throws SQLException
	{
		File file = new File("");
		Clob clob = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.fileSupport.getClob(file)).andReturn(clob);
		
		this.sqlResultSet.updateClob(index, clob);
		
		this.control.replay();

		this.resultSet.updateClob(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-clob")
	Object[][] stringClobProvider()
	{
		return new Object[][] { new Object[] { "", EasyMock.createMock(Clob.class) } };
	}

	/**
	 * @see java.sql.ResultSet#updateClob(java.lang.String, java.sql.Clob)
	 */
	@Test(dataProvider = "string-clob")
	public void updateClob(String name, Clob value) throws SQLException
	{
		File file = new File("");
		Clob clob = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.fileSupport.createFile(value)).andReturn(file);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		EasyMock.expect(this.fileSupport.getClob(file)).andReturn(clob);
		
		this.sqlResultSet.updateClob(name, clob);
		
		this.control.replay();

		this.resultSet.updateClob(name, value);
		
		this.control.verify();
	}

	@DataProvider(name = "int-date")
	Object[][] intDateProvider()
	{
		return new Object[][] { new Object[] { 1, new Date(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.ResultSet#updateDate(int, java.sql.Date)
	 */
	@Test(dataProvider = "int-date")
	public void updateDate(int index, Date value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateDate(index, value);
		
		this.control.replay();

		this.resultSet.updateDate(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-date")
	Object[][] stringDateProvider()
	{
		return new Object[][] { new Object[] { "", new Date(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.ResultSet#updateDate(java.lang.String, java.sql.Date)
	 */
	@Test(dataProvider = "string-date")
	public void updateDate(String name, Date value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateDate(name, value);
		
		this.control.replay();

		this.resultSet.updateDate(name, value);
		
		this.control.verify();
	}

	@DataProvider(name = "int-double")
	Object[][] intDoubleProvider()
	{
		return new Object[][] { new Object[] { 1, 1.0 } };
	}

	/**
	 * @see java.sql.ResultSet#updateDouble(int, double)
	 */
	@Test(dataProvider = "int-double")
	public void updateDouble(int index, double value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateDouble(index, value);
		
		this.control.replay();

		this.resultSet.updateDouble(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-double")
	Object[][] stringDoubleProvider()
	{
		return new Object[][] { new Object[] { "", 1.0 } };
	}

	/**
	 * @see java.sql.ResultSet#updateDouble(java.lang.String, double)
	 */
	@Test(dataProvider = "string-double")
	public void updateDouble(String name, double value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateDouble(name, value);
		
		this.control.replay();

		this.resultSet.updateDouble(name, value);
		
		this.control.verify();
	}

	@DataProvider(name = "int-float")
	Object[][] intFloatProvider()
	{
		return new Object[][] { new Object[] { 1, 1.0F } };
	}

	/**
	 * @see java.sql.ResultSet#updateFloat(int, float)
	 */
	@Test(dataProvider = "int-float")
	public void updateFloat(int index, float value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateFloat(index, value);
		
		this.control.replay();

		this.resultSet.updateFloat(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-float")
	Object[][] stringFloatProvider()
	{
		return new Object[][] { new Object[] { "", 1.0F } };
	}

	/**
	 * @see java.sql.ResultSet#updateFloat(java.lang.String, float)
	 */
	@Test(dataProvider = "string-float")
	public void updateFloat(String name, float value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateFloat(name, value);
		
		this.control.replay();

		this.resultSet.updateFloat(name, value);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateInt(int, int)
	 */
	@Test(dataProvider = "int-int")
	public void updateInt(int index, int value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateInt(index, value);
		
		this.control.replay();

		this.resultSet.updateInt(index, value);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateInt(java.lang.String, int)
	 */
	@Test(dataProvider = "string-int")
	public void updateInt(String name, int value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateInt(name, value);
		
		this.control.replay();

		this.resultSet.updateInt(name, value);
		
		this.control.verify();
	}

	@DataProvider(name = "int-long")
	Object[][] intLongProvider()
	{
		return new Object[][] { new Object[] { 1, 1L } };
	}

	/**
	 * @see java.sql.ResultSet#updateLong(int, long)
	 */
	@Test(dataProvider = "int-long")
	public void updateLong(int index, long value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateLong(index, value);
		
		this.control.replay();

		this.resultSet.updateLong(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-long")
	Object[][] stringLongProvider()
	{
		return new Object[][] { new Object[] { "", 1L } };
	}

	/**
	 * @see java.sql.ResultSet#updateLong(java.lang.String, long)
	 */
	@Test(dataProvider = "string-long")
	public void updateLong(String name, long value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateLong(name, value);
		
		this.control.replay();

		this.resultSet.updateLong(name, value);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateNull(int)
	 */
	@Test(dataProvider = "int")
	public void updateNull(int index) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateNull(index);
		
		this.control.replay();

		this.resultSet.updateNull(index);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateNull(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void updateNull(String name) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateNull(name);
		
		this.control.replay();

		this.resultSet.updateNull(name);
		
		this.control.verify();
	}

	@DataProvider(name = "int-object")
	Object[][] intObjectProvider()
	{
		return new Object[][] { new Object[] { 1, new Object() } };
	}

	/**
	 * @see java.sql.ResultSet#updateObject(int, java.lang.Object)
	 */
	@Test(dataProvider = "int-object")
	public void updateObject(int index, Object value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateObject(index, value);
		
		this.control.replay();

		this.resultSet.updateObject(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-object")
	Object[][] stringObjectProvider()
	{
		return new Object[][] { new Object[] { "", new Object() } };
	}

	/**
	 * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object)
	 */
	@Test(dataProvider = "string-object")
	public void updateObject(String name, Object value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateObject(name, value);
		
		this.control.replay();

		this.resultSet.updateObject(name, value);
		
		this.control.verify();
	}

	@DataProvider(name = "int-object-int")
	Object[][] intObjectIntProvider()
	{
		return new Object[][] { new Object[] { 1, new Object(), 1 } };
	}

	/**
	 * @see java.sql.ResultSet#updateObject(int, java.lang.Object, int)
	 */
	@Test(dataProvider = "int-object-int")
	public void updateObject(int index, Object value, int scale) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateObject(index, value, scale);
		
		this.control.replay();

		this.resultSet.updateObject(index, value, scale);
		
		this.control.verify();
	}

	@DataProvider(name = "string-object-int")
	Object[][] stringObjectIntProvider()
	{
		return new Object[][] { new Object[] { "", new Object(), 1 } };
	}

	/**
	 * @see java.sql.ResultSet#updateObject(java.lang.String, java.lang.Object, int)
	 */
	@Test(dataProvider = "string-object-int")
	public void updateObject(String name, Object value, int scale) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateObject(name, value, scale);
		
		this.control.replay();

		this.resultSet.updateObject(name, value, scale);
		
		this.control.verify();
	}

	@DataProvider(name = "int-ref")
	Object[][] intRefProvider()
	{
		return new Object[][] { new Object[] { 1, EasyMock.createMock(Ref.class) } };
	}

	/**
	 * @see java.sql.ResultSet#updateRef(int, java.sql.Ref)
	 */
	@Test(dataProvider = "int-ref")
	public void updateRef(int index, Ref value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateRef(index, value);
		
		this.control.replay();

		this.resultSet.updateRef(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-ref")
	Object[][] stringRefProvider()
	{
		return new Object[][] { new Object[] { "", EasyMock.createMock(Ref.class) } };
	}

	/**
	 * @see java.sql.ResultSet#updateRef(java.lang.String, java.sql.Ref)
	 */
	@Test(dataProvider = "string-ref")
	public void updateRef(String name, Ref value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateRef(name, value);
		
		this.control.replay();

		this.resultSet.updateRef(name, value);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#updateRow()
	 */
	@Test
	public void updateRow() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.getLockManager()).andReturn(this.lockManager);
		EasyMock.expect(this.lockManager.readLock(LockManager.GLOBAL)).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateRow();
		
		this.lock.unlock();
		
		this.control.replay();
		
		this.resultSet.updateRow();
		
		this.control.verify();
	}

	@DataProvider(name = "int-short")
	Object[][] intShortProvider()
	{
		return new Object[][] { new Object[] { 1, Integer.valueOf(1).shortValue() } };
	}

	/**
	 * @see java.sql.ResultSet#updateShort(int, short)
	 */
	@Test(dataProvider = "int-short")
	public void updateShort(int index, short value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateShort(index, value);
		
		this.control.replay();

		this.resultSet.updateShort(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-short")
	Object[][] stringShortProvider()
	{
		return new Object[][] { new Object[] { "", Integer.valueOf(1).shortValue() } };
	}

	/**
	 * @see java.sql.ResultSet#updateShort(java.lang.String, short)
	 */
	@Test(dataProvider = "string-short")
	public void updateShort(String name, short value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateShort(name, value);
		
		this.control.replay();

		this.resultSet.updateShort(name, value);
		
		this.control.verify();
	}

	@DataProvider(name = "int-string")
	Object[][] intStringProvider()
	{
		return new Object[][] { new Object[] { 1, "" } };
	}

	/**
	 * @see java.sql.ResultSet#updateString(int, java.lang.String)
	 */
	@Test(dataProvider = "int-string")
	public void updateString(int index, String value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateString(index, value);
		
		this.control.replay();

		this.resultSet.updateString(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-string")
	Object[][] stringStringProvider()
	{
		return new Object[][] { new Object[] { "", "" } };
	}

	/**
	 * @see java.sql.ResultSet#updateString(java.lang.String, java.lang.String)
	 */
	@Test(dataProvider = "string-string")
	public void updateString(String name, String value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateString(name, value);
		
		this.control.replay();

		this.resultSet.updateString(name, value);
		
		this.control.verify();
	}

	@DataProvider(name = "int-time")
	Object[][] intTimeProvider()
	{
		return new Object[][] { new Object[] { 1, new Time(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.ResultSet#updateTime(int, java.sql.Time)
	 */
	@Test(dataProvider = "int-time")
	public void updateTime(int index, Time value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateTime(index, value);
		
		this.control.replay();

		this.resultSet.updateTime(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-time")
	Object[][] stringTimeProvider()
	{
		return new Object[][] { new Object[] { "", new Time(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.ResultSet#updateTime(java.lang.String, java.sql.Time)
	 */
	@Test(dataProvider = "string-time")
	public void updateTime(String name, Time value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateTime(name, value);
		
		this.control.replay();

		this.resultSet.updateTime(name, value);
		
		this.control.verify();
	}

	@DataProvider(name = "int-timestamp")
	Object[][] intTimestampProvider()
	{
		return new Object[][] { new Object[] { 1, new Timestamp(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.ResultSet#updateTimestamp(int, java.sql.Timestamp)
	 */
	@Test(dataProvider = "int-timestamp")
	public void updateTimestamp(int index, Timestamp value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateTimestamp(index, value);
		
		this.control.replay();

		this.resultSet.updateTimestamp(index, value);
		
		this.control.verify();
	}

	@DataProvider(name = "string-timestamp")
	Object[][] stringTimestampProvider()
	{
		return new Object[][] { new Object[] { "", new Timestamp(System.currentTimeMillis()) } };
	}

	/**
	 * @see java.sql.ResultSet#updateTimestamp(java.lang.String, java.sql.Timestamp)
	 */
	@Test(dataProvider = "string-timestamp")
	public void updateTimestamp(String name, Timestamp value) throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		this.sqlResultSet.updateTimestamp(name, value);
		
		this.control.replay();

		this.resultSet.updateTimestamp(name, value);
		
		this.control.verify();
	}

	/**
	 * @see java.sql.ResultSet#wasNull()
	 */
	@Test
	public boolean wasNull() throws SQLException
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		EasyMock.expect(this.sqlResultSet.wasNull()).andReturn(true);
		
		this.control.replay();

		boolean result = this.resultSet.wasNull();
		
		this.control.verify();
		
		assert result;
		
		return result;
	}	
}
