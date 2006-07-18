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
import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
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
import java.sql.Types;
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
import net.sf.hajdbc.MockDatabase;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLObject;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/***
 * @author  Paul Ferraro
 * @since   1.0
 */
@Test
public class TestResultSet
{
	private IMocksControl control = EasyMock.createStrictControl();
	
	private DatabaseCluster databaseCluster = this.control.createMock(DatabaseCluster.class);
	
	protected java.sql.Connection sqlConnection = this.control.createMock(java.sql.Connection.class);
	
	protected java.sql.Statement sqlStatement = this.control.createMock(java.sql.Statement.class);
	
	protected java.sql.ResultSet sqlResultSet = this.control.createMock(java.sql.ResultSet.class);
	
	private Database database = new MockDatabase();
	
	private Balancer balancer = this.control.createMock(Balancer.class);
	
	private FileSupport fileSupport = this.control.createMock(FileSupport.class);
	
	private Lock lock = this.control.createMock(Lock.class);
	
	private Statement statement;
	private ResultSet resultSet;
	private List<Database> databaseList = Collections.singletonList(this.database);
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	
	@BeforeClass
	protected void setUp() throws Exception
	{
		Map map = Collections.singletonMap(this.database, new Object());
		
		EasyMock.expect(this.databaseCluster.getConnectionFactoryMap()).andReturn(map);
		
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		this.lock.lock();
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		this.lock.unlock();

		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		this.lock.lock();
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		this.lock.unlock();
		
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		this.lock.lock();
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		this.lock.unlock();
				
		this.control.replay();
		
		ConnectionFactory<Object> connectionFactory = new ConnectionFactory<Object>(this.databaseCluster, Object.class);
		
		Operation<Object, java.sql.Connection> operation = new Operation<Object, java.sql.Connection>()
		{
			public java.sql.Connection execute(Database database, Object sqlObject) throws SQLException
			{
				return TestResultSet.this.sqlConnection;
			}
		};
		
		Connection<Object> connection = new Connection<Object>(connectionFactory, operation, this.fileSupport);
		
		Operation<java.sql.Connection, java.sql.Statement> connectionOperation = new Operation<java.sql.Connection, java.sql.Statement>()
		{
			public java.sql.Statement execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return TestResultSet.this.sqlStatement;
			}
		};
		
		this.statement = new Statement(connection, connectionOperation);
		
		Operation<java.sql.Statement, java.sql.ResultSet> statementOperation = new Operation<java.sql.Statement, java.sql.ResultSet>()
		{
			public java.sql.ResultSet execute(Database database, java.sql.Statement statement) throws SQLException
			{
				return TestResultSet.this.sqlResultSet;
			}
		};
		
		this.resultSet = new ResultSet(this.statement, statementOperation);
		
		this.control.verify();
		this.control.reset();
	}
	
	@AfterMethod
	public void reset()
	{
		this.control.reset();
	}
	
	/**
	 * Test method for {@link SQLObject#getObject(Database)}
	 */
	public void testGetObject()
	{
		this.control.replay();
		
		Object resultSet = this.resultSet.getObject(this.database);
		
		this.control.verify();
		
		assert resultSet == this.sqlResultSet;
	}

	/**
	 * Test method for {@link SQLObject#getDatabaseCluster()}
	 */
	public void testGetDatabaseCluster()
	{
		this.control.replay();
		
		DatabaseCluster databaseCluster = this.resultSet.getDatabaseCluster();
		
		this.control.verify();
		
		assert databaseCluster == this.databaseCluster;
	}

	/**
	 * Test method for {@link SQLObject#handleExceptions(Map)}
	 */
	public void testHandleException()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlConnection.getAutoCommit()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.deactivate(this.database)).andReturn(false);
			
			this.control.replay();
			
			this.resultSet.handleExceptions(Collections.singletonMap(this.database, new SQLException()));
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link SQLObject#handleExceptions(Map)}
	 */
	public void testAutoCommitOffHandleException()
	{
		SQLException exception = new SQLException();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlConnection.getAutoCommit()).andReturn(false);
			
			this.databaseCluster.handleFailure(this.database, exception);
			
			this.control.replay();
			
			this.resultSet.handleExceptions(Collections.singletonMap(this.database, exception));
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
	
	/**
	 * Test method for {@link ResultSet#absolute(int)}
	 */
	public void testAbsolute()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.absolute(10)).andReturn(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			boolean valid = this.resultSet.absolute(10);
			
			this.control.verify();
			
			assert valid;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#afterLast()}
	 */
	public void testAfterLast()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.afterLast();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.afterLast();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#beforeFirst()}
	 */
	public void testBeforeFirst()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.beforeFirst();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.beforeFirst();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#cancelRowUpdates()}
	 */
	public void testCancelRowUpdates()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.cancelRowUpdates();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.cancelRowUpdates();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#clearWarnings()}
	 */
	public void testClearWarnings()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.clearWarnings();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.clearWarnings();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#close()}
	 */
	public void testClose()
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.close();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.close();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#deleteRow()}
	 */
	public void testDeleteRow()
	{
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.deleteRow();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.deleteRow();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#findColumn(String)}
	 */
	public void testFindColumn()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.findColumn("test")).andReturn(1);
			
			this.control.replay();
			
			int column = this.resultSet.findColumn("test");
			
			this.control.verify();
			
			assert column == 1 : column;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#first()}
	 */
	public void testFirst()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getType()).andReturn(ResultSet.TYPE_FORWARD_ONLY);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.sqlResultSet.first()).andReturn(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			boolean valid = this.resultSet.first();
			
			this.control.verify();
			
			assert valid;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getArray(int)}
	 */
	public void testGetArrayInt()
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getArray(1)).andReturn(array);
			
			this.control.replay();
			
			Array value = this.resultSet.getArray(1);
			
			this.control.verify();
			
			assert value == array;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getArray(String)}
	 */
	public void testGetArrayString()
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getArray("column")).andReturn(array);
			
			this.control.replay();
			
			Array value = this.resultSet.getArray("column");
			
			this.control.verify();
			
			assert value == array;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getAsciiStream(int)}
	 */
	public void testGetAsciiStreamInt()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getAsciiStream(1)).andReturn(inputStream);
			
			this.control.replay();
			
			InputStream value = this.resultSet.getAsciiStream(1);
			
			this.control.verify();
			
			assert value == inputStream;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getAsciiStream(String)}
	 */
	public void testGetAsciiStreamString()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getAsciiStream("column")).andReturn(inputStream);
			
			this.control.replay();
			
			InputStream value = this.resultSet.getAsciiStream("column");
			
			this.control.verify();
			
			assert value == inputStream;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getBigDecimal(int)}
	 */
	public void testGetBigDecimalInt()
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getBigDecimal(1)).andReturn(decimal);
			
			this.control.replay();
			
			BigDecimal value = this.resultSet.getBigDecimal(1);
			
			this.control.verify();
			
			assert value == decimal;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/***
	 * Test method for {@link ResultSet#getBigDecimal(int, int)}
	 * @deprecated
	 */
	public void testGetBigDecimalIntInt()
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getBigDecimal(1, 0)).andReturn(decimal);
			
			this.control.replay();
			
			BigDecimal value = this.resultSet.getBigDecimal(1, 0);
			
			this.control.verify();
			
			assert value == decimal;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getBigDecimal(String)}
	 */
	public void testGetBigDecimalString()
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getBigDecimal("column")).andReturn(decimal);
			
			this.control.replay();
			
			BigDecimal value = this.resultSet.getBigDecimal("column");
			
			this.control.verify();
			
			assert value == decimal;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/***
	 * Test method for {@link ResultSet#getBigDecimal(String, int)}
	 * @deprecated
	 */
	public void testGetBigDecimalStringInt()
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getBigDecimal("column", 0)).andReturn(decimal);
			
			this.control.replay();
			
			BigDecimal value = this.resultSet.getBigDecimal("column", 0);
			
			this.control.verify();
			
			assert value == decimal;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getBinaryStream(int)}
	 */
	public void testGetBinaryStreamInt()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getBinaryStream(1)).andReturn(inputStream);
			
			this.control.replay();
			
			InputStream value = this.resultSet.getBinaryStream(1);
			
			this.control.verify();
			
			assert value == inputStream;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getBinaryStream(String)}
	 */
	public void testGetBinaryStreamString()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getBinaryStream("column")).andReturn(inputStream);
			
			this.control.replay();
			
			InputStream value = this.resultSet.getBinaryStream("column");
			
			this.control.verify();
			
			assert value == inputStream;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getBlob(int)}
	 */
	public void testGetBlobInt()
	{
		Blob blob = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getBlob(1)).andReturn(blob);
			
			this.control.replay();
			
			Blob value = this.resultSet.getBlob(1);
			
			this.control.verify();
			
			assert value == blob;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getBlob(String)}
	 */
	public void testGetBlobString()
	{
		Blob blob = EasyMock.createMock(Blob.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getBlob("column")).andReturn(blob);
			
			this.control.replay();
			
			Blob value = this.resultSet.getBlob("column");
			
			this.control.verify();
			
			assert value == blob;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getBoolean(int)}
	 */
	public void testGetBooleanInt()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getBoolean(1)).andReturn(true);
			
			this.control.replay();
			
			boolean value = this.resultSet.getBoolean(1);
			
			this.control.verify();
			
			assert value;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getBoolean(String)}
	 */
	public void testGetBooleanString()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getBoolean("column")).andReturn(true);
			
			this.control.replay();
			
			boolean value = this.resultSet.getBoolean("column");
			
			this.control.verify();
			
			assert value;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getByte(int)}
	 */
	public void testGetByteInt()
	{
		byte b = 1;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getByte(1)).andReturn(b);
			
			this.control.replay();
			
			byte value = this.resultSet.getByte(1);
			
			this.control.verify();
			
			assert value == b;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getByte(String)}
	 */
	public void testGetByteString()
	{
		byte b = 1;
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getByte("column")).andReturn(b);
			
			this.control.replay();
			
			byte value = this.resultSet.getByte("column");
			
			this.control.verify();
			
			assert value == b;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getBytes(int)}
	 */
	public void testGetBytesInt()
	{
		byte[] bytes = new byte[] { 1 };
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getBytes(1)).andReturn(bytes);
			
			this.control.replay();
			
			byte[] value = this.resultSet.getBytes(1);
			
			this.control.verify();
			
			assert value == bytes;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getBytes(String)}
	 */
	public void testGetBytesString()
	{
		byte[] bytes = new byte[] { 1 };
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getBytes("column")).andReturn(bytes);
			
			this.control.replay();
			
			byte[] value = this.resultSet.getBytes("column");
			
			this.control.verify();
			
			assert value == bytes;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getCharacterStream(int)}
	 */
	public void testGetCharacterStreamInt()
	{
		Reader reader = new StringReader("");
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getCharacterStream(1)).andReturn(reader);
			
			this.control.replay();
			
			Reader value = this.resultSet.getCharacterStream(1);
			
			this.control.verify();
			
			assert value == reader;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getCharacterStream(String)}
	 */
	public void testGetCharacterStreamString()
	{
		Reader reader = new StringReader("");
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getCharacterStream("column")).andReturn(reader);
			
			this.control.replay();
			
			Reader value = this.resultSet.getCharacterStream("column");
			
			this.control.verify();
			
			assert value == reader;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getClob(int)}
	 */
	public void testGetClobInt()
	{
		Clob clob = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getClob(1)).andReturn(clob);
			
			this.control.replay();
			
			Clob value = this.resultSet.getClob(1);
			
			this.control.verify();
			
			assert value == clob;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getClob(String)}
	 */
	public void testGetClobString()
	{
		Clob clob = EasyMock.createMock(Clob.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getClob("column")).andReturn(clob);
			
			this.control.replay();
			
			Clob value = this.resultSet.getClob("column");
			
			this.control.verify();
			
			assert value == clob;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getConcurrency()}
	 */
	public void testGetConcurrency()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getConcurrency()).andReturn(ResultSet.CONCUR_UPDATABLE);
			
			this.control.replay();
			
			int concurrency = this.resultSet.getConcurrency();
			
			this.control.verify();
			
			assert concurrency == ResultSet.CONCUR_UPDATABLE : concurrency;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getCursorName()}
	 */
	public void testGetCursorName()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getCursorName()).andReturn("test");
			
			this.control.replay();
			
			String cursor = this.resultSet.getCursorName();
			
			this.control.verify();
			
			assert cursor == "test";
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getDate(int)}
	 */
	public void testGetDateInt()
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getDate(1)).andReturn(date);
			
			this.control.replay();
			
			Date value = this.resultSet.getDate(1);
			
			this.control.verify();
			
			assert value == date;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getDate(int, Calendar)}
	 */
	public void testGetDateIntCalendar()
	{
		Date date = new Date(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getDate(1, calendar)).andReturn(date);
			
			this.control.replay();
			
			Date value = this.resultSet.getDate(1, calendar);
			
			this.control.verify();
			
			assert value == date;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getDate(String)}
	 */
	public void testGetDateString()
	{
		Date date = new Date(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getDate("column")).andReturn(date);
			
			this.control.replay();
			
			Date value = this.resultSet.getDate("column");
			
			this.control.verify();
			
			assert value == date;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getDate(String, Calendar)}
	 */
	public void testGetDateStringCalendar()
	{
		Date date = new Date(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getDate("column", calendar)).andReturn(date);
			
			this.control.replay();
			
			Date value = this.resultSet.getDate("column", calendar);
			
			this.control.verify();
			
			assert value == date;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getDouble(int)}
	 */
	public void testGetDoubleInt()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getDouble(1)).andReturn(1.0);
			
			this.control.replay();
			
			double value = this.resultSet.getDouble(1);
			
			this.control.verify();
			
			assert value == 1.0;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getDouble(String)}
	 */
	public void testGetDoubleString()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getDouble("column")).andReturn(1.0);
			
			this.control.replay();
			
			double value = this.resultSet.getDouble("column");
			
			this.control.verify();
			
			assert value == 1.0;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getFetchDirection()}
	 */
	public void testGetFetchDirection()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getFetchDirection()).andReturn(ResultSet.FETCH_REVERSE);
			
			this.control.replay();
			
			int direction = this.resultSet.getFetchDirection();
			
			this.control.verify();
			
			assert direction == ResultSet.FETCH_REVERSE : direction;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getFetchSize()}
	 */
	public void testGetFetchSize()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getFetchSize()).andReturn(100);
			
			this.control.replay();
			
			int size = this.resultSet.getFetchSize();
			
			this.control.verify();
			
			assert size == 100 : size;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getFloat(int)}
	 */
	public void testGetFloatInt()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getFloat(1)).andReturn(1.0f);
			
			this.control.replay();
			
			float value = this.resultSet.getFloat(1);
			
			this.control.verify();
			
			assert value == 1.0f;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getFloat(String)}
	 */
	public void testGetFloatString()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getFloat("column")).andReturn(1.0f);
			
			this.control.replay();
			
			float value = this.resultSet.getFloat("column");
			
			this.control.verify();
			
			assert value == 1.0f;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getInt(int)}
	 */
	public void testGetIntInt()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getInt(1)).andReturn(1);
			
			this.control.replay();
			
			int value = this.resultSet.getInt(1);
			
			this.control.verify();
			
			assert value == 1;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getInt(String)}
	 */
	public void testGetIntString()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getInt("column")).andReturn(1);
			
			this.control.replay();
			
			int value = this.resultSet.getInt("column");
			
			this.control.verify();
			
			assert value == 1;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getLong(int)}
	 */
	public void testGetLongInt()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getLong(1)).andReturn(100L);
			
			this.control.replay();
			
			long value = this.resultSet.getLong(1);
			
			this.control.verify();
			
			assert value == 100L;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getLong(String)}
	 */
	public void testGetLongString()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getLong("column")).andReturn(100L);
			
			this.control.replay();
			
			long value = this.resultSet.getLong("column");
			
			this.control.verify();
			
			assert value == 100L;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getMetaData()}
	 */
	public void testGetMetaData()
	{
		ResultSetMetaData resultSetMetaData = EasyMock.createMock(ResultSetMetaData.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getMetaData()).andReturn(resultSetMetaData);
			
			this.control.replay();
			
			ResultSetMetaData metaData = this.resultSet.getMetaData();
			
			this.control.verify();
			
			assert metaData == resultSetMetaData;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getObject(int)}
	 */
	public void testGetObjectInt()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getObject(1)).andReturn(object);
			
			this.control.replay();
			
			Object value = this.resultSet.getObject(1);
			
			this.control.verify();
			
			assert value == object;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getObject(int, Map)}
	 */
	public void testGetObjectIntMap()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getObject(1, Collections.EMPTY_MAP)).andReturn(object);
			
			this.control.replay();
			
			Object value = this.resultSet.getObject(1, Collections.EMPTY_MAP);
			
			this.control.verify();
			
			assert value == object;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getObject(String)}
	 */
	public void testGetObjectString()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getObject("column")).andReturn(object);
			
			this.control.replay();
			
			Object value = this.resultSet.getObject("column");
			
			this.control.verify();
			
			assert value == object;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getObject(String, Map)}
	 */
	public void testGetObjectStringMap()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getObject("column", Collections.EMPTY_MAP)).andReturn(object);
			
			this.control.replay();
			
			Object value = this.resultSet.getObject("column", Collections.EMPTY_MAP);
			
			this.control.verify();
			
			assert value == object;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getRef(int)}
	 */
	public void testGetRefInt()
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getRef(1)).andReturn(ref);
			
			this.control.replay();
			
			Ref value = this.resultSet.getRef(1);
			
			this.control.verify();
			
			assert value == ref;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getRef(String)}
	 */
	public void testGetRefString()
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getRef("column")).andReturn(ref);
			
			this.control.replay();
			
			Ref value = this.resultSet.getRef("column");
			
			this.control.verify();
			
			assert value == ref;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getRow()}
	 */
	public void testGetRow()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getRow()).andReturn(10);
			
			this.control.replay();
			
			int row = this.resultSet.getRow();
			
			this.control.verify();
			
			assert row == 10 : row;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getShort(int)}
	 */
	public void testGetShortInt()
	{
		short s = Integer.valueOf(1).shortValue();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getShort(1)).andReturn(s);
			
			this.control.replay();
			
			short value = this.resultSet.getShort(1);
			
			this.control.verify();
			
			assert value == s;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getShort(String)}
	 */
	public void testGetShortString()
	{
		short s = Integer.valueOf(1).shortValue();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getShort("column")).andReturn(s);
			
			this.control.replay();
			
			short value = this.resultSet.getShort("column");
			
			this.control.verify();
			
			assert value == s;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getStatement()}
	 */
	public void testGetStatement()
	{
		java.sql.Statement statement = this.resultSet.getStatement();
		
		assert statement == this.statement;
	}

	/**
	 * Test method for {@link ResultSet#getString(int)}
	 */
	public void testGetStringInt()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getString(1)).andReturn("test");
			
			this.control.replay();
			
			String value = this.resultSet.getString(1);
			
			this.control.verify();
			
			assert value == "test";
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getString(String)}
	 */
	public void testGetStringString()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getString("column")).andReturn("test");
			
			this.control.replay();
			
			String value = this.resultSet.getString("column");
			
			this.control.verify();
			
			assert value == "test";
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getTime(int)}
	 */
	public void testGetTimeInt()
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getTime(1)).andReturn(time);
			
			this.control.replay();
			
			Time value = this.resultSet.getTime(1);
			
			this.control.verify();
			
			assert value == time;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getTime(int, Calendar)}
	 */
	public void testGetTimeIntCalendar()
	{
		Time time = new Time(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getTime(1, calendar)).andReturn(time);
			
			this.control.replay();
			
			Time value = this.resultSet.getTime(1, calendar);
			
			this.control.verify();
			
			assert value == time;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getTime(String)}
	 */
	public void testGetTimeString()
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getTime("column")).andReturn(time);
			
			this.control.replay();
			
			Time value = this.resultSet.getTime("column");
			
			this.control.verify();
			
			assert value == time;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getTime(String, Calendar)}
	 */
	public void testGetTimeStringCalendar()
	{
		Time time = new Time(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getTime("column", calendar)).andReturn(time);
			
			this.control.replay();
			
			Time value = this.resultSet.getTime("column", calendar);
			
			this.control.verify();
			
			assert value == time;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getTimestamp(int)}
	 */
	public void testGetTimestampInt()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getTimestamp(1)).andReturn(timestamp);
			
			this.control.replay();
			
			Timestamp value = this.resultSet.getTimestamp(1);
			
			this.control.verify();
			
			assert value == timestamp;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getTimestamp(int, Calendar)}
	 */
	public void testGetTimestampIntCalendar()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getTimestamp(1, calendar)).andReturn(timestamp);
			
			this.control.replay();
			
			Timestamp value = this.resultSet.getTimestamp(1, calendar);
			
			this.control.verify();
			
			assert value == timestamp;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getTimestamp(String)}
	 */
	public void testGetTimestampString()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getTimestamp("column")).andReturn(timestamp);
			
			this.control.replay();
			
			Timestamp value = this.resultSet.getTimestamp("column");
			
			this.control.verify();
			
			assert value == timestamp;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getTimestamp(String, Calendar)}
	 */
	public void testGetTimestampStringCalendar()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		Calendar calendar = Calendar.getInstance();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getTimestamp("column", calendar)).andReturn(timestamp);
			
			this.control.replay();
			
			Timestamp value = this.resultSet.getTimestamp("column", calendar);
			
			this.control.verify();
			
			assert value == timestamp;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getType()}
	 */
	public void testGetType()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getType()).andReturn(ResultSet.TYPE_SCROLL_INSENSITIVE);
			
			this.control.replay();
			
			int type = this.resultSet.getType();
			
			this.control.verify();
			
			assert type == ResultSet.TYPE_SCROLL_INSENSITIVE : type;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/***
	 * Test method for {@link ResultSet#getUnicodeStream(int)}
	 * @deprecated
	 */
	public void testGetUnicodeStreamInt()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getUnicodeStream(1)).andReturn(inputStream);
			
			this.control.replay();
			
			InputStream value = this.resultSet.getUnicodeStream(1);
			
			this.control.verify();
			
			assert value == inputStream;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/***
	 * Test method for {@link ResultSet#getUnicodeStream(String)}
	 * @deprecated
	 */
	public void testGetUnicodeStreamString()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[0]);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getUnicodeStream("column")).andReturn(inputStream);
			
			this.control.replay();
			
			InputStream value = this.resultSet.getUnicodeStream("column");
			
			this.control.verify();
			
			assert value == inputStream;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getURL(int)}
	 */
	public void testGetURLInt()
	{
		try
		{
			URL url = new URL("http://www.google.com");
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.first()).andReturn(this.database);
			
			EasyMock.expect(this.sqlResultSet.getURL(1)).andReturn(url);
			
			this.control.replay();
			
			URL value = this.resultSet.getURL(1);
			
			this.control.verify();
			
			assert value == url;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
		catch (MalformedURLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getURL(String)}
	 */
	public void testGetURLString()
	{
		try
		{
			URL url = new URL("http://www.google.com");
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.first()).andReturn(this.database);
			
			EasyMock.expect(this.sqlResultSet.getURL("column")).andReturn(url);
			
			this.control.replay();
			
			URL value = this.resultSet.getURL("column");
			
			this.control.verify();
			
			assert value == url;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
		catch (MalformedURLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#getWarnings()}
	 */
	public void testGetWarnings()
	{
		SQLWarning warnings = new SQLWarning();
		
		try
		{
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.first()).andReturn(this.database);
			
			EasyMock.expect(this.sqlResultSet.getWarnings()).andReturn(warnings);
			
			this.control.replay();
			
			SQLWarning value = this.resultSet.getWarnings();
			
			this.control.verify();
			
			assert value == warnings;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#insertRow()}
	 */
	public void testInsertRow()
	{
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.insertRow();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.insertRow();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#isAfterLast()}
	 */
	public void testIsAfterLast()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.isAfterLast()).andReturn(true);
			
			this.control.replay();
			
			boolean afterLast = this.resultSet.isAfterLast();
			
			this.control.verify();
			
			assert afterLast;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#isBeforeFirst()}
	 */
	public void testIsBeforeFirst()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.isBeforeFirst()).andReturn(true);
			
			this.control.replay();
			
			boolean beforeFirst = this.resultSet.isBeforeFirst();
			
			this.control.verify();
			
			assert beforeFirst;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#isFirst()}
	 */
	public void testIsFirst()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getType()).andReturn(ResultSet.TYPE_FORWARD_ONLY);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.first()).andReturn(this.database);
			
			EasyMock.expect(this.sqlResultSet.isFirst()).andReturn(true);
			
			this.control.replay();
			
			boolean first = this.resultSet.isFirst();
			
			this.control.verify();
			
			assert first;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#isFirst()}
	 */
	public void testScrollSensitiveIsFirst()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getType()).andReturn(ResultSet.TYPE_SCROLL_SENSITIVE);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);
			
			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlResultSet.isFirst()).andReturn(true);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			boolean first = this.resultSet.isFirst();
			
			this.control.verify();
			
			assert first;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#isLast()}
	 */
	public void testIsLast()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getType()).andReturn(ResultSet.TYPE_FORWARD_ONLY);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.first()).andReturn(this.database);
			
			EasyMock.expect(this.sqlResultSet.isLast()).andReturn(true);
			
			this.control.replay();
			
			boolean last = this.resultSet.isLast();
			
			this.control.verify();
			
			assert last;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#isLast()}
	 */
	public void testScrollSensitiveIsLast()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getType()).andReturn(ResultSet.TYPE_SCROLL_SENSITIVE);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);
			
			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlResultSet.isLast()).andReturn(true);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			boolean last = this.resultSet.isLast();
			
			this.control.verify();
			
			assert last;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#last()}
	 */
	public void testLast()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getType()).andReturn(ResultSet.TYPE_FORWARD_ONLY);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.sqlResultSet.last()).andReturn(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			boolean valid = this.resultSet.last();
			
			this.control.verify();
			
			assert valid;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#moveToCurrentRow()}
	 */
	public void testMoveToCurrentRow()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getType()).andReturn(ResultSet.TYPE_FORWARD_ONLY);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.sqlResultSet.moveToCurrentRow();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.moveToCurrentRow();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#moveToInsertRow()}
	 */
	public void testMoveToInsertRow()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.moveToInsertRow();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.moveToInsertRow();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#next()}
	 */
	public void testNext()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getType()).andReturn(ResultSet.TYPE_FORWARD_ONLY);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);

			EasyMock.expect(this.sqlResultSet.next()).andReturn(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			boolean valid = this.resultSet.next();
			
			this.control.verify();
			
			assert valid;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#previous()}
	 */
	public void testPrevious()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.getType()).andReturn(ResultSet.TYPE_FORWARD_ONLY);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.sqlResultSet.previous()).andReturn(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			boolean valid = this.resultSet.previous();
			
			this.control.verify();
			
			assert valid;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#refreshRow()}
	 */
	public void testRefreshRow()
	{
		EasyMock.expect(this.databaseCluster.getNonTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.refreshRow();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.refreshRow();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#relative(int)}
	 */
	public void testRelative()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.relative(10)).andReturn(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			boolean valid = this.resultSet.relative(10);
			
			this.control.verify();
			
			assert valid;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#rowDeleted()}
	 */
	public void testRowDeleted()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.rowDeleted()).andReturn(true);
			
			this.control.replay();
			
			boolean deleted = this.resultSet.rowDeleted();
			
			this.control.verify();
			
			assert deleted;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#rowInserted()}
	 */
	public void testRowInserted()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.rowUpdated()).andReturn(true);
			
			this.control.replay();
			
			boolean updated = this.resultSet.rowUpdated();
			
			this.control.verify();
			
			assert updated;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#rowUpdated()}
	 */
	public void testRowUpdated()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.rowInserted()).andReturn(true);
			
			this.control.replay();
			
			boolean inserted = this.resultSet.rowInserted();
			
			this.control.verify();
			
			assert inserted;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#setFetchDirection(int)}
	 */
	public void testSetFetchDirection()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.setFetchDirection(ResultSet.FETCH_REVERSE);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.setFetchDirection(ResultSet.FETCH_REVERSE);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#setFetchSize(int)}
	 */
	public void testSetFetchSize()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.setFetchSize(100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.setFetchSize(100);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateArray(int, Array)}
	 */
	public void testUpdateArrayIntArray()
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateArray(1, array);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateArray(1, array);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateArray(String, Array)}
	 */
	public void testUpdateArrayStringArray()
	{
		Array array = EasyMock.createMock(Array.class);
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateArray("column", array);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateArray("column", array);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateAsciiStream(int, InputStream, int)}
	 */
	public void testUpdateAsciiStreamIntInputStreamInt()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
		File file = new File("");
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
			
			this.sqlResultSet.updateAsciiStream(1, inputStream, 100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateAsciiStream(1, inputStream, 100);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateAsciiStream(String, InputStream, int)}
	 */
	public void testUpdateAsciiStreamStringInputStreamInt()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
		File file = new File("");
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
			
			this.sqlResultSet.updateAsciiStream("column", inputStream, 100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateAsciiStream("column", inputStream, 100);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateBigDecimal(int, BigDecimal)}
	 */
	public void testUpdateBigDecimalIntBigDecimal()
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateBigDecimal(1, decimal);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateBigDecimal(1, decimal);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateBigDecimal(String, BigDecimal)}
	 */
	public void testUpdateBigDecimalStringBigDecimal()
	{
		BigDecimal decimal = new BigDecimal(1.0);
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateBigDecimal("column", decimal);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateBigDecimal("column", decimal);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateBinaryStream(int, InputStream, int)}
	 */
	public void testUpdateBinaryStreamIntInputStreamInt()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
		File file = new File("");
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
			
			this.sqlResultSet.updateBinaryStream(1, inputStream, 100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateBinaryStream(1, inputStream, 100);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateBinaryStream(String, InputStream, int)}
	 */
	public void testUpdateBinaryStreamStringInputStreamInt()
	{
		InputStream inputStream = new ByteArrayInputStream(new byte[] { 1 });
		File file = new File("");
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(inputStream)).andReturn(file);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.fileSupport.getInputStream(file)).andReturn(inputStream);
			
			this.sqlResultSet.updateBinaryStream("column", inputStream, 100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateBinaryStream("column", inputStream, 100);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateBlob(int, Blob)}
	 */
	public void testUpdateBlobIntBlob()
	{
		Blob blob = EasyMock.createMock(Blob.class);
		File file = new File("");
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(blob)).andReturn(file);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.fileSupport.getBlob(file)).andReturn(blob);
			
			this.sqlResultSet.updateBlob(1, blob);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateBlob(1, blob);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateBlob(String, Blob)}
	 */
	public void testUpdateBlobStringBlob()
	{
		Blob blob = EasyMock.createMock(Blob.class);
		File file = new File("");
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(blob)).andReturn(file);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.fileSupport.getBlob(file)).andReturn(blob);
			
			this.sqlResultSet.updateBlob("column", blob);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateBlob("column", blob);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateBoolean(int, boolean)}
	 */
	public void testUpdateBooleanIntBoolean()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateBoolean(1, true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateBoolean(1, true);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateBoolean(String, boolean)}
	 */
	public void testUpdateBooleanStringBoolean()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateBoolean("column", true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateBoolean("column", true);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateByte(int, byte)}
	 */
	public void testUpdateByteIntByte()
	{
		byte value = 1;
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateByte(1, value);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateByte(1, value);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateByte(String, byte)}
	 */
	public void testUpdateByteStringByte()
	{
		byte value = 1;
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateByte("column", value);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateByte("column", value);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateBytes(int, byte[])}
	 */
	public void testUpdateBytesIntByteArray()
	{
		byte[] value = new byte[] { 1 };
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateBytes(1, value);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateBytes(1, value);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateBytes(String, byte[])}
	 */
	public void testUpdateBytesStringByteArray()
	{
		byte[] value = new byte[] { 1 };
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateBytes("column", value);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateBytes("column", value);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateCharacterStream(int, Reader, int)}
	 */
	public void testUpdateCharacterStreamIntReaderInt()
	{
		Reader reader = new StringReader("test");
		File file = new File("");
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(reader)).andReturn(file);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader);
			
			this.sqlResultSet.updateCharacterStream(1, reader, 100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateCharacterStream(1, reader, 100);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateCharacterStream(String, Reader, int)}
	 */
	public void testUpdateCharacterStreamStringReaderInt()
	{
		Reader reader = new StringReader("test");
		File file = new File("");
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(reader)).andReturn(file);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.fileSupport.getReader(file)).andReturn(reader);
			
			this.sqlResultSet.updateCharacterStream("column", reader, 100);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateCharacterStream("column", reader, 100);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateClob(int, Clob)}
	 */
	public void testUpdateClobIntClob()
	{
		Clob clob = EasyMock.createMock(Clob.class);
		File file = new File("");
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(clob)).andReturn(file);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.fileSupport.getClob(file)).andReturn(clob);
			
			this.sqlResultSet.updateClob(1, clob);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateClob(1, clob);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateClob(String, Clob)}
	 */
	public void testUpdateClobStringClob()
	{
		Clob clob = EasyMock.createMock(Clob.class);
		File file = new File("");
		
		try
		{
			EasyMock.expect(this.fileSupport.createFile(clob)).andReturn(file);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.fileSupport.getClob(file)).andReturn(clob);
			
			this.sqlResultSet.updateClob("column", clob);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateClob("column", clob);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateDate(int, Date)}
	 */
	public void testUpdateDateIntDate()
	{
		Date date = new Date(System.currentTimeMillis());

		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateDate(1, date);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateDate(1, date);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateDate(String, Date)}
	 */
	public void testUpdateDateStringDate()
	{
		Date date = new Date(System.currentTimeMillis());

		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateDate("column", date);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateDate("column", date);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateDouble(int, double)}
	 */
	public void testUpdateDoubleIntDouble()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateDouble(1, 1.1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateDouble(1, 1.1);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateDouble(String, double)}
	 */
	public void testUpdateDoubleStringDouble()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateDouble("column", 1.1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateDouble("column", 1.1);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateFloat(int, float)}
	 */
	public void testUpdateFloatIntFloat()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateFloat(1, 1.1f);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateFloat(1, 1.1f);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateFloat(String, float)}
	 */
	public void testUpdateFloatStringFloat()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateFloat("column", 1.1f);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateFloat("column", 1.1f);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateInt(int, int)}
	 */
	public void testUpdateIntIntInt()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateInt(1, 1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateInt(1, 1);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateInt(String, int)}
	 */
	public void testUpdateIntStringInt()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateInt("column", 1);;
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateInt("column", 1);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateLong(int, long)}
	 */
	public void testUpdateLongIntLong()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateLong(1, 1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateLong(1, 1);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateLong(String, long)}
	 */
	public void testUpdateLongStringLong()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateLong("column", 1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateLong("column", 1);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateNull(int)}
	 */
	public void testUpdateNullInt()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateNull(1);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateNull(1);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateNull(String)}
	 */
	public void testUpdateNullString()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateNull("column");
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateNull("column");
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateObject(int, Object)}
	 */
	public void testUpdateObjectIntObject()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateObject(1, object);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateObject(1, object);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateObject(int, Object, int)}
	 */
	public void testUpdateObjectIntObjectInt()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateObject(1, object, Types.ARRAY);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateObject(1, object, Types.ARRAY);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateObject(String, Object)}
	 */
	public void testUpdateObjectStringObject()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateObject("column", object);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateObject("column", object);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateObject(String, Object, int)}
	 */
	public void testUpdateObjectStringObjectInt()
	{
		Object object = new Object();
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateObject("column", object, Types.ARRAY);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateObject("column", object, Types.ARRAY);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateRef(int, Ref)}
	 */
	public void testUpdateRefIntRef()
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateObject(1, ref);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateObject(1, ref);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateRef(String, Ref)}
	 */
	public void testUpdateRefStringRef()
	{
		Ref ref = EasyMock.createMock(Ref.class);
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateObject("column", ref);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateObject("column", ref);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateRow()}
	 */
	public void testUpdateRow()
	{
		EasyMock.expect(this.databaseCluster.getTransactionalExecutor()).andReturn(this.executor);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateRow();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateRow();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateShort(int, short)}
	 */
	public void testUpdateShortIntShort()
	{
		short s = Integer.valueOf(1).shortValue();
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateShort(1, s);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateShort(1, s);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateShort(String, short)}
	 */
	public void testUpdateShortStringShort()
	{
		short s = Integer.valueOf(1).shortValue();
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateShort("column", s);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateShort("column", s);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateString(int, String)}
	 */
	public void testUpdateStringIntString()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateString(1, "test");
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateString(1, "test");
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateString(String, String)}
	 */
	public void testUpdateStringStringString()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateString("column", "test");
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateString("column", "test");
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateTime(int, Time)}
	 */
	public void testUpdateTimeIntTime()
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateTime(1, time);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateTime(1, time);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateTime(String, Time)}
	 */
	public void testUpdateTimeStringTime()
	{
		Time time = new Time(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateTime("column", time);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateTime("column", time);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateTimestamp(int, Timestamp)}
	 */
	public void testUpdateTimestampIntTimestamp()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateTimestamp(1, timestamp);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateTimestamp(1, timestamp);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#updateTimestamp(String, Timestamp)}
	 */
	public void testUpdateTimestampStringTimestamp()
	{
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlResultSet.updateTimestamp("column", timestamp);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.resultSet.updateTimestamp("column", timestamp);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link ResultSet#wasNull()}
	 */
	public void testWasNull()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlResultSet.wasNull()).andReturn(true);
			
			this.control.replay();
			
			boolean wasNull = this.resultSet.wasNull();
			
			this.control.verify();
			
			assert wasNull;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
}
