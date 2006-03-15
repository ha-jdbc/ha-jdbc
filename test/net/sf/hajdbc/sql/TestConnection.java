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
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Operation;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.Configuration;
import org.testng.annotations.Test;

/**
 * Unit test for {@link Connection}
 * @author  Paul Ferraro
 * @since   1.0
 */
@Test
public class TestConnection
{
	private IMocksControl control = EasyMock.createControl();
	private DatabaseCluster databaseCluster = this.control.createMock(DatabaseCluster.class);
	private java.sql.Connection sqlConnection = this.control.createMock(java.sql.Connection.class);
	private Database database = this.control.createMock(Database.class);
	private Balancer balancer = this.control.createMock(Balancer.class);
	private FileSupport fileSupport = this.control.createMock(FileSupport.class);
	private Connection<java.sql.Connection> connection;
	private Lock lock = this.control.createMock(Lock.class);
	private List<Database> databaseList = Collections.singletonList(this.database);
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	
	@Configuration(beforeTestClass = true)
	public void init() throws Exception
	{
		Map map = Collections.singletonMap(this.database, this.sqlConnection);
		
		EasyMock.expect(this.databaseCluster.getConnectionFactoryMap()).andReturn(map);
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		this.lock.lock();
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
		this.lock.unlock();
		
		this.control.replay();
		
		ConnectionFactory<java.sql.Connection> connectionFactory = new ConnectionFactory<java.sql.Connection>(this.databaseCluster, java.sql.Connection.class);
		
		Operation<java.sql.Connection, java.sql.Connection> operation = new Operation<java.sql.Connection, java.sql.Connection>()
		{
			public java.sql.Connection execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return connection;
			}
		};
		
		this.connection = new Connection<java.sql.Connection>(connectionFactory, operation, this.fileSupport);

		this.control.verify();
		this.control.reset();
	}

	@Configuration(afterTestMethod = true)
	public void reset()
	{
		this.control.reset();
	}
	
	/**
	 * Test method for {@link Connection#getObject(Database)}
	 */
	public void testGetObject()
	{
		this.control.replay();
		
		Object connection = this.connection.getObject(this.database);
		
		this.control.verify();
		
		assert this.sqlConnection == connection;
	}

	/**
	 * Test method for {@link Connection#getDatabaseCluster()}
	 */
	public void testGetDatabaseCluster()
	{
		this.control.replay();
		
		DatabaseCluster databaseCluster = this.connection.getDatabaseCluster();
		
		this.control.verify();
		
		assert this.databaseCluster == databaseCluster;
	}

	/**
	 * Test method for {@link Connection#handleExceptions(Map)}
	 */
	public void testHandleExceptions()
	{
		try
		{
			EasyMock.expect(this.databaseCluster.deactivate(this.database)).andReturn(Boolean.FALSE);
			
			this.control.replay();
			
			this.connection.handleExceptions(Collections.singletonMap(this.database, new SQLException()));
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
	
	/**
	 * Test method for {@link Connection#clearWarnings()}
	 */
	public void testClearWarnings()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.clearWarnings();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.connection.clearWarnings();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#close()}
	 */
	public void testClose()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
		
		try
		{
			this.sqlConnection.close();
			
			this.lock.unlock();
			
			this.fileSupport.close();
			
			this.control.replay();
			
			this.connection.close();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#commit()}
	 */
	public void testCommit()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
		
		try
		{
			this.sqlConnection.commit();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.connection.commit();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#createStatement()}
	 */
	public void testCreateStatement()
	{
		Statement sqlStatement = EasyMock.createMock(Statement.class);

		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(Boolean.FALSE);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);		
			
			EasyMock.expect(this.sqlConnection.createStatement()).andReturn(sqlStatement);
			
			this.lock.unlock();
			
			this.control.replay();
			
			Statement statement = this.connection.createStatement();
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.Statement.class.cast(statement).getObject(this.database) == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#createStatement()}
	 */
	public void testReadOnlyCreateStatement()
	{
		Statement sqlStatement = EasyMock.createMock(Statement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(Boolean.TRUE);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.first()).andReturn(this.database);
			
			EasyMock.expect(this.sqlConnection.createStatement()).andReturn(sqlStatement);

			this.control.replay();
			
			Statement statement = this.connection.createStatement();
			
			this.control.verify();
			
			assert sqlStatement == statement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#createStatement(int, int)}
	 */
	public void testCreateStatementIntInt()
	{
		Statement sqlStatement = EasyMock.createMock(Statement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
			
			EasyMock.expect(this.sqlConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)).andReturn(sqlStatement);
			
			this.lock.unlock();
			
			this.control.replay();
			
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.Statement.class.cast(statement).getObject(this.database) == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#createStatement(int, int)}
	 */
	public void testReadOnlyCreateStatementIntInt()
	{
		Statement sqlStatement = EasyMock.createMock(Statement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.first()).andReturn(this.database);

			EasyMock.expect(this.sqlConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)).andReturn(sqlStatement);
			
			this.control.replay();
			
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.control.verify();
			
			assert sqlStatement == statement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#createStatement(int, int, int)}
	 */
	public void testCreateStatementIntIntInt()
	{
		Statement sqlStatement = EasyMock.createMock(Statement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
			
			EasyMock.expect(this.sqlConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE)).andReturn(sqlStatement);
			
			this.lock.unlock();
			
			this.control.replay();
			
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.Statement.class.cast(statement).getObject(this.database) == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#createStatement(int, int, int)}
	 */
	public void testReadOnlyCreateStatementIntIntInt()
	{
		Statement sqlStatement = EasyMock.createMock(Statement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
			EasyMock.expect(this.balancer.first()).andReturn(this.database);

			EasyMock.expect(this.sqlConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE)).andReturn(sqlStatement);
			
			this.control.replay();
			
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.control.verify();
			
			assert sqlStatement == statement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#getAutoCommit()}
	 */
	public void testGetAutoCommit()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlConnection.getAutoCommit()).andReturn(true);
			
			this.control.replay();
			
			boolean autoCommit = this.connection.getAutoCommit();
			
			this.control.verify();
			
			assert autoCommit;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#getCatalog()}
	 */
	public void testGetCatalog()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		try
		{
			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.getCatalog()).andReturn("test");
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			String catalog = this.connection.getCatalog();
			
			this.control.verify();
			
			assert catalog.equals("test") : catalog;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#getHoldability()}
	 */
	public void testGetHoldability()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlConnection.getHoldability()).andReturn(ResultSet.HOLD_CURSORS_OVER_COMMIT);
			
			this.control.replay();
			
			int holdability = this.connection.getHoldability();
			
			this.control.verify();
			
			assert holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT : holdability;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#getMetaData()}
	 */
	public void testGetMetaData()
	{
		DatabaseMetaData databaseMetaData = EasyMock.createMock(DatabaseMetaData.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		try
		{
			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.getMetaData()).andReturn(databaseMetaData);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			DatabaseMetaData metaData = this.connection.getMetaData();
			
			this.control.verify();
			
			assert databaseMetaData == metaData;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#getTransactionIsolation()}
	 */
	public void testGetTransactionIsolation()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.next()).andReturn(this.database);
		
		try
		{
			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.getTransactionIsolation()).andReturn(Connection.TRANSACTION_NONE);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			int transactionIsolation = this.connection.getTransactionIsolation();
			
			this.control.verify();
			
			assert transactionIsolation == Connection.TRANSACTION_NONE : transactionIsolation;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#getTypeMap()}
	 */
	public void testGetTypeMap()
	{
		Map typeMap = EasyMock.createMock(Map.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlConnection.getTypeMap()).andReturn(typeMap);
			
			this.control.replay();
			
			Map map = this.connection.getTypeMap();
			
			this.control.verify();
			
			assert map == typeMap;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#getWarnings()}
	 */
	public void testGetWarnings()
	{
		SQLWarning sqlWarning = new SQLWarning();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlConnection.getWarnings()).andReturn(sqlWarning);
			
			this.control.replay();
			
			SQLWarning warning = this.connection.getWarnings();
			
			this.control.verify();
			
			assert warning == sqlWarning;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#isClosed()}
	 */
	public void testIsClosed()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlConnection.isClosed()).andReturn(true);
			
			this.control.replay();
			
			boolean closed = this.connection.isClosed();
			
			this.control.verify();
			
			assert closed;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#isReadOnly()}
	 */
	public void testIsReadOnly()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			this.control.replay();
			
			boolean readOnly = this.connection.isReadOnly();
			
			this.control.verify();
			
			assert readOnly;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#nativeSQL(String)}
	 */
	public void testNativeSQL()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);
		
		try
		{
			EasyMock.expect(this.sqlConnection.nativeSQL("test")).andReturn("SELECT 'test'");
			
			this.control.replay();
			
			String nativeSQL = this.connection.nativeSQL("test");
			
			this.control.verify();
			
			assert nativeSQL.equals("SELECT 'test'") : nativeSQL;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#prepareCall(String)}
	 */
	public void testPrepareCallString()
	{
		CallableStatement sqlStatement = EasyMock.createMock(CallableStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);

			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);

			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
			
			EasyMock.expect(this.sqlConnection.prepareCall("CALL ME")).andReturn(sqlStatement);
			
			this.lock.unlock();
			
			this.control.replay();
			
			CallableStatement statement = this.connection.prepareCall("CALL ME");
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.CallableStatement.class.cast(statement).getObject(this.database) == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#prepareCall(String)}
	 */
	public void testReadOnlyPrepareCallString()
	{
		CallableStatement sqlStatement = EasyMock.createMock(CallableStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.prepareCall("CALL ME")).andReturn(sqlStatement);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			CallableStatement statement = this.connection.prepareCall("CALL ME");
			
			this.control.verify();
			
			assert sqlStatement == statement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#prepareCall(String, int, int)}
	 */
	public void testPrepareCallStringIntInt()
	{
		CallableStatement sqlStatement = EasyMock.createMock(CallableStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);

			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
						
			EasyMock.expect(this.sqlConnection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)).andReturn(sqlStatement);
			
			this.lock.unlock();
			
			this.control.replay();
			
			CallableStatement statement = this.connection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.CallableStatement.class.cast(statement).getObject(this.database) == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#prepareCall(String, int, int)}
	 */
	public void testReadOnlyPrepareCallStringIntInt()
	{
		CallableStatement sqlStatement = EasyMock.createMock(CallableStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)).andReturn(sqlStatement);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			CallableStatement statement = this.connection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.control.verify();
			
			assert statement == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#prepareCall(String, int, int, int)}
	 */
	public void testPrepareCallStringIntIntInt()
	{
		CallableStatement sqlStatement = EasyMock.createMock(CallableStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);

			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
			
			EasyMock.expect(this.sqlConnection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE)).andReturn(sqlStatement);
			
			this.lock.unlock();
			
			this.control.replay();
			
			CallableStatement statement = this.connection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.CallableStatement.class.cast(statement).getObject(this.database) == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#prepareCall(String, int, int, int)}
	 */
	public void testReadOnlyPrepareCallStringIntIntInt()
	{
		CallableStatement sqlStatement = EasyMock.createMock(CallableStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE)).andReturn(sqlStatement);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			CallableStatement statement = this.connection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.control.verify();
			
			assert sqlStatement == statement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String)}
	 */
	public void testPrepareStatementString()
	{
		PreparedStatement sqlStatement = EasyMock.createMock(PreparedStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);

			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
						
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME")).andReturn(sqlStatement);
			
			this.lock.unlock();
			
			this.control.replay();
			
			PreparedStatement statement = this.connection.prepareStatement("SELECT ME");
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.PreparedStatement.class.cast(statement).getObject(this.database) == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String)}
	 */
	public void testReadOnlyPrepareStatementString()
	{
		PreparedStatement sqlStatement = EasyMock.createMock(PreparedStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME")).andReturn(sqlStatement);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			PreparedStatement statement = this.connection.prepareStatement("SELECT ME");
			
			this.control.verify();
			
			assert statement == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String, int)}
	 */
	public void testPrepareStatementStringInt()
	{
		PreparedStatement sqlStatement = EasyMock.createMock(PreparedStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
			
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME")).andReturn(sqlStatement);
			
			this.lock.unlock();
			
			this.control.replay();
			
			PreparedStatement statement = this.connection.prepareStatement("SELECT ME");
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.PreparedStatement.class.cast(statement).getObject(this.database) == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String, int)}
	 */
	public void testReadOnlyPrepareStatementStringInt()
	{
		PreparedStatement sqlStatement = EasyMock.createMock(PreparedStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME")).andReturn(sqlStatement);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			PreparedStatement statement = this.connection.prepareStatement("SELECT ME");
			
			this.control.verify();
			
			assert statement == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String, int, int)}
	 */
	public void testPrepareStatementStringIntInt()
	{
		PreparedStatement sqlStatement = EasyMock.createMock(PreparedStatement.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
			
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)).andReturn(sqlStatement);
			
			this.lock.unlock();
			
			this.control.replay();
			
			PreparedStatement statement = this.connection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.PreparedStatement.class.cast(statement).getObject(this.database) == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String, int, int)}
	 */
	public void testReadOnlyPrepareStatementStringIntInt()
	{
		PreparedStatement sqlStatement = EasyMock.createMock(PreparedStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)).andReturn(sqlStatement);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			PreparedStatement statement = this.connection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.control.verify();
			
			assert statement == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String, int, int, int)}
	 */
	public void testPrepareStatementStringIntIntInt()
	{
		PreparedStatement sqlStatement = EasyMock.createMock(PreparedStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
			
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE)).andReturn(sqlStatement);
			
			this.lock.unlock();
			
			this.control.replay();
			
			PreparedStatement statement = this.connection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.PreparedStatement.class.cast(statement).getObject(this.database) == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String, int, int, int)}
	 */
	public void testReadOnlyPrepareStatementStringIntIntInt()
	{
		PreparedStatement sqlStatement = EasyMock.createMock(PreparedStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE)).andReturn(sqlStatement);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			PreparedStatement statement = this.connection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.control.verify();
			
			assert statement == sqlStatement;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#releaseSavepoint(java.sql.Savepoint)}
	 */
	public void testReleaseSavepoint()
	{
		final java.sql.Savepoint sqlSavepoint = EasyMock.createMock(java.sql.Savepoint.class);
		
		Operation<java.sql.Connection, java.sql.Savepoint> operation = new Operation<java.sql.Connection, java.sql.Savepoint>()
		{
			public java.sql.Savepoint execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return sqlSavepoint;
			}
		};
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);

		this.lock.unlock();
		
		this.control.replay();
		
		try
		{
			Savepoint savepoint = new Savepoint(this.connection, operation);
			
			this.control.verify();
			this.control.reset();

			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
			
			this.sqlConnection.releaseSavepoint(savepoint);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.connection.releaseSavepoint(savepoint);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#rollback()}
	 */
	public void testRollback()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
		
		try
		{
			this.sqlConnection.rollback();
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.connection.rollback();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#rollback(java.sql.Savepoint)}
	 */
	public void testRollbackSavepoint()
	{
		final java.sql.Savepoint sqlSavepoint = EasyMock.createMock(java.sql.Savepoint.class);
		
		Operation<java.sql.Connection, java.sql.Savepoint> operation = new Operation<java.sql.Connection, java.sql.Savepoint>()
		{
			public java.sql.Savepoint execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return sqlSavepoint;
			}
		};
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
		
		this.lock.unlock();
		
		this.control.replay();
		
		try
		{
			Savepoint savepoint = new Savepoint(this.connection, operation);
			
			this.control.verify();
			this.control.reset();
			
			EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
			
			this.lock.lock();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
			
			this.sqlConnection.rollback(savepoint);

			this.lock.unlock();
			
			this.control.replay();
			
			this.connection.rollback(savepoint);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#setAutoCommit(boolean)}
	 */
	public void testSetAutoCommit()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
		
		try
		{
			this.sqlConnection.setAutoCommit(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.connection.setAutoCommit(true);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#setCatalog(String)}
	 */
	public void testSetCatalog()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
		
		try
		{
			this.sqlConnection.setCatalog("test");
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.connection.setCatalog("test");
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#setHoldability(int)}
	 */
	public void testSetHoldability()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#setReadOnly(boolean)}
	 */
	public void testSetReadOnly()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.setReadOnly(true);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.connection.setReadOnly(true);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#setSavepoint()}
	 */
	public void testSetSavepoint()
	{
		java.sql.Savepoint sqlSavepoint = EasyMock.createMock(java.sql.Savepoint.class);
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
		
		try
		{
			EasyMock.expect(this.sqlConnection.setSavepoint()).andReturn(sqlSavepoint);
			
			this.lock.unlock();
			
			this.control.replay();
			
			java.sql.Savepoint savepoint = this.connection.setSavepoint();
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.Savepoint.class.cast(savepoint).getObject(this.database) == sqlSavepoint;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#setSavepoint()}
	 */
	public void testSetSavepointString()
	{
		java.sql.Savepoint sqlSavepoint = EasyMock.createMock(java.sql.Savepoint.class);
		
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);
		
		try
		{
			EasyMock.expect(this.sqlConnection.setSavepoint("test")).andReturn(sqlSavepoint);
			
			this.lock.unlock();
			
			this.control.replay();
			
			java.sql.Savepoint savepoint = this.connection.setSavepoint("test");
			
			this.control.verify();
			
			assert net.sf.hajdbc.sql.Savepoint.class.cast(savepoint).getObject(this.database) == sqlSavepoint;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#setTransactionIsolation(int)}
	 */
	public void testSetTransactionIsolation()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(this.executor);		
		
		try
		{
			this.sqlConnection.setTransactionIsolation(Connection.TRANSACTION_NONE);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.connection.setTransactionIsolation(Connection.TRANSACTION_NONE);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Connection#setTypeMap(Map)}
	 */
	public void testSetTypeMap()
	{
		EasyMock.expect(this.databaseCluster.readLock()).andReturn(this.lock);
		
		this.lock.lock();
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.all()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.setTypeMap(Collections.EMPTY_MAP);
			
			this.lock.unlock();
			
			this.control.replay();
			
			this.connection.setTypeMap(Collections.EMPTY_MAP);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
}
