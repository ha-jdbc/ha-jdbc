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
import net.sf.hajdbc.EasyMockTestCase;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLObject;

import java.util.concurrent.Executors;

import org.easymock.EasyMock;

/**
 * Unit test for {@link Connection}
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestConnection extends EasyMockTestCase
{
	private DatabaseCluster databaseCluster = this.control.createMock(DatabaseCluster.class);
	private java.sql.Connection sqlConnection = this.control.createMock(java.sql.Connection.class);
	private Database database = this.control.createMock(Database.class);
	private Balancer balancer = this.control.createMock(Balancer.class);
	private FileSupport fileSupport = this.control.createMock(FileSupport.class);
	private Connection<java.sql.Connection> connection;
	private List<Database> databaseList = Collections.singletonList(this.database);
	
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		Map map = Collections.singletonMap(this.database, this.sqlConnection);
		
		EasyMock.expect(this.databaseCluster.getConnectionFactoryMap()).andReturn(map);
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer).times(2);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList).times(2);
		
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
	
	/**
	 * Test method for {@link Connection#getObject(Database)}
	 */
	public void testGetObject()
	{
		this.control.replay();
		
		Object connection = this.connection.getObject(this.database);
		
		this.control.verify();
		
		assertSame(this.sqlConnection, connection);
	}

	/**
	 * Test method for {@link Connection#getDatabaseCluster()}
	 */
	public void testGetDatabaseCluster()
	{
		this.control.replay();
		
		DatabaseCluster databaseCluster = this.connection.getDatabaseCluster();
		
		this.control.verify();
		
		assertSame(this.databaseCluster, databaseCluster);
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
			fail(e);
		}
	}
	
	/**
	 * Test method for {@link Connection#clearWarnings()}
	 */
	public void testClearWarnings()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.clearWarnings();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			this.connection.clearWarnings();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#close()}
	 */
	public void testClose()
	{
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.close();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.fileSupport.close();
			
			this.control.replay();
			
			this.connection.close();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#commit()}
	 */
	public void testCommit()
	{
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.commit();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			this.connection.commit();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#createStatement()}
	 */
	public void testCreateStatement()
	{
		Statement statement1 = EasyMock.createMock(Statement.class);

		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(Boolean.FALSE);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.sqlConnection.createStatement()).andReturn(statement1);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			Statement statement = this.connection.createStatement();
			
			this.control.verify();
			
			assertNotNull(statement);
			assertTrue(SQLObject.class.isInstance(statement));
			assertSame(statement1, ((SQLObject) statement).getObject(this.database));
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			assertSame(sqlStatement, statement);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#createStatement(int, int)}
	 */
	public void testCreateStatementIntInt()
	{
		Statement statement1 = EasyMock.createMock(Statement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.sqlConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)).andReturn(statement1);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			Statement sqlStatement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.control.verify();
			
			assertNotNull(sqlStatement);
			assertTrue(SQLObject.class.isInstance(sqlStatement));
			assertSame(statement1, ((SQLObject) sqlStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			assertSame(sqlStatement, statement);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#createStatement(int, int, int)}
	 */
	public void testCreateStatementIntIntInt()
	{
		Statement statement1 = EasyMock.createMock(Statement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.sqlConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE)).andReturn(statement1);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			Statement sqlStatement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.control.verify();
			
			assertNotNull(sqlStatement);
			assertTrue(SQLObject.class.isInstance(sqlStatement));
			assertSame(statement1, ((SQLObject) sqlStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			assertSame(sqlStatement, statement);
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			assertTrue(autoCommit);
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			assertEquals("test", catalog);
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, holdability);
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			assertSame(databaseMetaData, metaData);
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			assertEquals(Connection.TRANSACTION_NONE, transactionIsolation);
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			assertSame(typeMap, map);
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			assertSame(sqlWarning, warning);
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			assertTrue(closed);
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			assertTrue(readOnly);
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			assertEquals("SELECT 'test'", nativeSQL);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#prepareCall(String)}
	 */
	public void testPrepareCallString()
	{
		CallableStatement statement1 = EasyMock.createMock(CallableStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.sqlConnection.prepareCall("CALL ME")).andReturn(statement1);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			CallableStatement callableStatement = this.connection.prepareCall("CALL ME");
			
			this.control.verify();
			
			assertNotNull(callableStatement);
			assertTrue(SQLObject.class.isInstance(callableStatement));
			assertSame(statement1, ((SQLObject) callableStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#prepareCall(String)}
	 */
	public void testReadOnlyPrepareCallString()
	{
		CallableStatement statement = EasyMock.createMock(CallableStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.prepareCall("CALL ME")).andReturn(statement);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			CallableStatement callableStatement = this.connection.prepareCall("CALL ME");
			
			this.control.verify();
			
			assertSame(statement, callableStatement);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#prepareCall(String, int, int)}
	 */
	public void testPrepareCallStringIntInt()
	{
		CallableStatement statement = EasyMock.createMock(CallableStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
						
			EasyMock.expect(this.sqlConnection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)).andReturn(statement);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			CallableStatement callableStatement = this.connection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.control.verify();
			
			assertNotNull(callableStatement);
			assertTrue(SQLObject.class.isInstance(callableStatement));
			assertSame(statement, SQLObject.class.cast(callableStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#prepareCall(String, int, int)}
	 */
	public void testReadOnlyPrepareCallStringIntInt()
	{
		CallableStatement statement = EasyMock.createMock(CallableStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)).andReturn(statement);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			CallableStatement callableStatement = this.connection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.control.verify();
			
			assertSame(statement, callableStatement);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#prepareCall(String, int, int, int)}
	 */
	public void testPrepareCallStringIntIntInt()
	{
		CallableStatement statement = EasyMock.createMock(CallableStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.sqlConnection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE)).andReturn(statement);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			CallableStatement callableStatement = this.connection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.control.verify();
			
			assertNotNull(callableStatement);
			assertTrue(SQLObject.class.isInstance(callableStatement));
			assertSame(statement, SQLObject.class.cast(callableStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#prepareCall(String, int, int, int)}
	 */
	public void testReadOnlyPrepareCallStringIntIntInt()
	{
		CallableStatement statement = EasyMock.createMock(CallableStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE)).andReturn(statement);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			CallableStatement callableStatement = this.connection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.control.verify();
			
			assertSame(statement, callableStatement);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String)}
	 */
	public void testPrepareStatementString()
	{
		PreparedStatement statement = EasyMock.createMock(PreparedStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
						
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME")).andReturn(statement);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			PreparedStatement preparedStatement = this.connection.prepareStatement("SELECT ME");
			
			this.control.verify();
			
			assertNotNull(preparedStatement);
			assertTrue(SQLObject.class.isInstance(preparedStatement));
			assertSame(statement, SQLObject.class.cast(preparedStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String)}
	 */
	public void testReadOnlyPrepareStatementString()
	{
		PreparedStatement statement = EasyMock.createMock(PreparedStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME")).andReturn(statement);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			PreparedStatement preparedStatement = this.connection.prepareStatement("SELECT ME");
			
			this.control.verify();
			
			assertSame(statement, preparedStatement);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String, int)}
	 */
	public void testPrepareStatementStringInt()
	{
		PreparedStatement statement = EasyMock.createMock(PreparedStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME")).andReturn(statement);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			PreparedStatement preparedStatement = this.connection.prepareStatement("SELECT ME");
			
			this.control.verify();
			
			assertNotNull(preparedStatement);
			assertTrue(SQLObject.class.isInstance(preparedStatement));
			assertSame(statement, SQLObject.class.cast(preparedStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String, int)}
	 */
	public void testReadOnlyPrepareStatementStringInt()
	{
		PreparedStatement statement = EasyMock.createMock(PreparedStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME")).andReturn(statement);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			PreparedStatement preparedStatement = this.connection.prepareStatement("SELECT ME");
			
			this.control.verify();
			
			assertSame(statement, preparedStatement);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String, int, int)}
	 */
	public void testPrepareStatementStringIntInt()
	{
		PreparedStatement statement = EasyMock.createMock(PreparedStatement.class);
		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)).andReturn(statement);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);		
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			PreparedStatement preparedStatement = this.connection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.control.verify();
			
			assertNotNull(preparedStatement);
			assertTrue(SQLObject.class.isInstance(preparedStatement));
			assertSame(statement, SQLObject.class.cast(preparedStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String, int, int)}
	 */
	public void testReadOnlyPrepareStatementStringIntInt()
	{
		PreparedStatement statement = EasyMock.createMock(PreparedStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE)).andReturn(statement);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			PreparedStatement preparedStatement = this.connection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.control.verify();
			
			assertSame(statement, preparedStatement);
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String, int, int, int)}
	 */
	public void testPrepareStatementStringIntIntInt()
	{
		PreparedStatement statement = EasyMock.createMock(PreparedStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(false);
			
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE)).andReturn(statement);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			PreparedStatement preparedStatement = this.connection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.control.verify();
			
			assertNotNull(preparedStatement);
			assertTrue(SQLObject.class.isInstance(preparedStatement));			
			assertSame(statement, SQLObject.class.cast(preparedStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#prepareStatement(String, int, int, int)}
	 */
	public void testReadOnlyPrepareStatementStringIntIntInt()
	{
		PreparedStatement statement = EasyMock.createMock(PreparedStatement.class);

		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.first()).andReturn(this.database);

		try
		{
			EasyMock.expect(this.sqlConnection.isReadOnly()).andReturn(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.next()).andReturn(this.database);

			this.balancer.beforeOperation(this.database);
			
			EasyMock.expect(this.sqlConnection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE)).andReturn(statement);
			
			this.balancer.afterOperation(this.database);
			
			this.control.replay();
			
			PreparedStatement preparedStatement = this.connection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.control.verify();
			
			assertSame(statement, preparedStatement);
		}
		catch (SQLException e)
		{
			fail(e);
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
		
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer).times(2);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList).times(2);
		
		this.control.replay();
		
		try
		{
			Savepoint savepoint = new Savepoint(this.connection, operation);
			
			this.control.verify();
			this.control.reset();
			
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.sqlConnection.releaseSavepoint(savepoint);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			this.connection.releaseSavepoint(savepoint);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#rollback()}
	 */
	public void testRollback()
	{
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.rollback();
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			this.connection.rollback();
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
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
		
		try
		{
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer).times(2);
			
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList).times(2);
			
			this.control.replay();
			
			Savepoint savepoint = new Savepoint(this.connection, operation);
			
			this.control.verify();
			this.control.reset();
			
			EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer).times(2);
			
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList).times(2);
			
			this.sqlConnection.rollback(savepoint);
			
			this.control.replay();
			
			this.connection.rollback(savepoint);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#setAutoCommit(boolean)}
	 */
	public void testSetAutoCommit()
	{
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.setAutoCommit(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			this.connection.setAutoCommit(true);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#setCatalog(String)}
	 */
	public void testSetCatalog()
	{
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.setCatalog("test");
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			this.connection.setCatalog("test");
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#setHoldability(int)}
	 */
	public void testSetHoldability()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			this.connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#setReadOnly(boolean)}
	 */
	public void testSetReadOnly()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.setReadOnly(true);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			this.connection.setReadOnly(true);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#setSavepoint()}
	 */
	public void testSetSavepoint()
	{
		java.sql.Savepoint sqlSavepoint = EasyMock.createMock(java.sql.Savepoint.class);
		
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlConnection.setSavepoint()).andReturn(sqlSavepoint);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			java.sql.Savepoint savepoint = this.connection.setSavepoint();
			
			this.control.verify();
			
			assertNotNull(savepoint);
			assertTrue(SQLObject.class.isInstance(savepoint));
			assertSame(sqlSavepoint, SQLObject.class.cast(savepoint).getObject(this.database));
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#setSavepoint()}
	 */
	public void testSetSavepointString()
	{
		java.sql.Savepoint sqlSavepoint = EasyMock.createMock(java.sql.Savepoint.class);
		
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			EasyMock.expect(this.sqlConnection.setSavepoint("test")).andReturn(sqlSavepoint);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			java.sql.Savepoint savepoint = this.connection.setSavepoint("test");
			
			this.control.verify();
			
			assertNotNull(savepoint);
			assertTrue(SQLObject.class.isInstance(savepoint));
			assertSame(sqlSavepoint, SQLObject.class.cast(savepoint).getObject(this.database));
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#setTransactionIsolation(int)}
	 */
	public void testSetTransactionIsolation()
	{
		EasyMock.expect(this.databaseCluster.getExecutor()).andReturn(Executors.newSingleThreadExecutor());		
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.setTransactionIsolation(Connection.TRANSACTION_NONE);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			this.connection.setTransactionIsolation(Connection.TRANSACTION_NONE);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Connection#setTypeMap(Map)}
	 */
	public void testSetTypeMap()
	{
		EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
		EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
		
		try
		{
			this.sqlConnection.setTypeMap(Collections.EMPTY_MAP);
			
			EasyMock.expect(this.databaseCluster.getBalancer()).andReturn(this.balancer);
			EasyMock.expect(this.balancer.list()).andReturn(this.databaseList);
			
			this.control.replay();
			
			this.connection.setTypeMap(Collections.EMPTY_MAP);
			
			this.control.verify();
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}
}
