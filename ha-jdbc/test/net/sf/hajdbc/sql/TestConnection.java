/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2005 Paul Ferraro
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
import java.util.Map;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.ConnectionFactory;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.EasyMockTestCase;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLObject;

import org.easymock.MockControl;

/**
 * Unit test for {@link Connection}
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestConnection extends EasyMockTestCase
{
	private MockControl databaseClusterControl = this.createControl(DatabaseCluster.class);
	private DatabaseCluster databaseCluster = (DatabaseCluster) this.databaseClusterControl.getMock();
	
	private MockControl sqlConnectionControl = this.createControl(java.sql.Connection.class);
	private java.sql.Connection sqlConnection = (java.sql.Connection) this.sqlConnectionControl.getMock();
	
	private MockControl databaseControl = this.createControl(Database.class);
	private Database database = (Database) this.databaseControl.getMock();
	
	private MockControl balancerControl = this.createControl(Balancer.class);
	private Balancer balancer = (Balancer) this.balancerControl.getMock();
	
	private MockControl fileSupportControl = this.createControl(FileSupport.class);
	private FileSupport fileSupport = (FileSupport) this.fileSupportControl.getMock();
	
	private Connection connection;
	private Database[] databases = new Database[] { this.database };
	
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		this.replay();
		
		ConnectionFactory connectionFactory = new ConnectionFactory(this.databaseCluster, Collections.singletonMap(this.database, this.sqlConnection));
		
		Operation operation = new Operation()
		{
			public Object execute(Database database, Object sqlObject) throws SQLException
			{
				return sqlObject;
			}
		};
		
		this.connection = new Connection(connectionFactory, operation, this.fileSupport);

		this.verify();
		this.reset();
	}
	
	public void testGetObject()
	{
		this.replay();
		
		Object connection = this.connection.getObject(this.database);
		
		this.verify();
		
		assertSame(this.sqlConnection, connection);
	}

	public void testGetDatabaseCluster()
	{
		this.replay();
		
		DatabaseCluster databaseCluster = this.connection.getDatabaseCluster();
		
		this.verify();
		
		assertSame(this.databaseCluster, databaseCluster);
	}

	public void testHandleException()
	{
		try
		{
			this.databaseCluster.deactivate(this.database);
			this.databaseClusterControl.setReturnValue(false);
			
			this.replay();
			
			this.connection.handleExceptions(Collections.singletonMap(this.database, new SQLException()));
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}
	
	public void testClearWarnings()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlConnection.clearWarnings();
			this.sqlConnectionControl.setVoidCallable();
			
			replay();
			
			this.connection.clearWarnings();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testClose()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlConnection.close();
			this.sqlConnectionControl.setVoidCallable();
			
			this.fileSupport.close();
			this.fileSupportControl.setVoidCallable();
			
			this.replay();
			
			this.connection.close();
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testCommit()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlConnection.commit();
			this.sqlConnectionControl.setVoidCallable();
			
			this.replay();
			
			this.connection.commit();
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testCreateStatement()
	{
		Statement statement1 = (Statement) this.createMock(Statement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(false);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.createStatement();
			this.sqlConnectionControl.setReturnValue(statement1);
			
			this.replay();
			
			Statement statement = this.connection.createStatement();
			
			this.verify();
			
			assertNotNull(statement);
			assertTrue(SQLObject.class.isInstance(statement));
			assertSame(statement1, ((SQLObject) statement).getObject(this.database));
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testReadOnlyCreateStatement()
	{
		Statement sqlStatement = (Statement) this.createMock(Statement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database, 2);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(true);
			
			this.sqlConnection.createStatement();
			this.sqlConnectionControl.setReturnValue(sqlStatement);

			this.replay();
			
			Statement statement = this.connection.createStatement();
			
			this.verify();
			
			assertSame(sqlStatement, statement);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testCreateStatementIntInt()
	{
		Statement statement1 = (Statement) this.createMock(Statement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(false);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			this.sqlConnectionControl.setReturnValue(statement1);
			
			this.replay();
			
			Statement sqlStatement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.verify();
			
			assertNotNull(sqlStatement);
			assertTrue(SQLObject.class.isInstance(sqlStatement));
			assertSame(statement1, ((SQLObject) sqlStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testReadOnlyCreateStatementIntInt()
	{
		Statement sqlStatement = (Statement) this.createMock(Statement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database, 2);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(true);
			
			this.sqlConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			this.sqlConnectionControl.setReturnValue(sqlStatement);
			
			this.replay();
			
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.verify();
			
			assertSame(sqlStatement, statement);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testCreateStatementIntIntInt()
	{
		Statement statement1 = (Statement) this.createMock(Statement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(false);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			this.sqlConnectionControl.setReturnValue(statement1);
			
			this.replay();
			
			Statement sqlStatement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.verify();
			
			assertNotNull(sqlStatement);
			assertTrue(SQLObject.class.isInstance(sqlStatement));
			assertSame(statement1, ((SQLObject) sqlStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testReadOnlyCreateStatementIntIntInt()
	{
		Statement sqlStatement = (Statement) this.createMock(Statement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database, 2);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(true);
			
			this.sqlConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			this.sqlConnectionControl.setReturnValue(sqlStatement);
			
			this.replay();
			
			Statement statement = this.connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.verify();
			
			assertSame(sqlStatement, statement);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testGetAutoCommit()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 1);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlConnection.getAutoCommit();
			this.sqlConnectionControl.setReturnValue(true);
			
			this.replay();
			
			boolean autoCommit = this.connection.getAutoCommit();
			
			this.verify();
			
			assertTrue(autoCommit);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testGetCatalog()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 1);
		
		this.balancer.next();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.sqlConnection.getCatalog();
			this.sqlConnectionControl.setReturnValue("test");
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.replay();
			
			String catalog = this.connection.getCatalog();
			
			this.verify();
			
			assertEquals("test", catalog);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testGetHoldability()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 1);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlConnection.getHoldability();
			this.sqlConnectionControl.setReturnValue(ResultSet.HOLD_CURSORS_OVER_COMMIT);
			
			this.replay();
			
			int holdability = this.connection.getHoldability();
			
			this.verify();
			
			assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, holdability);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testGetMetaData()
	{
		DatabaseMetaData databaseMetaData = (DatabaseMetaData) this.createMock(DatabaseMetaData.class);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 1);
		
		this.balancer.next();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.sqlConnection.getMetaData();
			this.sqlConnectionControl.setReturnValue(databaseMetaData);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.replay();
			
			DatabaseMetaData metaData = this.connection.getMetaData();
			
			this.verify();
			
			assertSame(databaseMetaData, metaData);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testGetTransactionIsolation()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 1);
		
		this.balancer.next();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.sqlConnection.getTransactionIsolation();
			this.sqlConnectionControl.setReturnValue(Connection.TRANSACTION_NONE);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.replay();
			
			int transactionIsolation = this.connection.getTransactionIsolation();
			
			this.verify();
			
			assertEquals(Connection.TRANSACTION_NONE, transactionIsolation);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testGetTypeMap()
	{
		Map typeMap = Collections.EMPTY_MAP;
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 1);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlConnection.getTypeMap();
			this.sqlConnectionControl.setReturnValue(typeMap);
			
			this.replay();
			
			Map map = this.connection.getTypeMap();
			
			this.verify();
			
			assertSame(typeMap, map);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testGetWarnings()
	{
		SQLWarning sqlWarning = new SQLWarning();
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 1);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlConnection.getWarnings();
			this.sqlConnectionControl.setReturnValue(sqlWarning);
			
			this.replay();
			
			SQLWarning warning = this.connection.getWarnings();
			
			this.verify();
			
			assertSame(sqlWarning, warning);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testIsClosed()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 1);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlConnection.isClosed();
			this.sqlConnectionControl.setReturnValue(true);
			
			this.replay();
			
			boolean closed = this.connection.isClosed();
			
			this.verify();
			
			assertTrue(closed);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testIsReadOnly()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 1);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(true);
			
			this.replay();
			
			boolean readOnly = this.connection.isReadOnly();
			
			this.verify();
			
			assertTrue(readOnly);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testNativeSQL()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 1);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlConnection.nativeSQL("test");
			this.sqlConnectionControl.setReturnValue("SELECT 'test'");
			
			this.replay();
			
			String nativeSQL = this.connection.nativeSQL("test");
			
			this.verify();
			
			assertEquals("SELECT 'test'", nativeSQL);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testPrepareCallString()
	{
		CallableStatement statement1 = (CallableStatement) this.createMock(CallableStatement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(false);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.prepareCall("CALL ME");
			this.sqlConnectionControl.setReturnValue(statement1);
			
			this.replay();
			
			CallableStatement callableStatement = this.connection.prepareCall("CALL ME");
			
			this.verify();
			
			assertNotNull(callableStatement);
			assertTrue(SQLObject.class.isInstance(callableStatement));
			assertSame(statement1, ((SQLObject) callableStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testReadOnlyPrepareCallString()
	{
		CallableStatement statement = (CallableStatement) this.createMock(CallableStatement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(true);
			
			this.balancer.next();
			this.balancerControl.setReturnValue(this.database);

			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.sqlConnection.prepareCall("CALL ME");
			this.sqlConnectionControl.setReturnValue(statement);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.replay();
			
			CallableStatement callableStatement = this.connection.prepareCall("CALL ME");
			
			this.verify();
			
			assertSame(statement, callableStatement);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testPrepareCallStringIntInt()
	{
		CallableStatement statement1 = (CallableStatement) this.createMock(CallableStatement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(false);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			this.sqlConnectionControl.setReturnValue(statement1);
			
			this.replay();
			
			CallableStatement callableStatement = this.connection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.verify();
			
			assertNotNull(callableStatement);
			assertTrue(SQLObject.class.isInstance(callableStatement));
			assertSame(statement1, ((SQLObject) callableStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testReadOnlyPrepareCallStringIntInt()
	{
		CallableStatement statement = (CallableStatement) this.createMock(CallableStatement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(true);
			
			this.balancer.next();
			this.balancerControl.setReturnValue(this.database);

			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.sqlConnection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			this.sqlConnectionControl.setReturnValue(statement);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.replay();
			
			CallableStatement callableStatement = this.connection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.verify();
			
			assertSame(statement, callableStatement);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testPrepareCallStringIntIntInt()
	{
		CallableStatement statement1 = (CallableStatement) this.createMock(CallableStatement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(false);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			this.sqlConnectionControl.setReturnValue(statement1);
			
			this.replay();
			
			CallableStatement callableStatement = this.connection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.verify();
			
			assertNotNull(callableStatement);
			assertTrue(SQLObject.class.isInstance(callableStatement));
			assertSame(statement1, ((SQLObject) callableStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testReadOnlyPrepareCallStringIntIntInt()
	{
		CallableStatement statement = (CallableStatement) this.createMock(CallableStatement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(true);
			
			this.balancer.next();
			this.balancerControl.setReturnValue(this.database);

			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.sqlConnection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			this.sqlConnectionControl.setReturnValue(statement);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.replay();
			
			CallableStatement callableStatement = this.connection.prepareCall("CALL ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.verify();
			
			assertSame(statement, callableStatement);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testPrepareStatementString()
	{
		PreparedStatement statement1 = (PreparedStatement) this.createMock(PreparedStatement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(false);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.prepareStatement("SELECT ME");
			this.sqlConnectionControl.setReturnValue(statement1);
			
			this.replay();
			
			PreparedStatement preparedStatement = this.connection.prepareStatement("SELECT ME");
			
			this.verify();
			
			assertNotNull(preparedStatement);
			assertTrue(SQLObject.class.isInstance(preparedStatement));
			assertSame(statement1, ((SQLObject) preparedStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testReadOnlyPrepareStatementString()
	{
		PreparedStatement statement = (PreparedStatement) this.createMock(PreparedStatement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(true);
			
			this.balancer.next();
			this.balancerControl.setReturnValue(this.database);

			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.sqlConnection.prepareStatement("SELECT ME");
			this.sqlConnectionControl.setReturnValue(statement);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.replay();
			
			PreparedStatement preparedStatement = this.connection.prepareStatement("SELECT ME");
			
			this.verify();
			
			assertSame(statement, preparedStatement);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testPrepareStatementStringIntInt()
	{
		PreparedStatement statement1 = (PreparedStatement) this.createMock(PreparedStatement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(false);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			this.sqlConnectionControl.setReturnValue(statement1);
			
			this.replay();
			
			PreparedStatement preparedStatement = this.connection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.verify();
			
			assertNotNull(preparedStatement);
			assertTrue(SQLObject.class.isInstance(preparedStatement));
			assertSame(statement1, ((SQLObject) preparedStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testReadOnlyPrepareStatementStringIntInt()
	{
		PreparedStatement statement = (PreparedStatement) this.createMock(PreparedStatement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(true);
			
			this.balancer.next();
			this.balancerControl.setReturnValue(this.database);

			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.sqlConnection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			this.sqlConnectionControl.setReturnValue(statement);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.replay();
			
			PreparedStatement preparedStatement = this.connection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			this.verify();
			
			assertSame(statement, preparedStatement);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testPrepareStatementStringIntIntInt()
	{
		PreparedStatement statement1 = (PreparedStatement) this.createMock(PreparedStatement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(false);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			this.sqlConnectionControl.setReturnValue(statement1);
			
			this.replay();
			
			PreparedStatement preparedStatement = this.connection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.verify();
			
			assertNotNull(preparedStatement);
			assertTrue(SQLObject.class.isInstance(preparedStatement));			
			assertSame(statement1, ((SQLObject) preparedStatement).getObject(this.database));
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testReadOnlyPrepareStatementStringIntIntInt()
	{
		PreparedStatement statement = (PreparedStatement) this.createMock(PreparedStatement.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlConnection.isReadOnly();
			this.sqlConnectionControl.setReturnValue(true);
			
			this.balancer.next();
			this.balancerControl.setReturnValue(this.database);

			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.sqlConnection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			this.sqlConnectionControl.setReturnValue(statement);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.replay();
			
			PreparedStatement preparedStatement = this.connection.prepareStatement("SELECT ME", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE, ResultSet.FETCH_REVERSE);
			
			this.verify();
			
			assertSame(statement, preparedStatement);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testReleaseSavepoint()
	{
		final java.sql.Savepoint savepoint1 = (java.sql.Savepoint) this.createMock(java.sql.Savepoint.class);
		
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return savepoint1;
			}
		};
		
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.replay();
			
			Savepoint savepoint = new Savepoint(this.connection, operation);
			
			this.verify();
			this.reset();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.releaseSavepoint(savepoint1);
			this.sqlConnectionControl.setVoidCallable();
			
			this.replay();
			
			this.connection.releaseSavepoint(savepoint);
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testRollback()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlConnection.rollback();
			this.sqlConnectionControl.setVoidCallable();
			
			this.replay();
			
			this.connection.rollback();
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testRollbackSavepoint()
	{
		final java.sql.Savepoint savepoint1 = (java.sql.Savepoint) this.createMock(java.sql.Savepoint.class);
		
		ConnectionOperation operation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return savepoint1;
			}
		};
		
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.replay();
			
			Savepoint savepoint = new Savepoint(this.connection, operation);
			
			this.verify();
			this.reset();
			
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.rollback(savepoint1);
			this.sqlConnectionControl.setVoidCallable();
			
			this.replay();
			
			this.connection.rollback(savepoint);
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testSetAutoCommit()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlConnection.setAutoCommit(true);
			this.sqlConnectionControl.setVoidCallable();
			
			this.replay();
			
			this.connection.setAutoCommit(true);
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testSetCatalog()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlConnection.setCatalog("test");
			this.sqlConnectionControl.setVoidCallable();
			
			this.replay();
			
			this.connection.setCatalog("test");
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testSetHoldability()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlConnection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
			this.sqlConnectionControl.setVoidCallable();
			
			this.replay();
			
			this.connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testSetReadOnly()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlConnection.setReadOnly(true);
			this.sqlConnectionControl.setVoidCallable();
			
			this.replay();
			
			this.connection.setReadOnly(true);
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testSetSavepoint()
	{
		java.sql.Savepoint savepoint1 = (java.sql.Savepoint) this.createMock(java.sql.Savepoint.class);
		
		try
		{
			this.databaseCluster.getBalancer();
			this.databaseClusterControl.setReturnValue(this.balancer, 2);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlConnection.setSavepoint("test");
			this.sqlConnectionControl.setReturnValue(savepoint1);
			
			this.replay();
			
			java.sql.Savepoint savepoint = this.connection.setSavepoint("test");
			
			this.verify();
			
			assertNotNull(savepoint);
			assertTrue(SQLObject.class.isInstance(savepoint));
			assertSame(savepoint1, ((SQLObject) savepoint).getObject(this.database));
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testSetTransactionIsolation()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlConnection.setTransactionIsolation(Connection.TRANSACTION_NONE);
			this.sqlConnectionControl.setVoidCallable();
			
			this.replay();
			
			this.connection.setTransactionIsolation(Connection.TRANSACTION_NONE);
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	public void testSetTypeMap()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlConnection.setTypeMap(Collections.EMPTY_MAP);
			this.sqlConnectionControl.setVoidCallable();
			
			this.replay();
			
			this.connection.setTypeMap(Collections.EMPTY_MAP);
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}
}
