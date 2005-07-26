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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Collections;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.ConnectionFactory;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.EasyMockTestCase;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLObject;

import org.easymock.MockControl;

public class TestStatement extends EasyMockTestCase
{
	protected MockControl databaseClusterControl = this.createControl(DatabaseCluster.class);
	protected DatabaseCluster databaseCluster = (DatabaseCluster) this.databaseClusterControl.getMock();
	
	protected MockControl sqlStatementControl = this.createControl(this.getSQLStatementClass());
	protected java.sql.Statement sqlStatement = (java.sql.Statement) this.sqlStatementControl.getMock();
	
	protected MockControl databaseControl = this.createControl(Database.class);
	protected Database database = (Database) this.databaseControl.getMock();
	
	protected MockControl balancerControl = this.createControl(Balancer.class);
	protected Balancer balancer = (Balancer) this.balancerControl.getMock();
	
	protected MockControl fileSupportControl = this.createControl(FileSupport.class);
	protected FileSupport fileSupport = (FileSupport) this.fileSupportControl.getMock();
	
	protected Connection connection;
	protected Statement statement;
	protected Database[] databases = new Database[] { this.database };
	
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 4);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 4);
		
		this.replay();
		
		ConnectionFactory connectionFactory = new ConnectionFactory(this.databaseCluster, Collections.singletonMap(this.database, new Object()));
		
		Operation operation = new Operation()
		{
			public Object execute(Database database, Object sqlObject) throws SQLException
			{
				return MockControl.createControl(java.sql.Connection.class).getMock();
			}
		};
		
		this.connection = new Connection(connectionFactory, operation, this.fileSupport);
		
		ConnectionOperation connectionOperation = new ConnectionOperation()
		{
			public Object execute(Database database, java.sql.Connection connection) throws SQLException
			{
				return sqlStatement;
			}
		};
		
		this.statement = this.createStatement(connection, connectionOperation);
		
		this.verify();
		this.reset();
	}
	
	protected Statement createStatement(Connection connection, ConnectionOperation operation) throws SQLException
	{
		return new Statement(connection, operation);
	}
		
	protected Class getSQLStatementClass()
	{
		return java.sql.Statement.class;
	}
	
	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.addBatch(String)'
	 */
	public void testAddBatch()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.addBatch("test");
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.statement.addBatch("test");
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.cancel()'
	 */
	public void testCancel()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.cancel();
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.statement.cancel();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.clearBatch()'
	 */
	public void testClearBatch()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.clearBatch();
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.statement.clearBatch();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.clearWarnings()'
	 */
	public void testClearWarnings()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.clearWarnings();
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.statement.clearWarnings();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.close()'
	 */
	public void testClose()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.close();
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.statement.close();
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.execute(String)'
	 */
	public void testExecuteString()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.execute("test");
			this.sqlStatementControl.setReturnValue(true);
			
			replay();
			
			boolean results = this.statement.execute("test");
			
			verify();
			
			assertTrue(results);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.execute(String, int)'
	 */
	public void testExecuteStringInt()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.execute("SELECT ME", 1);
			this.sqlStatementControl.setReturnValue(true);
			
			replay();
			
			boolean results = this.statement.execute("SELECT ME", 1);
			
			verify();
			
			assertTrue(results);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.execute(String, int[])'
	 */
	public void testExecuteStringIntArray()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		int[] columns = new int[] { 0 };
		
		try
		{
			this.sqlStatement.execute("SELECT ME", columns);
			this.sqlStatementControl.setReturnValue(true);
			
			replay();
			
			boolean results = this.statement.execute("SELECT ME", columns);
			
			verify();
			
			assertTrue(results);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.execute(String, String[])'
	 */
	public void testExecuteStringStringArray()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		String[] columns = new String[] { "column" };
		try
		{
			this.sqlStatement.execute("SELECT ME", columns);
			this.sqlStatementControl.setReturnValue(true);
			
			replay();
			
			boolean results = this.statement.execute("SELECT ME", columns);
			
			verify();
			
			assertTrue(results);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.executeBatch()'
	 */
	public void testExecuteBatch()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			int[] rows = new int[] { 100 };
			
			this.sqlStatement.executeBatch();
			this.sqlStatementControl.setReturnValue(rows);
			
			replay();
			
			int[] results = this.statement.executeBatch();
			
			verify();
			
			assertSame(rows, results);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.executeQuery(String)'
	 */
	public void testExecuteQuery()
	{
		ResultSet resultSet = (ResultSet) this.createMock(ResultSet.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlStatement.getResultSetConcurrency();
			this.sqlStatementControl.setReturnValue(ResultSet.CONCUR_READ_ONLY);
			
			this.balancer.next();
			this.balancerControl.setReturnValue(this.database);
			
			this.balancer.beforeOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.sqlStatement.executeQuery("SELECT ME");
			this.sqlStatementControl.setReturnValue(resultSet);
			
			this.balancer.afterOperation(this.database);
			this.balancerControl.setVoidCallable();
			
			this.replay();
			
			ResultSet rs = this.statement.executeQuery("SELECT ME");
			
			this.verify();
			
			assertSame(resultSet, rs);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.executeQuery(String)'
	 */
	public void testUpdatableExecuteQuery()
	{
		ResultSet resultSet = (ResultSet) this.createMock(ResultSet.class);

		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 3);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);

		try
		{
			this.sqlStatement.getResultSetConcurrency();
			this.sqlStatementControl.setReturnValue(ResultSet.CONCUR_UPDATABLE);
			
			this.balancer.toArray();
			this.balancerControl.setReturnValue(this.databases, 2);
			
			this.sqlStatement.executeQuery("SELECT ME");
			this.sqlStatementControl.setReturnValue(resultSet);
			
			this.replay();
			
			ResultSet rs = this.statement.executeQuery("SELECT ME");
			
			this.verify();
			
			assertNotNull(rs);
			assertTrue(SQLObject.class.isInstance(rs));			
			assertSame(resultSet, ((SQLObject) rs).getObject(this.database));
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.executeUpdate(String)'
	 */
	public void testExecuteUpdateString()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.executeUpdate("INSERT ME");
			this.sqlStatementControl.setReturnValue(1);
			
			replay();
			
			int result = this.statement.executeUpdate("INSERT ME");
			
			verify();
			
			assertEquals(1, result);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.executeUpdate(String, int)'
	 */
	public void testExecuteUpdateStringInt()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.executeUpdate("INSERT ME", 1);
			this.sqlStatementControl.setReturnValue(1);
			
			replay();
			
			int result = this.statement.executeUpdate("INSERT ME", 1);
			
			verify();
			
			assertEquals(1, result);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.executeUpdate(String, int[])'
	 */
	public void testExecuteUpdateStringIntArray()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		int[] columns = new int[] { 0 };
		
		try
		{
			this.sqlStatement.executeUpdate("INSERT ME", columns);
			this.sqlStatementControl.setReturnValue(1);
			
			replay();
			
			int result = this.statement.executeUpdate("INSERT ME", columns);
			
			verify();
			
			assertEquals(1, result);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.executeUpdate(String, String[])'
	 */
	public void testExecuteUpdateStringStringArray()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		String[] columns = new String[] { "column" };
		
		try
		{
			this.sqlStatement.executeUpdate("INSERT ME", columns);
			this.sqlStatementControl.setReturnValue(1);
			
			replay();
			
			int result = this.statement.executeUpdate("INSERT ME", columns);
			
			verify();
			
			assertEquals(1, result);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getConnection()'
	 */
	public void testGetConnection()
	{
		java.sql.Connection connection = this.statement.getConnection();
		
		assertSame(this.connection, connection);
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getFetchDirection()'
	 */
	public void testGetFetchDirection()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlStatement.getFetchDirection();
			this.sqlStatementControl.setReturnValue(ResultSet.FETCH_REVERSE);
			
			replay();
			
			int direction = this.statement.getFetchDirection();
			
			verify();
			
			assertEquals(ResultSet.FETCH_REVERSE, direction);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getFetchSize()'
	 */
	public void testGetFetchSize()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlStatement.getFetchSize();
			this.sqlStatementControl.setReturnValue(100);
			
			replay();
			
			int size = this.statement.getFetchSize();
			
			verify();
			
			assertEquals(100, size);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getGeneratedKeys()'
	 */
	public void testGetGeneratedKeys()
	{
		ResultSet resultSet = (ResultSet) this.createMock(ResultSet.class);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlStatement.getGeneratedKeys();
			this.sqlStatementControl.setReturnValue(resultSet);
			
			replay();
			
			ResultSet rs = this.statement.getGeneratedKeys();
			
			verify();
			
			assertSame(resultSet, rs);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getMaxFieldSize()'
	 */
	public void testGetMaxFieldSize()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlStatement.getMaxFieldSize();
			this.sqlStatementControl.setReturnValue(100);
			
			replay();
			
			int size = this.statement.getMaxFieldSize();
			
			verify();
			
			assertEquals(100, size);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getMaxRows()'
	 */
	public void testGetMaxRows()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlStatement.getMaxRows();
			this.sqlStatementControl.setReturnValue(100);
			
			replay();
			
			int size = this.statement.getMaxRows();
			
			verify();
			
			assertEquals(100, size);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getMoreResults()'
	 */
	public void testGetMoreResults()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.getMoreResults();
			this.sqlStatementControl.setReturnValue(true);
			
			replay();
			
			boolean more = this.statement.getMoreResults();
			
			verify();
			
			assertTrue(more);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getMoreResults(int)'
	 */
	public void testGetMoreResultsInt()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.getMoreResults(1);
			this.sqlStatementControl.setReturnValue(true);
			
			replay();
			
			boolean more = this.statement.getMoreResults(1);
			
			verify();
			
			assertTrue(more);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getQueryTimeout()'
	 */
	public void testGetQueryTimeout()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlStatement.getQueryTimeout();
			this.sqlStatementControl.setReturnValue(100);
			
			replay();
			
			int timeout = this.statement.getQueryTimeout();
			
			verify();
			
			assertEquals(100, timeout);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getResultSet()'
	 */
	public void testGetResultSet()
	{
		ResultSet resultSet = (ResultSet) this.createMock(ResultSet.class);
		
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlStatement.getGeneratedKeys();
			this.sqlStatementControl.setReturnValue(resultSet);
			
			replay();
			
			ResultSet rs = this.statement.getGeneratedKeys();
			
			verify();
			
			assertSame(resultSet, rs);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getResultSetConcurrency()'
	 */
	public void testGetResultSetConcurrency()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlStatement.getResultSetConcurrency();
			this.sqlStatementControl.setReturnValue(ResultSet.CONCUR_UPDATABLE);
			
			replay();
			
			int concurrency = this.statement.getResultSetConcurrency();
			
			verify();
			
			assertEquals(ResultSet.CONCUR_UPDATABLE, concurrency);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getResultSetHoldability()'
	 */
	public void testGetResultSetHoldability()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlStatement.getResultSetConcurrency();
			this.sqlStatementControl.setReturnValue(ResultSet.HOLD_CURSORS_OVER_COMMIT);
			
			replay();
			
			int holdability = this.statement.getResultSetConcurrency();
			
			verify();
			
			assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, holdability);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getResultSetType()'
	 */
	public void testGetResultSetType()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlStatement.getResultSetType();
			this.sqlStatementControl.setReturnValue(ResultSet.TYPE_SCROLL_SENSITIVE);
			
			replay();
			
			int type = this.statement.getResultSetType();
			
			verify();
			
			assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, type);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getUpdateCount()'
	 */
	public void testGetUpdateCount()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		try
		{
			this.sqlStatement.getUpdateCount();
			this.sqlStatementControl.setReturnValue(100);
			
			replay();
			
			int count = this.statement.getUpdateCount();
			
			verify();
			
			assertEquals(100, count);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.getWarnings()'
	 */
	public void testGetWarnings()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer);
		
		this.balancer.first();
		this.balancerControl.setReturnValue(this.database);
		
		SQLWarning warnings = new SQLWarning();
		
		try
		{
			this.sqlStatement.getWarnings();
			this.sqlStatementControl.setReturnValue(warnings);
			
			replay();
			
			SQLWarning warn = this.statement.getWarnings();
			
			verify();
			
			assertEquals(warnings, warn);
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.setCursorName(String)'
	 */
	public void testSetCursorName()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.setCursorName("test");
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.statement.setCursorName("test");
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.setEscapeProcessing(boolean)'
	 */
	public void testSetEscapeProcessing()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.setEscapeProcessing(true);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.statement.setEscapeProcessing(true);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.setFetchDirection(int)'
	 */
	public void testSetFetchDirection()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.setFetchDirection(ResultSet.FETCH_REVERSE);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.statement.setFetchDirection(ResultSet.FETCH_REVERSE);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.setFetchSize(int)'
	 */
	public void testSetFetchSize()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.setFetchSize(100);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.statement.setFetchSize(100);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.setMaxFieldSize(int)'
	 */
	public void testSetMaxFieldSize()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.setMaxFieldSize(100);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.statement.setMaxFieldSize(100);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.setMaxRows(int)'
	 */
	public void testSetMaxRows()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.setMaxRows(100);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.statement.setMaxRows(100);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/*
	 * Test method for 'net.sf.hajdbc.sql.Statement.setQueryTimeout(int)'
	 */
	public void testSetQueryTimeout()
	{
		this.databaseCluster.getBalancer();
		this.databaseClusterControl.setReturnValue(this.balancer, 2);
		
		this.balancer.toArray();
		this.balancerControl.setReturnValue(this.databases, 2);
		
		try
		{
			this.sqlStatement.setQueryTimeout(100);
			this.sqlStatementControl.setVoidCallable();
			
			replay();
			
			this.statement.setQueryTimeout(100);
			
			verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}
}
